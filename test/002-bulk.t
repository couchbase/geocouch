#!/usr/bin/env escript
%% -*- erlang -*-

% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-define(MOD, vtree_bulk).
-define(MAX_FILLED, 4).


% same as in vtree
-record(node, {
    % type = inner | leaf
    type=leaf}).

-record(seedtree_root, {
    tree = [] :: list(),
    outliers = [] :: list(),
    height = 0 :: integer()
}).


main(_) ->
    code:add_pathz(filename:dirname(escript:script_name())),
    gc_test_util:init_code_path(),
    etap:plan(99),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail(Other)
    end,
    ok.



test() ->
    test_bulk_load(),
    test_bulk_load_regression(),
    test_omt_load(),
    test_omt_write_tree(),
    test_omt_sort_nodes(),
    test_seedtree_insert(),
    test_seedtree_insert_list(),
    test_seedtree_init(),
    test_seedtree_write_single(),
    test_seedtree_write_case1(),
    test_seedtree_write_case2(),
    test_seedtree_write_case3(),
    test_insert_subtree(),
    test_insert_outliers(),
    test_seedtree_write_insert(),
    ok.



test_bulk_load() ->
    Filename = "/tmp/bulk.bin",
    Fd = case couch_file:open(Filename, [create, overwrite]) of
    {ok, Fd2} ->
        Fd2;
    {error, _Reason} ->
        io:format("ERROR: Couldn't open file (~s) for tree storage~n",
                  [Filename])
    end,

    % Load the initial tree (with bulk operation)
    Nodes1 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeGeom, NodeData}} =
            gc_test_util:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {[NodeId, <<"Bulk1">>], {NodeGeom, NodeData}}},
        [Node|Acc]
    end, [], lists:seq(1,7)),

    {ok, Pos1, Height1} = ?MOD:bulk_load(Fd, 0, 0, Nodes1),
    etap:is(Height1, 2, "Height is correct (bulk_load) (a)"),
    {ok, Lookup1} = gc_test_util:lookup(Fd, Pos1, {0,0,1001,1001}),
    etap:is(length(Lookup1), 7, "Number of nodes is correct (bulk_load) (a)"),
    LeafDepths1 = vtreestats:leaf_depths(Fd, Pos1),
    etap:is(LeafDepths1, [1],
        "Tree depth is equal and correct (bulk_load) (a)"),

    % Load some more nodes
    Nodes2 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeGeom, NodeData}} =
            gc_test_util:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {[NodeId, <<"Bulk1">>], {NodeGeom, NodeData}}},
        [Node|Acc]
    end, [], lists:seq(1,20)),

    {ok, Pos2, Height2} = ?MOD:bulk_load(Fd, Pos1, Height1, Nodes2),
    etap:is(Height2, 4, "Height is correct (bulk_load) (b)"),
    {ok, Lookup2} = gc_test_util:lookup(Fd, Pos2, {0,0,1001,1001}),
    etap:is(length(Lookup2), 27,
        "Number of nodes is correct (bulk_load) (b)"),
    LeafDepths2 = vtreestats:leaf_depths(Fd, Pos2),
    etap:is(LeafDepths2, [3],
        "Tree depth is equal and correct (bulk_load) (b)"),

    % Load some more nodes to find a bug where the tree gets unbalanced
    BulkSize3 = [20, 100, 18],
    Results3 = lists:foldl(fun(Size, Acc2) ->
        {RootPos, RootHeight, _, _} = hd(Acc2),
        Nodes = lists:foldl(fun(I, Acc) ->
           {NodeId, {NodeMbr, NodeMeta, NodeGeom, NodeData}} =
                gc_test_util:random_node({I,27+I*329,45}),
            Node = {NodeMbr, NodeMeta, {[NodeId, <<"Bulk1">>], {NodeGeom, NodeData}}},
          [Node|Acc]
        end, [], lists:seq(1, Size)),

        {ok, Pos, Height} = ?MOD:bulk_load(Fd, RootPos, RootHeight, Nodes),
        {ok, Lookup} = gc_test_util:lookup(Fd, Pos, {0,0,1001,1001}),
        LeafDepths = vtreestats:leaf_depths(Fd, Pos),
        [{Pos, Height, LeafDepths, length(Lookup)}|Acc2]
    end, [{Pos2, Height2, [0], 0}], BulkSize3),
    Results3_2 = [{H, Ld, Num} || {_, H, Ld, Num} <-
            tl(lists:reverse(Results3))],
    etap:is(Results3_2, [{4,[3],47},{5,[4],147},{5,[4],165}],
        "Insertion was correct  (bulk_load) (a)"),

    BulkSize4 = [20, 17, 8, 64, 100],
    Results4 = lists:foldl(fun(Size, Acc2) ->
        {RootPos, RootHeight, _, _} = hd(Acc2),
        Nodes = lists:foldl(fun(I, Acc) ->
            {NodeId, {NodeMbr, NodeMeta, NodeGeom, NodeData}} =
                gc_test_util:random_node({I,27+I*329,45}),
            Node = {NodeMbr, NodeMeta, {[NodeId, <<"Bulk1">>], {NodeGeom, NodeData}}},
           [Node|Acc]
        end, [], lists:seq(1, Size)),
        {ok, Pos, Height} = ?MOD:bulk_load(Fd, RootPos, RootHeight, Nodes),
        {ok, Lookup} = gc_test_util:lookup(Fd, Pos, {0,0,1001,1001}),
        LeafDepths = vtreestats:leaf_depths(Fd, Pos),
       [{Pos, Height, LeafDepths, length(Lookup)}|Acc2]
    end, [{Pos2, Height2, [0], 0}], BulkSize4),
    Results4_2 = [{H, Ld, Num} || {_, H, Ld, Num} <-
            tl(lists:reverse(Results4))],
    etap:is(Results4_2,
        [{4,[3],47},{4,[3],64},{4,[3],72},{5,[4],136},{6,[5],236}],
        "Insertion was correct  (bulk_load) (b)"),
    ok.

test_bulk_load_regression() ->
    Filename = "/tmp/bulk.bin",
    Fd = case couch_file:open(Filename, [create, overwrite]) of
    {ok, Fd2} ->
        Fd2;
    {error, _Reason} ->
        io:format("ERROR: Couldn't open file (~s) for tree storage~n",
                  [Filename])
    end,
    % Huge overflow at the root lead to error
    Node1 = {{-10,-10,20,20}, #node{type=leaf},
        {<<"Bulk">>, {{linestring, [[-10,-10],[20,20]]}, <<"Data">>}}},
    Nodes2 = lists:foldl(fun(_I, Acc) ->
        Node = {{-10,-10,20,20}, #node{type=leaf},
            {<<"2Bulk">>, {{linestring, [[-10,-10],[20,20]]},<<"2Data">>}}},
        [Node|Acc]
    end, [], lists:seq(1,64)),

    {ok, Pos1, Height1} = ?MOD:bulk_load(Fd, 0, 0, [Node1]),
    {ok, Pos2, Height2} = ?MOD:bulk_load(Fd, Pos1, Height1, Nodes2),
    etap:is(Height2, 4, "Height is correct  (bulk_load_regression)"),
    {ok, Lookup2} = gc_test_util:lookup(Fd, Pos2, {0,0,1001,1001}),
    etap:is(length(Lookup2), 65,
        "Number of nodes is correct (bulk_load_regression)"),
    LeafDepths2 = vtreestats:leaf_depths(Fd, Pos2),
    etap:is(LeafDepths2, [3],
        "Tree depth is equal and correct (bulk_load_regression)").


test_omt_load() ->
    Nodes1 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeGeom, NodeData}} =
            gc_test_util:random_node({I,20+I*250,30}),
        Node = {NodeMbr, NodeMeta, {NodeId, NodeGeom, NodeData}},
        [Node|Acc]
    end, [], lists:seq(1,9)),
    {Omt1, _OmtHeight1} = ?MOD:omt_load(Nodes1, 2),
    [L1a, L1b] = Omt1,
    [L2a, L2b] = L1a,
    [L2c, L2d] = L1b,
    [L3a, L3b] = L2a,
    [L3c] = L2b,
    [L3d] = L2c,
    [L3e] = L2d,
    [L4a, L4b] = L3a,
    [L4c] = L3b,
    [L4d, L4e] = L3c,
    [L4f, L4g] = L3d,
    [L4h, L4i] = L3e,
    etap:ok(is_tuple(L4a) and is_tuple(L4b) and is_tuple(L4c) and
        is_tuple(L4d) and is_tuple(L4e) and is_tuple(L4f) and
        is_tuple(L4g) and is_tuple(L4h) and is_tuple(L4i),
        "Only tuples on the lowest level (a)"),

    {Omt2, _OmtHeight2} = ?MOD:omt_load(Nodes1, 4),
    [M1a, M1b, M1c] = Omt2,
    [M2a, M2b, M2c] = M1a,
    [M2d, M2e, M2f] = M1b,
    [M2g, M2h, M2i] = M1c,
    etap:ok(is_tuple(M2a) and is_tuple(M2b) and is_tuple(M2c) and
        is_tuple(M2d) and is_tuple(M2e) and is_tuple(M2f) and
        is_tuple(M2g) and is_tuple(M2h) and is_tuple(M2i),
        "Only tuples on the lowest level (b)"),

    {Omt3, _Omtheight3} = ?MOD:omt_load(Nodes1, 12),
    etap:is(length(Omt3), 9, "Number of nodes is correct").


test_omt_write_tree() ->
    Filename = "/tmp/omt.bin",
    Fd = case couch_file:open(Filename, [create, overwrite]) of
    {ok, Fd2} ->
        Fd2;
    {error, _Reason} ->
        io:format("ERROR: Couldn't open file (~s) for tree storage~n",
                  [Filename])
    end,

    Nodes1 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeGeom, NodeData}} =
            gc_test_util:random_node({I,20+I*250,30}),
        Node = {NodeMbr, NodeMeta, {NodeId, {NodeGeom, NodeData}}},
        [Node|Acc]
    end, [], lists:seq(1,9)),

    % Test 1: OMT tree with MAX_FILLED = 2
    {Omt1, _OmtHeight1} = ?MOD:omt_load(Nodes1, 2),
    {ok, MbrAndPosList} = ?MOD:omt_write_tree(Fd, Omt1),
    RootPosList = [Pos || {_Mbr, Pos} <- MbrAndPosList],

    % just test if the number of children match on all levels
    etap:is(length(RootPosList), 2,
        "Number of children is correct (omt_write_tree) (a)"),
    {ok, L1a} = couch_file:pread_term(Fd, lists:nth(1, RootPosList)),
    etap:is(length(element(3, L1a)), 2,
        "Number of children is correct (omt_write_tree) (b)"),
    {ok, L1b} = couch_file:pread_term(Fd, lists:nth(2, RootPosList)),
    etap:is(length(element(3, L1b)), 2,
        "Number of children is correct (omt_write_tree) (c)"),
    {ok, L2a} = couch_file:pread_term(Fd, lists:nth(1, element(3, L1a))),
    etap:is(length(element(3, L2a)), 2,
        "Number of children is correct (omt_write_tree) (d)"),
    {ok, L2b} = couch_file:pread_term(Fd, lists:nth(2, element(3, L1a))),
    etap:is(length(element(3, L2b)), 1,
        "Number of children is correct (omt_write_tree) (e)"),
    {ok, L2c} = couch_file:pread_term(Fd, lists:nth(1, element(3, L1b))),
    etap:is(length(element(3, L2c)), 1,
        "Number of children is correct (omt_write_tree) (f)"),
    {ok, L2d} = couch_file:pread_term(Fd, lists:nth(2, element(3, L1b))),
    etap:is(length(element(3, L2d)), 1,
        "Number of children is correct (omt_write_tree) (g)"),
    {ok, L3aNode} = couch_file:pread_term(Fd, lists:nth(1, element(3, L2a))),
    {_, _, L3a} = L3aNode,
    {ok, L3bNode} = couch_file:pread_term(Fd, lists:nth(2, element(3, L2a))),
    {_, _, L3b} = L3bNode,
    {ok, L3cNode} = couch_file:pread_term(Fd, lists:nth(1, element(3, L2b))),
    {_, _, L3c} = L3cNode,
    {ok, L3dNode} = couch_file:pread_term(Fd, lists:nth(1, element(3, L2c))),
    {_, _, L3d} = L3dNode,
    {ok, L3eNode} = couch_file:pread_term(Fd, lists:nth(1, element(3, L2d))),
    {_, _, L3e} = L3eNode,

    [L4a, L4b] = L3a,
    [L4c] = L3b,
    [L4d, L4e] = L3c,
    [L4f, L4g] = L3d,
    [L4h, L4i] = L3e,

    etap:ok(is_tuple(L4a) and is_tuple(L4b) and is_tuple(L4c) and
        is_tuple(L4d) and is_tuple(L4e) and is_tuple(L4f) and
        is_tuple(L4g) and is_tuple(L4h) and is_tuple(L4i),
        "Only tuples on the lowest level (omt_write_tree) (MAX_FILLED==2)"),


    % Test 2: OMT tree with MAX_FILLED = 4
    {Omt2, _OmtHeight2} = ?MOD:omt_load(Nodes1, 4),
    {ok, MbrAndPosList2} = ?MOD:omt_write_tree(Fd, Omt2),
    RootPosList2 = [Pos || {_Mbr, Pos} <- MbrAndPosList2],
    etap:is(length(RootPosList2), 3,
        "Correct number of root nodes (omt_write_tree) (MAX_FILLED==4)"),

    {ok, M1aNode} = couch_file:pread_term(Fd, lists:nth(1, RootPosList2)),
    {_, _, M1a} = M1aNode,
    etap:is(length(M1a), 3,
        "Correct number of child nodes (omt_write_tree) (MAX_FILLED==4) (a)"),
    {ok, M1bNode} = couch_file:pread_term(Fd, lists:nth(2, RootPosList2)),
    {_, _, M1b} = M1bNode,
    etap:is(length(M1b), 3,
        "Correct number of child nodes (omt_write_tree) (MAX_FILLED==4) (b)"),
    {ok, M1cNode} = couch_file:pread_term(Fd, lists:nth(3, RootPosList2)),
    {_, _, M1c} = M1cNode,
    etap:is(length(M1c), 3,
        "Correct number of child nodes (omt_write_tree) (MAX_FILLED==4) (c)"),

    [M2a, M2b, M2c] = M1a,
    [M2d, M2e, M2f] = M1b,
    [M2g, M2h, M2i] = M1c,

    etap:ok(is_tuple(M2a) and is_tuple(M2b) and is_tuple(M2c) and
        is_tuple(M2d) and is_tuple(M2e) and is_tuple(M2f) and
        is_tuple(M2g) and is_tuple(M2h) and is_tuple(M2i),
        "Only tuples on the lowest level (omt_write_tree) (MAX_FILLED==4)"),


    % Test 3: OMT tree with MAX_FILLED = 4 and only one node
    Node3 = [{{68,132,678,722}, #node{type=leaf},
        {<<"Node-1">>, {{linestring, [[68,132],[678,722]]}, <<"Value-1">>}}}],

    {Omt3, _OmtHeight3} = ?MOD:omt_load(Node3, 4),
    {ok, MbrAndPosList3} = ?MOD:omt_write_tree(Fd, Omt3),
    RootPosList3 = [Pos || {_Mbr, Pos} <- MbrAndPosList3],
    {ok, Lookup3} = gc_test_util:lookup(Fd, lists:nth(1,RootPosList3), {0,0,1001,1001}),
    etap:is(length(Lookup3), 1,
        "Correct number of nodes (omt_write_tree) "
        "(MAX_FILLED==4, only one node)"),


    % Test 4: round-trip: some nodes => OMT tree, write to disk, load from disk
    {ok, LeafNodes1} = gc_test_util:lookup(
            Fd, lists:nth(1, RootPosList), {0,0,1001,1001}),
    {ok, LeafNodes2} = gc_test_util:lookup(
            Fd, lists:nth(2, RootPosList), {0,0,1001,1001}),
    LeafNodes3 = lists:foldl(fun(LeafNode, Acc) ->
        {NodeMbr, NodeId, NodeGeom, NodeData} = LeafNode,
        Node = {NodeMbr, #node{type=leaf}, {NodeId, {NodeGeom, NodeData}}},
        [Node|Acc]
    end, [], LeafNodes1 ++ LeafNodes2),
    {Omt4, _OmtHeight4} = ?MOD:omt_load(LeafNodes3, 2),
    etap:is(Omt4, Omt1, "Round-trip worked (omt_write_tree)").


test_omt_sort_nodes() ->
    Nodes1 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeGeom, NodeData}} =
            gc_test_util:random_node({I,20+I*300,30-I*54}),
        Node = {NodeMbr, NodeMeta, {NodeId, {NodeGeom, NodeData}}},
        [Node|Acc]
    end, [], lists:seq(1,6)),
    Sorted1 = ?MOD:omt_sort_nodes(Nodes1, 1),
    etap:is(Sorted1, [lists:nth(2, Nodes1),
        lists:nth(1, Nodes1),
        lists:nth(6, Nodes1),
        lists:nth(5, Nodes1),
        lists:nth(4, Nodes1),
        lists:nth(3, Nodes1)],
        "Nodes were sorted correctly (a)"),
    Sorted2 = ?MOD:omt_sort_nodes(Nodes1, 2),
    etap:is(Sorted2, [lists:nth(3, Nodes1),
        lists:nth(5, Nodes1),
        lists:nth(6, Nodes1),
        lists:nth(2, Nodes1),
        lists:nth(4, Nodes1),
        lists:nth(1, Nodes1)],
        "Nodes were sorted correctly (b)").


test_seedtree_insert() ->
    {ok, {Fd, {RootPos, _}}} = gc_test_util:build_random_tree(
            "/tmp/randtree.bin", 20),
    SeedTree = ?MOD:seedtree_init(Fd, RootPos, 3),
    {NodeId, {NodeMbr, NodeMeta, NodeGeom, NodeData}} = gc_test_util:random_node(),
    Node = {NodeMbr, NodeMeta, {NodeId, {NodeGeom, NodeData}}},
    SeedTree2 = ?MOD:seedtree_insert(SeedTree, Node),
    SeedTree2Tree = SeedTree2#seedtree_root.tree,
    etap:is(SeedTree2Tree, {{4,43,980,986},{node,inner},[
        {{4,43,980,960},{node,inner},[
            {{4,43,865,787},{node,inner},{seedtree_leaf,[10099,10719,11088],
                [{{66,132,252,718},{node,leaf},
                    {<<"Node718132">>,
                        {{linestring,[[66,132],[252,718]]},
                        <<"Value718132">>}}}],11362}},
            {{220,45,980,960},{node,inner},{seedtree_leaf,[9538,5338],[],9931}}]},
        {{27,163,597,986},{node,inner},[
            {{37,163,597,911},{node,inner},{seedtree_leaf,[5786,8592],[],9258}},
            {{27,984,226,986},{node,inner},{seedtree_leaf,[7761],[],8480}}]}]},
        "Seed tree is correct (seedtree_insert) (a)"),

    NodeNotInTree3 = {{2,3,4,5}, NodeMeta,
        {<<"notintree">>, {{linestring,[[2,3],[4,5]]}, datafoo}}},
    SeedTree3 = ?MOD:seedtree_insert(SeedTree, NodeNotInTree3),
    Outliers3 = SeedTree3#seedtree_root.outliers,
    etap:is(Outliers3, [{{2,3,4,5}, {node,leaf},
        {<<"notintree">>, {{linestring,[[2,3],[4,5]]}, datafoo}}}],
        "Seed tree outliers are correct (seedtree_insert) (a)"),
    NodeNotInTree4 = {{-2,300.5,4.4,50.45}, {node,leaf},
        {<<"notintree2">>, {{linestring,[[-2,300.5],[4.4,50.45]]}, datafoo2}}},
    SeedTree4 = ?MOD:seedtree_insert(SeedTree2, NodeNotInTree4),
    Outliers4 = SeedTree4#seedtree_root.outliers,
    etap:is(Outliers4,
        [{{-2,300.5,4.4,50.45},{node,leaf},
         {<<"notintree2">>,{{linestring,[[-2,300.5],[4.4,50.45]]},datafoo2}}}],
        "Seed tree outliers are correct (seedtree_insert) (b)"),
    SeedTree5 = ?MOD:seedtree_insert(SeedTree3, NodeNotInTree4),
    Outliers5 = SeedTree5#seedtree_root.outliers,
    etap:is(Outliers5,
        [{{-2,300.5,4.4,50.45},{node,leaf},
          {<<"notintree2">>, {{linestring,[[-2,300.5],[4.4,50.45]]},datafoo2}}},
         {{2,3,4,5},{node,leaf},
          {<<"notintree">>, {{linestring,[[2,3],[4,5]]},datafoo}}}],
        "Seed tree outliers are correct (seedtree_insert) (c)"),
    Node6 = {{342,456,959,513}, NodeMeta,
        {<<"intree01">>, {{linestring,[[342,456],[959,513]]}, datafoo3}}},
    SeedTree6 = ?MOD:seedtree_insert(SeedTree, Node6),

    SeedTree6Tree = SeedTree6#seedtree_root.tree,
    etap:is(SeedTree6Tree,
        {{4,43,980,986},{node,inner},[
        {{4,43,980,960},{node,inner},[
            {{4,43,865,787},{node,inner},{seedtree_leaf,[10099,10719,11088],[],11362}},
            {{220,45,980,960},{node,inner},{seedtree_leaf,[9538,5338],
                [{{342,456,959,513},{node,leaf},
                    {<<"intree01">>,
                        {{linestring,[[342,456],[959,513]]}, datafoo3}}}],9931}}]},
        {{27,163,597,986},{node,inner},[
            {{37,163,597,911},{node,inner},{seedtree_leaf,[5786,8592],[],9258}},
            {{27,984,226,986},{node,inner},{seedtree_leaf,[7761],[],8480}}]}]},
        "Seed tree is correct (b)"),
    SeedTree7 = ?MOD:seedtree_insert(SeedTree2, Node6),
    SeedTree7Tree = SeedTree7#seedtree_root.tree,
    etap:is(SeedTree7Tree,
        {{4,43,980,986},{node,inner},[
        {{4,43,980,960},{node,inner},[
            {{4,43,865,787},{node,inner},{seedtree_leaf,[10099,10719,11088],
                [{{66,132,252,718},{node,leaf},
                    {<<"Node718132">>,
                        {{linestring,[[66,132],[252,718]]},
                        <<"Value718132">>}}}],11362}},
            {{220,45,980,960},{node,inner},{seedtree_leaf,[9538,5338],
                [{{342,456,959,513},{node,leaf},
                    {<<"intree01">>,
                        {{linestring,[[342,456],[959,513]]}, datafoo3}}}],9931}}]},
        {{27,163,597,986},{node,inner},[
            {{37,163,597,911},{node,inner},{seedtree_leaf,[5786,8592],[],9258}},
            {{27,984,226,986},{node,inner},{seedtree_leaf,[7761],[],8480}}]}]},
        "Seed tree is correct (seedtree_insert) (b)").


test_seedtree_insert_list() ->
    {ok, {Fd, {RootPos, _}}} = gc_test_util:build_random_tree(
            "/tmp/randtree.bin", 20),
    SeedTree = ?MOD:seedtree_init(Fd, RootPos, 3),
    {NodeId, {NodeMbr, NodeMeta, NodeGeom, NodeData}} = gc_test_util:random_node(),
    Node = {NodeMbr, NodeMeta, {NodeId, {NodeGeom, NodeData}}},
    SeedTree2 = ?MOD:seedtree_insert(SeedTree, Node),
    SeedTree2Tree = SeedTree2#seedtree_root.tree,
    etap:is(SeedTree2Tree,
        {{4,43,980,986},{node,inner},[
        {{4,43,980,960},{node,inner},[
            {{4,43,865,787},{node,inner},{seedtree_leaf,[10099,10719,11088],
                [{{66,132,252,718},{node,leaf},
                    {<<"Node718132">>,
                        {{linestring,[[66,132],[252,718]]},
                        <<"Value718132">>}}}],11362}},
            {{220,45,980,960},{node,inner},{seedtree_leaf,[9538,5338],[],9931}}]},
        {{27,163,597,986},{node,inner},[
            {{37,163,597,911},{node,inner},{seedtree_leaf,[5786,8592],[],9258}},
            {{27,984,226,986},{node,inner},{seedtree_leaf,[7761],[],8480}}]}]},
        "Seed tree is correct (seedtree_insert_list) (a)"),

    NodeNotInTree3 = {{2,3,4,5}, NodeMeta,
        {<<"notintree">>, {{linestring,[[2,3],[4,5]]}, datafoo}}},
    NodeNotInTree4 = {{-2,300.5,4.4,50.45}, {node,leaf},
        {<<"notintree2">>, {{linestring,[[-2,300.5],[4.4,50.45]]}, datafoo2}}},
    SeedTree5 = ?MOD:seedtree_insert_list(SeedTree,
        [NodeNotInTree3, NodeNotInTree4]),
    Outliers5 = SeedTree5#seedtree_root.outliers,
    etap:is(Outliers5,
        [{{-2,300.5,4.4,50.45},{node,leaf},
          {<<"notintree2">>,{{linestring,[[-2,300.5],[4.4,50.45]]},datafoo2}}},
         {{2,3,4,5},{node,leaf},
          {<<"notintree">>,{{linestring,[[2,3],[4,5]]},datafoo}}}],
        "Seed tree outliers are correct (seedtree_insert_list)"),

    Node6 = {{342,456,959,513}, NodeMeta,
        {<<"intree01">>, {{linestring,[[342,456],[959,513]]}, datafoo3}}},
    SeedTree7 = ?MOD:seedtree_insert_list(SeedTree, [Node, Node6]),
    SeedTree7Tree = SeedTree7#seedtree_root.tree,
    etap:is(SeedTree7Tree, {{4,43,980,986},{node,inner},[
        {{4,43,980,960},{node,inner},[
            {{4,43,865,787},{node,inner},{seedtree_leaf,[10099,10719,11088],
                [{{66,132,252,718},{node,leaf},
                    {<<"Node718132">>,
                        {{linestring,[[66,132],[252,718]]},
                        <<"Value718132">>}}}],11362}},
            {{220,45,980,960},{node,inner},{seedtree_leaf,[9538,5338],
                [{{342,456,959,513},{node,leaf},
                    {<<"intree01">>,
                        {{linestring,[[342,456],[959,513]]}, datafoo3}}}],9931}}]},
        {{27,163,597,986},{node,inner},[
            {{37,163,597,911},{node,inner},{seedtree_leaf,[5786,8592],[],9258}},
            {{27,984,226,986},{node,inner},{seedtree_leaf,[7761],[],8480}}]}]},
        "Seed tree is correct (seedtree_insert_list) (b)").

test_seedtree_init() ->
    {ok, {Fd, {RootPos, _}}} = gc_test_util:build_random_tree(
            "/tmp/randtree.bin", 20),
    SeedTree1 = ?MOD:seedtree_init(Fd, RootPos, 2),
    etap:is(SeedTree1,
        {seedtree_root, {{4,43,980,986}, {node,inner},
        [{{4,43,980,960}, {node,inner},{seedtree_leaf,[11362,9931],[],11423}},
         {{27,163,597,986},{node,inner},{seedtree_leaf,[9258,8480],[],9426}}]},
         [], 2},
        "Seed tree is correct (seedtree_init) (a)"),
    SeedTree2 = ?MOD:seedtree_init(Fd, RootPos, 3),
    etap:is(SeedTree2, {seedtree_root, {{4,43,980,986}, {node,inner},
        [{{4,43,980,960}, {node,inner},
            [{{4,43,865,787},{node,inner},{seedtree_leaf,[10099,10719,11088],[],11362}},
             {{220,45,980,960},{node,inner},{seedtree_leaf,[9538,5338],[],9931}}]},
         {{27,163,597,986}, {node,inner},
            [{{37,163,597,911},{node,inner},{seedtree_leaf,[5786,8592],[],9258}},
             {{27,984,226,986},{node,inner},{seedtree_leaf,[7761],[],8480}}]}]},
         [], 3},
        "Seed tree os cprrect (seedtree_init) (b)"),

    % If you init a seedtree with MaxDepth bigger than the height of the
    % original tree, use the lowest level
    SeedTree3 = ?MOD:seedtree_init(Fd, RootPos, 5),
    SeedTree4 = ?MOD:seedtree_init(Fd, RootPos, 15),
    etap:is(SeedTree4#seedtree_root.tree,
        SeedTree3#seedtree_root.tree,
        "Lowest level of the seed tree was used").


test_seedtree_write_single() ->
    % insert single new node
    TargetTreeNodeNum = 7,
    {ok, {Fd, {RootPos, TargetTreeHeight}}} = gc_test_util:build_random_tree(
            "/tmp/seedtree_write.bin", TargetTreeNodeNum),

    Nodes1 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeGeom, NodeData}} =
            gc_test_util:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {[NodeId, <<"bulk">>], {NodeGeom, NodeData}}},
        [Node|Acc]
    end, [], lists:seq(1,1)),

    Seedtree1 = ?MOD:seedtree_init(Fd, RootPos, 1),
    Seedtree2 = ?MOD:seedtree_insert_list(Seedtree1, Nodes1),
    {ok, Result1, Height1, _HeightDiff1} = ?MOD:seedtree_write(
            Fd, Seedtree2, TargetTreeHeight - Seedtree2#seedtree_root.height),
    etap:is(Height1, 2, "Height is correct (seedtree_write_single)"),
    {ok, ResultPos1} = couch_file:append_term(Fd, hd(Result1)),
    {ok, Lookup1} = gc_test_util:lookup(Fd, ResultPos1, {0,0,1001,1001}),
    etap:is(length(Lookup1), 8,
        "Number of nodes is correct (seedtree_write_single)"),
    LeafDepths1 = vtreestats:leaf_depths(Fd, ResultPos1),
    etap:is(LeafDepths1, [1],
        "Tree depth is equal and correct (seedtree_write_single)").


test_seedtree_write_case1() ->
    % Test "Case 1: input R-tree fits in the target node" (chapter 6.1)
    % Test 1.1: input tree fits in
    % NOTE vmx: seedtree's maximum height should/must be height of the
    %    targetree - 2
    % XXX vmx: This test creates a strange root node
    % NOTE vmx: Not sure if that's still the case
    TargetTreeNodeNum = 25,
    {ok, {Fd, {RootPos, TargetTreeHeight}}} = gc_test_util:build_random_tree(
            "/tmp/seedtree_write.bin", TargetTreeNodeNum),

    Nodes1 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeGeom, NodeData}} =
            gc_test_util:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {[NodeId, <<"bulk">>], {NodeGeom, NodeData}}},
        [Node|Acc]
    end, [], lists:seq(1,20)),

    Seedtree1 = ?MOD:seedtree_init(Fd, RootPos, 1),
    Seedtree2 = ?MOD:seedtree_insert_list(Seedtree1, Nodes1),
    {ok, Result1, Height1, _HeightDiff1} = ?MOD:seedtree_write(
            Fd, Seedtree2, TargetTreeHeight - Seedtree2#seedtree_root.height),
    etap:is(Height1, 4, "Height is correct (seedtree_write_case1)"),
    {ok, ResultPos1} = couch_file:append_term(Fd, hd(Result1)),
    {ok, Lookup1} = gc_test_util:lookup(Fd, ResultPos1, {0,0,1001,1001}),
    etap:is(length(Lookup1), 45,
        "Number of nodes is correct (seedtree_write_case1)"),
    LeafDepths1 = vtreestats:leaf_depths(Fd, ResultPos1),
    etap:is(LeafDepths1, [3],
        "Tree depth is equal and correct (seedtree_write_case1)"),

    % Test 1.2: input tree produces splits (seedtree height=1)
    TargetTreeNodeNum2 = 64,
    TargetTreeHeight2 = ?MOD:log_n_ceil(?MAX_FILLED, TargetTreeNodeNum2),
    {Nodes2, Fd2, RootPos2} = create_random_nodes_and_packed_tree(
        12, TargetTreeNodeNum2, ?MAX_FILLED),

    Seedtree2_1 = ?MOD:seedtree_init(Fd2, RootPos2, 1),
    Seedtree2_2 = ?MOD:seedtree_insert_list(Seedtree2_1, Nodes2),
    {ok, Result2, Height2, _HeightDiff2} = ?MOD:seedtree_write(
            Fd2, Seedtree2_2,
            TargetTreeHeight2 - Seedtree2_2#seedtree_root.height),
    etap:is(Height2, 3,
        "Height is correct (seedtree_write_case1) (height==1)"),
    {ok, ResultPos2} = ?MOD:write_parent(Fd2, Result2),
    {ok, Lookup2} = gc_test_util:lookup(Fd2, ResultPos2, {0,0,1001,1001}),
    etap:is(length(Lookup2), 76,
        "Number of nodes is correct (seedtree_write_case1) (height==1)"),
    LeafDepths2 = vtreestats:leaf_depths(Fd2, ResultPos2),
    etap:is(LeafDepths2, [3],
        "Tree depth is equal and correct (seedtree_write_case1) (height==1)"),

    % Test 1.3: input tree produces splits (recursively) (seedtree height=2)
    % It would create a root node with 5 nodes
    TargetTreeNodeNum3 = 196,
    TargetTreeHeight3 = ?MOD:log_n_ceil(4, TargetTreeNodeNum3),
    {Nodes3, Fd3, RootPos3} = create_random_nodes_and_packed_tree(
        12, TargetTreeNodeNum3, 4),
    Seedtree3_1 = ?MOD:seedtree_init(Fd3, RootPos3, 2),
    Seedtree3_2 = ?MOD:seedtree_insert_list(Seedtree3_1, Nodes3),
    {ok, Result3, Height3, _HeightDiff3} = ?MOD:seedtree_write(
            Fd3, Seedtree3_2,
            TargetTreeHeight3 - Seedtree3_2#seedtree_root.height),
    etap:is(Height3, 4,
        "Height is correct (seedtree_write_case1) (height==2)"),
    {ok, ResultPos3} = ?MOD:write_parent(Fd3, Result3),
    {ok, Lookup3} = gc_test_util:lookup(Fd3, ResultPos3, {0,0,1001,1001}),
    etap:is(length(Lookup3), 208,
        "Number of nodes is correct (seedtree_write_case1) (height==2)"),
    LeafDepths3 = vtreestats:leaf_depths(Fd3, ResultPos3),
    etap:is(LeafDepths3, [4],
        "Tree depth is equal and correct (seedtree_write_case1) (height==2)"),

    % Test 1.4: input tree produces splits (recursively) (seedtree height=3)
    TargetTreeNodeNum4 = 900,
    TargetTreeHeight4 = ?MOD:log_n_ceil(4, TargetTreeNodeNum4),
    {Nodes4, Fd4, RootPos4} = create_random_nodes_and_packed_tree(
        14, TargetTreeNodeNum4, 4),
    Seedtree4_1 = ?MOD:seedtree_init(Fd4, RootPos4, 3),
    Seedtree4_2 = ?MOD:seedtree_insert_list(Seedtree4_1, Nodes4),
    {ok, Result4, Height4, _HeightDiff4} = ?MOD:seedtree_write(
            Fd4, Seedtree4_2,
            TargetTreeHeight4 - Seedtree4_2#seedtree_root.height),
    etap:is(Height4, 5,
        "Height is correct (seedtree_write_case1) (height==3)"),
    {ok, ResultPos4} = ?MOD:write_parent(Fd4, Result4),
    {ok, Lookup4} = gc_test_util:lookup(Fd4, ResultPos4, {0,0,1001,1001}),
    etap:is(length(Lookup4), 914,
        "Number of nodes is correct (seedtree_write_case1) (height==3)"),
    LeafDepths4 = vtreestats:leaf_depths(Fd4, ResultPos4),
    etap:is(LeafDepths4, [5],
        "Tree depth is equal and correct (seedtree_write_case1) (height==3)"),

    % Test 1.5: adding new data with height=4 (seedtree height=1)
    TargetTreeNodeNum5 = 800,
    TargetTreeHeight5 = ?MOD:log_n_ceil(4, TargetTreeNodeNum5),
    {Nodes5, Fd5, RootPos5} = create_random_nodes_and_packed_tree(
        251, TargetTreeNodeNum5, 4),
    Seedtree5_1 = ?MOD:seedtree_init(Fd5, RootPos5, 1),
    Seedtree5_2 = ?MOD:seedtree_insert_list(Seedtree5_1, Nodes5),
    {ok, Result5, Height5, _HeightDiff5} = ?MOD:seedtree_write(
            Fd5, Seedtree5_2,
            TargetTreeHeight5 - Seedtree5_2#seedtree_root.height),
    etap:is(Height5, 5,
        "Height is correct (seedtree_write_case1) (height==1) "
        "(new data height==4)"),
    {ok, ResultPos5} = ?MOD:write_parent(Fd5, Result5),
    {ok, Lookup5} = gc_test_util:lookup(Fd5, ResultPos5, {0,0,1001,1001}),
    etap:is(length(Lookup5), 1051,
        "Number of nodes is correct (seedtree_write_case1) (height==1) "
        "(new data height==4)"),
    LeafDepths5 = vtreestats:leaf_depths(Fd5, ResultPos5),
    etap:is(LeafDepths5, [5],
        "Tree depth is equal and correct (seedtree_write_case1) (height==1) "
        "(new data height==4)").

test_seedtree_write_case2() ->
    % Test "Case 2: input R-tree is not shorter than the level of the
    % target node" (chapter 6.2)
    % Test 2.1: input tree is too high (1 level)
    TargetTreeNodeNum = 25,
    % NOTE vmx: wonder why this tree is not really dense
    {ok, {Fd, {RootPos, TargetTreeHeight}}} = gc_test_util:build_random_tree(
            "/tmp/seedtree_write.bin", TargetTreeNodeNum),
    Nodes1 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeGeom, NodeData}} =
            gc_test_util:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {[NodeId, <<"bulk">>], {NodeGeom, NodeData}}},
        [Node|Acc]
    end, [], lists:seq(1,80)),

    Seedtree1 = ?MOD:seedtree_init(Fd, RootPos, 1),
    Seedtree2 = ?MOD:seedtree_insert_list(Seedtree1, Nodes1),
    {ok, Result1, Height1, _HeightDiff1} = ?MOD:seedtree_write(
            Fd, Seedtree2, TargetTreeHeight - Seedtree2#seedtree_root.height),
    etap:is(Height1, 4, "Height is correct (seedtree_write_case2)"),
    {ok, ResultPos1} = ?MOD:write_parent(Fd, Result1),
    {ok, Lookup1} = gc_test_util:lookup(Fd, ResultPos1, {0,0,1001,1001}),
    etap:is(length(Lookup1), 105,
        "Number of nodes is correct (seedtree_write_case2)"),
    LeafDepths1 = vtreestats:leaf_depths(Fd, ResultPos1),
    etap:is(LeafDepths1, [4],
        "Tree depth is equal and correct (seedtree_write_case2)"),

    % Test 2.2: input tree is too high (2 levels)
    Nodes2 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeGeom, NodeData}} =
            gc_test_util:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {[NodeId, <<"bulk">>], {NodeGeom, NodeData}}},
        [Node|Acc]
    end, [], lists:seq(1,300)),

    Seedtree2_2 = ?MOD:seedtree_insert_list(Seedtree1, Nodes2),
    {ok, Result2, Height2, _HeightDiff2} = ?MOD:seedtree_write(
            Fd, Seedtree2_2,
            TargetTreeHeight - Seedtree2_2#seedtree_root.height),
    etap:is(Height2, 5, "Height is correct (seedtree_write_case2) (2 levels)"),
    {ok, ResultPos2} = ?MOD:write_parent(Fd, Result2),
    {ok, Lookup2} = gc_test_util:lookup(Fd, ResultPos2, {0,0,1001,1001}),
    etap:is(length(Lookup2), 325,
        "Number of nodes is correct (seedtree_write_case2) (2 levels)"),
    LeafDepths2 = vtreestats:leaf_depths(Fd, ResultPos2),
    etap:is(LeafDepths2, [5],
        "Tree depth is equal and correct (seedtree_write_case2) (2 levels)"),

    % Test 2.3: input tree is too high (1 level) and procudes split
    % (seedtree height=1)
    TargetTreeNodeNum3 = 64,
    TargetTreeHeight3 = ?MOD:log_n_ceil(?MAX_FILLED, TargetTreeNodeNum3),
    {Nodes3, Fd3, RootPos3} = create_random_nodes_and_packed_tree(
        50, TargetTreeNodeNum3, ?MAX_FILLED),

    Seedtree3_1 = ?MOD:seedtree_init(Fd3, RootPos3, 1),
    Seedtree3_2 = ?MOD:seedtree_insert_list(Seedtree3_1, Nodes3),
    {ok, Result3, Height3, _HeightDiff3} = ?MOD:seedtree_write(
            Fd3, Seedtree3_2,
            TargetTreeHeight3 - Seedtree3_2#seedtree_root.height),
    etap:is(Height3, 3, "Height is correct (seedtree_write_case2) (1 level)"),
    {ok, ResultPos3} = ?MOD:write_parent(Fd3, Result3),
    {ok, Lookup3} = gc_test_util:lookup(Fd3, ResultPos3, {0,0,1001,1001}),
    etap:is(length(Lookup3), 114,
        "Number of nodes is correct (seedtree_write_case2) (1 level)"),
    LeafDepths3 = vtreestats:leaf_depths(Fd3, ResultPos3),
    etap:is(LeafDepths3, [3],
        "Tree depth is equal and correct (seedtree_write_case2) (1 level)"),

    % Test 2.4: input tree is too high (1 level) and produces multiple
    % splits (recusively) (seedtree height=2)
    % XXX vmx: not really sure if there's more than one split
    TargetTreeNodeNum4 = 196,
    TargetTreeHeight4 = ?MOD:log_n_ceil(4, TargetTreeNodeNum4),
    {Nodes4, Fd4, RootPos4} = create_random_nodes_and_packed_tree(
        50, TargetTreeNodeNum4, 4),
    Seedtree4_1 = ?MOD:seedtree_init(Fd4, RootPos4, 2),
    Seedtree4_2 = ?MOD:seedtree_insert_list(Seedtree4_1, Nodes4),
    {ok, Result4, Height4, _HeightDiff4} = ?MOD:seedtree_write(
            Fd4, Seedtree4_2,
            TargetTreeHeight4 - Seedtree4_2#seedtree_root.height),
    etap:is(Height4, 4,
        "Height is correct (seedtree_write_case2) "
        "(input tree 1 level too high)"),
    {ok, ResultPos4} = ?MOD:write_parent(Fd4, Result4),
    {ok, Lookup4} = gc_test_util:lookup(Fd4, ResultPos4, {0,0,1001,1001}),
    etap:is(length(Lookup4), 246,
        "Number of nodes is correct (seedtree_write_case2) "
        "(input tree 1 level too high)"),
    LeafDepths4 = vtreestats:leaf_depths(Fd4, ResultPos4),
    etap:is(LeafDepths4, [4],
        "Tree depth is equal and correct (seedtree_write_case2) "
        "(input tree 1 level too high)"),

    % Test 2.5: input tree is too high (2 levels) and produces multiple
    % splits (recusively) (seedtree height=2)
    % XXX vmx: not really sure if there's more than one split
    TargetTreeNodeNum5 = 196,
    TargetTreeHeight5 = ?MOD:log_n_ceil(4, TargetTreeNodeNum5),
    {Nodes5, Fd5, RootPos5} = create_random_nodes_and_packed_tree(
        100, TargetTreeNodeNum5, 4),
    Seedtree5_1 = ?MOD:seedtree_init(Fd5, RootPos5, 2),
    Seedtree5_2 = ?MOD:seedtree_insert_list(Seedtree5_1, Nodes5),
    {ok, Result5, Height5, _HeightDiff5} = ?MOD:seedtree_write(
            Fd5, Seedtree5_2,
            TargetTreeHeight5 - Seedtree5_2#seedtree_root.height),
    etap:is(Height5, 4,
        "Height is correct (seedtree_write_case2) "
        "(input tree 2 levels too high)"),
    {ok, ResultPos5} = ?MOD:write_parent(Fd5, Result5),
    {ok, Lookup5} = gc_test_util:lookup(Fd5, ResultPos5, {0,0,1001,1001}),
    etap:is(length(Lookup5), 296,
        "Number of nodes is correct (seedtree_write_case2) "
        "(input tree 2 levels too high)"),
    LeafDepths5 = vtreestats:leaf_depths(Fd5, ResultPos5),
    etap:is(LeafDepths5, [4],
        "Tree depth is equal and correct (seedtree_write_case2) "
        "(input tree 2 levels too high)"),

    % Test 2.6: input tree is too high (2 levels). Insert a OMT tree that
    % leads to massive overflow
    TargetTreeNodeNum6 = 20,
    TargetTreeHeight6 = ?MOD:log_n_ceil(4, TargetTreeNodeNum6),
    {Nodes6, Fd6, RootPos6} = create_random_nodes_and_packed_tree(
        200, TargetTreeNodeNum6, 4),
    Seedtree6_1 = ?MOD:seedtree_init(Fd6, RootPos6, 2),
    Seedtree6_2 = ?MOD:seedtree_insert_list(Seedtree6_1, Nodes6),
    {ok, Result6, Height6, _HeightDiff6} = ?MOD:seedtree_write(
            Fd6, Seedtree6_2,
            TargetTreeHeight6 - Seedtree6_2#seedtree_root.height),
    etap:is(Height6, 4,
        "Height is correct (seedtree_write_case2) "
        "(input tree 2 levels too high, massive overflow)"),
    {ok, ResultPos6} = ?MOD:write_parent(Fd6, Result6),
    {ok, Lookup6} = gc_test_util:lookup(Fd6, ResultPos6, {0,0,1001,1001}),
    etap:is(length(Lookup6), 220,
        "Number of nodes is correct (seedtree_write_case2) "
        "(input tree 2 levels too high, massive overflow)"),
    LeafDepths6 = vtreestats:leaf_depths(Fd6, ResultPos6),
    etap:is(LeafDepths6, [4],
        "Tree depth is equal and correct (seedtree_write_case2) "
        "(input tree 2 levels too high, massive overflow)").

test_seedtree_write_case3() ->
    % Test "Case 3: input R-tree is shorter than the level of the child level of the target node" (chapter 6.3)
    % Test 3.1: input tree is too small (1 level)
    TargetTreeNodeNum = 25,
    % NOTE vmx: wonder why this tree is not really dense
    {ok, {Fd, {RootPos, TargetTreeHeight}}} = gc_test_util:build_random_tree(
            "/tmp/seedtree_write.bin", TargetTreeNodeNum),
    Nodes1 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeGeom, NodeData}} =
            gc_test_util:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {[NodeId, <<"bulk">>], {NodeGeom, NodeData}}},
        [Node|Acc]
    end, [], lists:seq(1,12)),

    Seedtree1 = ?MOD:seedtree_init(Fd, RootPos, 1),
    Seedtree2 = ?MOD:seedtree_insert_list(Seedtree1, Nodes1),
    {ok, Result1, Height1, _HeightDiff1} = ?MOD:seedtree_write(
            Fd, Seedtree2, TargetTreeHeight - Seedtree2#seedtree_root.height),
    etap:is(Height1, 4, "Height is correct (seedtree_write_case3)"),
    {ok, ResultPos1} = couch_file:append_term(Fd, hd(Result1)),
    {ok, Lookup1} = gc_test_util:lookup(Fd, ResultPos1, {0,0,1001,1001}),
    etap:is(length(Lookup1), 37,
        "Number of nodes is correct (seedtree_write_case3)"),
    LeafDepths1 = vtreestats:leaf_depths(Fd, ResultPos1),
    etap:is(LeafDepths1, [3],
        "Tree depth is equal and correct (seedtree_write_case3)"),


    % Test 3.2: input tree is too small (2 levels)
    TargetTreeNodeNum2 = 80,
    % NOTE vmx: wonder why this tree is not really dense
    {ok, {Fd2, {RootPos2, TargetTreeHeight2}}} = gc_test_util:build_random_tree(
            "/tmp/seedtree_write.bin", TargetTreeNodeNum2),

    Nodes2 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeGeom, NodeData}} =
            gc_test_util:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {[NodeId, <<"bulk">>], {NodeGeom, NodeData}}},
        [Node|Acc]
    end, [], lists:seq(1,12)),

    Seedtree2_1 = ?MOD:seedtree_init(Fd2, RootPos2, 1),
    Seedtree2_2 = ?MOD:seedtree_insert_list(Seedtree2_1, Nodes2),
    {ok, Result2, Height2, _HeightDiff2} = ?MOD:seedtree_write(
            Fd2, Seedtree2_2,
            TargetTreeHeight2 - Seedtree2_2#seedtree_root.height),
    etap:is(Height2, 5,
        "Height is correct (seedtree_write_case3) "
        "(input tree 2 levels too small)"),
    {ok, ResultPos2} = ?MOD:write_parent(Fd2, Result2),
    {ok, Lookup2_1} = gc_test_util:lookup(Fd2, ResultPos2, {0,0,1001,1001}),
    etap:is(length(Lookup2_1), 92,
        "Number of nodes is correct (seedtree_write_case3) "
        "(input tree 2 levels too small)"),
    LeafDepths2 = vtreestats:leaf_depths(Fd2, ResultPos2),
    etap:is(LeafDepths2, [5],
        "Tree depth is equal and correct (seedtree_write_case3) "
        "(input tree 2 levels too small)"),

    % Test 3.3: input tree is too small (1 level) and procudes split
    % (seedtree height=1)
    TargetTreeNodeNum3 = 64,
    TargetTreeHeight3 = ?MOD:log_n_ceil(?MAX_FILLED, TargetTreeNodeNum3),
    {Nodes3, Fd3, RootPos3} = create_random_nodes_and_packed_tree(
        4, TargetTreeNodeNum3, ?MAX_FILLED),

    Seedtree3_1 = ?MOD:seedtree_init(Fd3, RootPos3, 1),
    Seedtree3_2 = ?MOD:seedtree_insert_list(Seedtree3_1, Nodes3),
    {ok, Result3, Height3, _HeightDiff3} = ?MOD:seedtree_write(
            Fd3, Seedtree3_2,
            TargetTreeHeight3 - Seedtree3_2#seedtree_root.height),
    etap:is(Height3, 3,
        "Height is correct (seedtree_write_case3) "
        "(input tree 1 level too small, seed tree height==1))"),
    % Several parents => create new root node
    {ok, ResultPos3} = ?MOD:write_parent(Fd3, Result3),
    {ok, Lookup3} = gc_test_util:lookup(Fd3, ResultPos3, {0,0,1001,1001}),
    etap:is(length(Lookup3), 68,
        "Number of nodes is correct (seedtree_write_case3) "
        "(input tree 1 level too small, seed tree height==1))"),
    LeafDepths3 = vtreestats:leaf_depths(Fd3, ResultPos3),
    etap:is(LeafDepths3, [3],
        "Tree depth is equal and correct (seedtree_write_case3) "
        "(input tree 1 level too small, seed tree height==1))"),

    % Test 3.4: input tree is too small (1 level) and produces multiple
    % splits (recusively) (seedtree height=2)
    TargetTreeNodeNum4 = 196,
    TargetTreeHeight4 = ?MOD:log_n_ceil(4, TargetTreeNodeNum4),
    {Nodes4, Fd4, RootPos4} = create_random_nodes_and_packed_tree(
        4, TargetTreeNodeNum4, 4),
    Seedtree4_1 = ?MOD:seedtree_init(Fd4, RootPos4, 2),
    Seedtree4_2 = ?MOD:seedtree_insert_list(Seedtree4_1, Nodes4),
    {ok, Result4, Height4, _HeightDiff4} = ?MOD:seedtree_write(
            Fd4, Seedtree4_2,
            TargetTreeHeight4 - Seedtree4_2#seedtree_root.height),
    etap:is(Height4, 4,
        "Height is correct (seedtree_write_case3) "
        "(input tree 1 level too small, seed tree height==2)"),
    {ok, ResultPos4} = ?MOD:write_parent(Fd4, Result4),
    {ok, Lookup4} = gc_test_util:lookup(Fd4, ResultPos4, {0,0,1001,1001}),
    etap:is(length(Lookup4), 200,
        "Number of nodes is correct (seedtree_write_case3) "
        "(input tree 1 level too small, seed tree height==2))"),
    LeafDepths4 = vtreestats:leaf_depths(Fd4, ResultPos4),
    etap:is(LeafDepths4, [4],
        "Tree depth is equal and correct (seedtree_write_case3) "
        "(input tree 1 level too small, seed tree height==2))"),

    % Test 3.5: input tree is too small (2 levels) and produces multiple
    % splits (recusively) (seedtree height=2)
    TargetTreeNodeNum5 = 768,
    TargetTreeHeight5 = ?MOD:log_n_ceil(4, TargetTreeNodeNum5),
    {Nodes5, Fd5, RootPos5} = create_random_nodes_and_packed_tree(
        4, TargetTreeNodeNum5, 4),
    Seedtree5_1 = ?MOD:seedtree_init(Fd5, RootPos5, 2),
    Seedtree5_2 = ?MOD:seedtree_insert_list(Seedtree5_1, Nodes5),
    {ok, Result5, Height5, _HeightDiff5} = ?MOD:seedtree_write(
            Fd5, Seedtree5_2,
            TargetTreeHeight5 - Seedtree5_2#seedtree_root.height),
    etap:is(Height5, 5,
        "Height is correct (seedtree_write_case3) "
        "(input tree 2 levels too small, seed tree height==2)"),
    {ok, ResultPos5} = couch_file:append_term(Fd5, hd(Result5)),
    {ok, Lookup5} = gc_test_util:lookup(Fd5, ResultPos5, {0,0,1001,1001}),
    etap:is(length(Lookup5), 772,
        "Number of nodes is correct (seedtree_write_case3) "
        "(input tree 2 levels too small, seed tree height==2))"),
    LeafDepths5 = vtreestats:leaf_depths(Fd5, ResultPos5),
    etap:is(LeafDepths5, [4],
        "Tree depth is equal and correct (seedtree_write_case3) "
        "(input tree 2 levels too small, seed tree height==2))").


test_insert_subtree() ->
    {ok, {Fd, {RootPos, _TreeHeight}}} = gc_test_util:build_random_tree(
            "/tmp/randtree.bin", 20),

    % Test 1: no split of the root node
    Nodes1 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeGeom, NodeData}} =
            gc_test_util:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {[NodeId, <<"subtree">>], {NodeGeom, NodeData}}},
        [Node|Acc]
    end, [], lists:seq(1,5)),
    {Omt1, _SubtreeHeight1} = ?MOD:omt_load(Nodes1, ?MAX_FILLED),
    {ok, MbrAndPosList1} = ?MOD:omt_write_tree(Fd, Omt1),

    {ok, _NewMbr1, NewPos1, _Inc1} = ?MOD:insert_subtree(
            Fd, RootPos, MbrAndPosList1, 2),
    {ok, Lookup1} = gc_test_util:lookup(Fd, NewPos1, {0,0,1001,1001}),
    LeafDepths1 = vtreestats:leaf_depths(Fd, NewPos1),
    etap:is(length(Lookup1), 25,
        "Number of nodes is correct (insert_subtree)"),
    etap:is(LeafDepths1, [3],
        "Tree depth is equal and correct (insert_subtree)"),

    % Test 2: split of the root node => tree increases in height
    Nodes2 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeGeom, NodeData}} =
            gc_test_util:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {[NodeId, <<"subtree">>], {NodeGeom, NodeData}}},
        [Node|Acc]
    end, [], lists:seq(1,250)),
    {Omt2, _SubtreeHeight2} = ?MOD:omt_load(Nodes2, ?MAX_FILLED),
    {ok, MbrAndPosList2} = ?MOD:omt_write_tree(Fd, Omt2),

    {ok, _NewMbr2, NewPos2, Inc2} = ?MOD:insert_subtree(
            Fd, RootPos, MbrAndPosList2, 0),
    {ok, Lookup2} = gc_test_util:lookup(Fd, NewPos2, {0,0,1001,1001}),
    LeafDepths2 = vtreestats:leaf_depths(Fd, NewPos2),
    etap:is(Inc2, 1,
        "Tree height was increased (insert_subtree) (splitted root node)"),
    etap:is(length(Lookup2), 270,
        "Number of nodes is correct (insert_subtree) (splitted root node)"),
    etap:is(LeafDepths2, [4],
        "Tree depth is equal and correct (insert_subtree) (splitted root node)").


test_insert_outliers() ->
    TargetTreeNodeNum = 25,
    % NOTE vmx: wonder why this tree is not really dense
    {ok, {Fd, {TargetPos, TargetHeight}}} = gc_test_util:build_random_tree(
            "/tmp/seedtree_write.bin", TargetTreeNodeNum),
    {ok, {TargetMbr, _, _}} = couch_file:pread_term(Fd, TargetPos),

    % Test 1: insert nodes that result in a tree with same height as the
    % target tree. Outliers build a tree with several nodes at the root
    % level. Rootnodes from newly created tree and rootnodes from target
    % tree *fit* into one node, there's no need for a split of the root node.
    Nodes1 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeGeom, NodeData}} =
            gc_test_util:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {[NodeId, <<"Out">>], {NodeGeom, NodeData}}},
        [Node|Acc]
    end, [], lists:seq(1,300)),

    {ResultPos1, ResultHeight1} = ?MOD:insert_outlliers(
            Fd, TargetPos, TargetMbr, TargetHeight, Nodes1),
    etap:is(ResultHeight1, 5,
        "Height is correct (insert_outliers) "
        "(same height, fits into one node)"),
    {ok, Lookup1} = gc_test_util:lookup(Fd, ResultPos1, {0,0,1001,1001}),
    etap:is(length(Lookup1), 325,
        "Number of nodes is correct (insert_outliers) "
        "(same height, fits into one node)"),
    LeafDepths1 = vtreestats:leaf_depths(Fd, ResultPos1),
    % NOTE vmx: if the tree height is x, then the leaf depth is x-1
    etap:is(LeafDepths1, [4],
        "Tree depth is equal and correct (insert_outliers) "
        "(same height, fits into one node)"),

    % Test 2: insert nodes that result in a tree with same height as the
    % target tree. Outliers build a tree with several nodes at the root
    % level. Rootnodes from newly created tree and rootnodes from target
    % tree  *don't fit* into one node, root node needs to be split
    Nodes2 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeGeom, NodeData}} =
            gc_test_util:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {[NodeId, <<"Out">>], {NodeGeom, NodeData}}},
        [Node|Acc]
    end, [], lists:seq(1,1000)),

    {ResultPos2, ResultHeight2} = ?MOD:insert_outlliers(
            Fd, TargetPos, TargetMbr, TargetHeight, Nodes2),
    etap:is(ResultHeight2, 6,
        "Height is correct (insert_outliers) "
        "(same height, doesn't fit into one node)"),
    {ok, Lookup2} = gc_test_util:lookup(Fd, ResultPos2, {0,0,1001,1001}),
    etap:is(length(Lookup2), 1025,
        "Number of nodes is correct (insert_outliers) "
        "(same height, doesn't fit into one node)"),
    LeafDepths2 = vtreestats:leaf_depths(Fd, ResultPos2),
    etap:is(LeafDepths2, [5],
        "Tree depth is equal and correct (insert_outliers) "
        "(same height, doesn't fit into one node)"),

    % Test 3: insert nodes that result in a tree which is higher than the
    % target tree. Outliers build a tree with several nodes at the root
    % level. Rootnodes from newly created tree and nodes from target
    % tree *fit* into one node, there's no need for a split of a node.
    Nodes3 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeGeom, NodeData}} =
            gc_test_util:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {[NodeId, <<"Out">>], {NodeGeom, NodeData}}},
        [Node|Acc]
    end, [], lists:seq(1,1500)),

    {ResultPos3, ResultHeight3} = ?MOD:insert_outlliers(
            Fd, TargetPos, TargetMbr, TargetHeight, Nodes3),
    etap:is(ResultHeight3, 6,
        "Height is correct (insert_outliers) "
        "(higher than target tree, fits into one node)"),
    {ok, Lookup3} = gc_test_util:lookup(Fd, ResultPos3, {0,0,1001,1001}),
    etap:is(length(Lookup3), 1525,
        "Number of nodes is correct (insert_outliers) "
        "(higher than target tree, fits into one node)"),
    LeafDepths3 = vtreestats:leaf_depths(Fd, ResultPos3),
    etap:is(LeafDepths3, [5],
        "Tree depth is equal and correct (insert_outliers) "
        "(higher than target tree, fits into one node)"),

    % Test 4: insert nodes that result in a tree which is higher than the
    % target tree. Outliers build a tree with several nodes at the root
    % level. Rootnodes from newly created tree and nodes from target
    % tree *don't fit* into one node, it needs to be split.
    Nodes4 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeGeom, NodeData}} =
            gc_test_util:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {[NodeId, <<"Out">>], {NodeGeom, NodeData}}},
        [Node|Acc]
    end, [], lists:seq(1,4000)),

    {ResultPos4, ResultHeight4} = ?MOD:insert_outlliers(
            Fd, TargetPos, TargetMbr, TargetHeight, Nodes4),
    etap:is(ResultHeight4, 7,
        "Height is correct (insert_outliers) "
        "(higher than target tree, doens't fit into one node)"),
    {ok, Lookup4} = gc_test_util:lookup(Fd, ResultPos4, {0,0,1001,1001}),
    etap:is(length(Lookup4), 4025,
        "Number of nodes is correct (insert_outliers) "
        "(higher than target tree, doesn't fit into one node)"),
    LeafDepths4 = vtreestats:leaf_depths(Fd, ResultPos4),
    etap:is(LeafDepths4, [6],
        "Tree depth is equal and correct (insert_outliers) "
        "(higher than target tree, doesn't fit into one node)"),

    % Test 5: insert nodes that result in a tree which is smaller than the
    % target tree. Outliers build a tree with several nodes at the root
    % level. Rootnodes from newly created tree and nodes from target
    % tree *fit* into one node, there's no need for a split of a node.
    Nodes5 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeGeom, NodeData}} =
            gc_test_util:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {[NodeId, <<"Out">>], {NodeGeom, NodeData}}},
        [Node|Acc]
    end, [], lists:seq(1,7)),

    {ResultPos5, ResultHeight5} = ?MOD:insert_outlliers(
            Fd, TargetPos, TargetMbr, TargetHeight, Nodes5),
    etap:is(ResultHeight5, 4,
        "Height is correct (insert_outliers) "
        "(smaller than target tree, fits into one node)"),
    {ok, Lookup5} = gc_test_util:lookup(Fd, ResultPos5, {0,0,1001,1001}),
    etap:is(length(Lookup5), 32,
        "Number of nodes is correct (insert_outliers) "
        "(higher than target tree, fits into one node)"),
    LeafDepths5 = vtreestats:leaf_depths(Fd, ResultPos5),
    etap:is(LeafDepths5, [3],
        "Tree depth is equal and correct (insert_outliers) "
        "(higher than target tree, fits into one node)"),

    % Test 6: insert nodes that result in a tree which is smaller than the
    % target tree. Outliers build a tree with several nodes at the root
    % level. Rootnodes from newly created tree and nodes from target
    % tree *don't fit* into one node, it needs to be split.
    Nodes6 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeGeom, NodeData}} =
            gc_test_util:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {[NodeId, <<"Out">>], {NodeGeom, NodeData}}},
        [Node|Acc]
    end, [], lists:seq(1,16)),

    {ResultPos6, ResultHeight6} = ?MOD:insert_outlliers(
            Fd, TargetPos, TargetMbr, TargetHeight, Nodes6),
    etap:is(ResultHeight6, 4,
        "Height is correct (insert_outliers) "
        "(smaller than target tree, doesn't fit into one node)"),
    {ok, Lookup6} = gc_test_util:lookup(Fd, ResultPos6, {0,0,1001,1001}),
    etap:is(length(Lookup6), 41,
        "Number of nodes is correct (insert_outliers) "
        "(higher than target tree, doesn't fit into one node)"),
    LeafDepths6 = vtreestats:leaf_depths(Fd, ResultPos6),
    etap:is(LeafDepths6, [3],
        "Tree depth is equal and correct (insert_outliers) "
        "(higher than target tree, doesn't fit into one node)").


test_seedtree_write_insert() ->
    Filename = "/tmp/swi.bin",
    Fd = case couch_file:open(Filename, [create, overwrite]) of
    {ok, Fd2} ->
        Fd2;
    {error, _Reason} ->
        io:format("ERROR: Couldn't open file (~s) for tree storage~n",
                  [Filename])
    end,

    Nodes1 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeGeom, NodeData}} =
            gc_test_util:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {NodeId, NodeGeom, NodeData}},
        [Node|Acc]
    end, [], lists:seq(1,4)),
    Nodes2 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeGeom, NodeData}} =
            gc_test_util:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {[NodeId, <<"OMT">>], NodeGeom, NodeData}},
        [Node|Acc]
    end, [], lists:seq(1,7)),

    Mbr1 = vtree:calc_nodes_mbr(Nodes1),

    % This test will lead to an OMT tree with several nodes. There was a bug
    % that couldn't handle it.
    {OmtTree1, OmtHeight1} = ?MOD:omt_load(Nodes2, ?MAX_FILLED),
    {ok, Pos1} = couch_file:append_term(Fd, {Mbr1, #node{type=leaf}, Nodes1}),
    Result1 = ?MOD:seedtree_write_insert(Fd, [Pos1], OmtTree1, OmtHeight1),
    etap:is( length(Result1), 3, "Multiple root nodes"),

    % This test will lead to an OMT tree with several nodes. Insert into a
    % single level target tree. There was a bug that couldn't handle it.
    Leafnode = {Mbr1, #node{type=leaf}, Nodes1},
    Result2 = ?MOD:seedtree_write_insert(Fd, [Leafnode], OmtTree1, OmtHeight1),
    etap:is(length(Result2), 8 , "Multiple root nodes (single level target)").


%% Helpers %%

% @doc Returns a bunch of random nodes to bulk insert, a file descriptor and
%     the position of the root node in the file.
-spec create_random_nodes_and_packed_tree(NodesNum::integer(),
        TreeNodeNum::integer(), MaxFilled::integer()) ->
        {[tuple()], file:io_device(), integer()}.
create_random_nodes_and_packed_tree(NodesNum, TreeNodeNum, MaxFilled) ->
    Filename = "/tmp/random_packed_tree.bin",

    Fd = case couch_file:open(Filename, [create, overwrite]) of
    {ok, Fd2} ->
        Fd2;
    {error, _Reason} ->
        io:format("ERROR: Couldn't open file (~s) for tree storage~n",
                  [Filename])
    end,

    OmtNodes = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeGeom, NodeData}} =
            gc_test_util:random_node({I,20+I*250,30}),
        Node = {NodeMbr, NodeMeta, {NodeId, {NodeGeom, NodeData}}},
        [Node|Acc]
    end, [], lists:seq(1, TreeNodeNum)),
    {Omt, _OmtHeight} = ?MOD:omt_load(OmtNodes, MaxFilled),
    {ok, MbrAndPosList} = ?MOD:omt_write_tree(Fd, Omt),
    {Mbrs, Poss} = lists:unzip(MbrAndPosList),
    Mbr = vtree:calc_mbr(Mbrs),
    {ok, RootPos} = couch_file:append_term(Fd, {Mbr, #node{type=inner}, Poss}),

    Nodes = lists:foldl(fun(I, Acc) ->
        Node = {{200+I,250+I,300+I,350+I}, #node{type=leaf},
            {"Node-" ++ integer_to_list(I),
             {{linestring, [[200+I,250+I],[300+I,350+I]]},
             "Value-" ++ integer_to_list(I)}}},
        [Node|Acc]
    end, [], lists:seq(1, NodesNum)),

    {Nodes, Fd, RootPos}.
