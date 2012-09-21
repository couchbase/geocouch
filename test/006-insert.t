#!/usr/bin/env escript
%% -*- erlang -*-
%%! -na me insert@127.0.0.1

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

-include_lib("../include/vtree.hrl").

-define(MOD, vtree_insert).
-define(FILENAME, "/tmp/vtree_insert_vtree.bin").

main(_) ->
    % Set the random seed once. It might be reset a certain tests
    random:seed(1, 11, 91),

    code:add_pathz(filename:dirname(escript:script_name())),
    etap:plan(62),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            % Somehow etap:diag/1 and etap:bail/1 don't work properly
            %etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            %etap:bail(Other),
            io:format(standard_error, "Test died abnormally:~n~p~n", [Other])
     end.


test() ->
    couch_file_write_guard:sup_start_link(),
    test_insert(),
    test_insert_in_bulks(),
    test_insert_multiple(),
    test_partition_nodes(),
    test_write_new_root(),
    test_insert_into_nodes(),
    test_write_nodes(),
    test_write_multiple_nodes(),
    test_split_node(),
    test_add_to_nth(),
    test_get_key(),
    ok.


test_insert() ->
    Fd = vtree_test_util:create_file(?FILENAME),
    Nodes1 = vtree_test_util:generate_kvnodes(6),

    Vtree1 = #vtree{
      fd = Fd,
      fill_min = 4,
      fill_max = 8,
      less = fun(A, B) -> A < B end
     },

    NewVtree1 = ?MOD:insert(Vtree1, [hd(Nodes1)]),
    Root1 = NewVtree1#vtree.root,
    [InsertedNodes1] = vtree_io:read_node(Fd, Root1#kp_node.childpointer),
    etap:is(InsertedNodes1, hd(Nodes1),
            "Inserted single node into emtpy tree: node got inserted"),

    NewVtree2 = ?MOD:insert(Vtree1, Nodes1),
    Root2 = NewVtree2#vtree.root,
    InsertedNodes2 = vtree_io:read_node(Fd, Root2#kp_node.childpointer),
    etap:is(InsertedNodes2, Nodes1,
            "Inserted into 6 nodes into emtpy tree: all nodes got inserted"),

    Nodes3 = vtree_test_util:generate_kvnodes(500),
    NewVtree3 = ?MOD:insert(Vtree1, Nodes3),
    Root3 = NewVtree3#vtree.root,
    {Depths3, KvNodes3} = get_kvnodes(Fd, Root3#kp_node.childpointer),
    etap:is(sets:size(sets:from_list(Depths3)), 1,
            "Inserted 500 nodes into emtpy tree: tree is balanced"),
    etap:is(lists:sort(KvNodes3), lists:sort(Nodes3),
            "Inserted 500 nodes into emtpy tree: all nodes got inserted"),

    Nodes4 = vtree_test_util:generate_kvnodes(223),
    NewVtree4 = ?MOD:insert(NewVtree3, Nodes4),
    Root4 = NewVtree4#vtree.root,
    {Depths4, KvNodes4} = get_kvnodes(Fd, Root4#kp_node.childpointer),
    etap:is(sets:size(sets:from_list(Depths4)), 1,
            "Inserted 223 nodes into existing tree: tree is balanced"),
    etap:is(lists:sort(KvNodes4), lists:sort(Nodes3 ++ Nodes4),
            "Inserted 223 nodes into existing tree: all nodes got inserted"),

    Nodes5 = vtree_test_util:generate_kvnodes(1),
    NewVtree5 = ?MOD:insert(NewVtree1, Nodes5),
    Root5 = NewVtree5#vtree.root,
    {Depths5, KvNodes5} = get_kvnodes(Fd, Root5#kp_node.childpointer),
    etap:is(sets:size(sets:from_list(Depths5)), 1,
            "Inserted single node into existing single node tree: tree is "
            "balanced. Root has < fill_min nodes"),
    etap:is(lists:sort(KvNodes5), lists:sort([hd(Nodes1)|Nodes5]),
            "Inserted single node into existing tree: node got inserted. "
            "Root has < fill_min nodes"),

    couch_file:close(Fd).


test_insert_in_bulks()->
    Less = fun(A, B) -> A < B end,
    Fd = vtree_test_util:create_file(?FILENAME),

    Vtree1 = #vtree{
      fd = Fd,
      fill_min = 2,
      fill_max = 4,
      less = Less
     },

    Nodes1 = vtree_test_util:generate_kvnodes(20),
    Nodes2 = vtree_test_util:generate_kvnodes(100),
    Nodes3 = vtree_test_util:generate_kvnodes(50),

    #vtree{root=Root1} = ?MOD:insert_in_bulks(Vtree1, Nodes1, 7),
    {Depths1, KvNodes1} = get_kvnodes(Fd, Root1#kp_node.childpointer),
    etap:is(sets:size(sets:from_list(Depths1)), 1,
            "Tree is balanced (originally empty) (a)"),
    etap:is(lists:sort(KvNodes1), lists:sort(Nodes1),
            "All nodes were inserted (originally empty) (a)"),

    #vtree{root=Root2} = ?MOD:insert_in_bulks(Vtree1, Nodes2, 9),
    {Depths2, KvNodes2} = get_kvnodes(Fd, Root2#kp_node.childpointer),
    etap:is(sets:size(sets:from_list(Depths2)), 1,
            "Tree is balanced (originally empty) (b)"),
    etap:is(lists:sort(KvNodes2), lists:sort(Nodes2),
            "All nodes were inserted (originally empty) (b)"),

    #vtree{root=Root3} = ?MOD:insert_in_bulks(Vtree1#vtree{root=Root1},
                                              Nodes3, 10),
    {Depths3, KvNodes3} = get_kvnodes(Fd, Root3#kp_node.childpointer),
    etap:is(sets:size(sets:from_list(Depths3)), 1,
            "Tree is balanced (originally not empty)"),
    etap:is(lists:sort(KvNodes3), lists:sort(Nodes1 ++ Nodes3),
            "All nodes were inserted (originally not empty)"),

    couch_file:close(Fd).


test_insert_multiple() ->
    Less = fun(A, B) -> A < B end,
    Fd = vtree_test_util:create_file(?FILENAME),

    Vtree1 = #vtree{
      fd = Fd,
      fill_min = 2,
      fill_max = 4,
      less = Less
     },

    Nodes1 = vtree_test_util:generate_kvnodes(20),
    Nodes2 = vtree_test_util:generate_kvnodes(10),
    Nodes3 = vtree_test_util:generate_kvnodes(100),
    Vtree2 = #vtree{root=Root} = ?MOD:insert(Vtree1, Nodes1),

    PartitionedNodes1 = ?MOD:partition_nodes([Root], Nodes2, Less),
    [KpNode1] = ?MOD:insert_multiple(Vtree2, PartitionedNodes1, [Root], []),
    {Depths1, KvNodes1} = get_kvnodes(Fd, KpNode1#kp_node.childpointer),
    etap:is(sets:size(sets:from_list(Depths1)), 1, "Tree is balanced (a)"),
    etap:is(lists:sort(KvNodes1), lists:sort(Nodes1 ++ Nodes2),
            "All nodes were inserted (a)"),

    PartitionedNodes2 = ?MOD:partition_nodes([Root], Nodes3, Less),
    KpNodes2 = ?MOD:insert_multiple(Vtree2, PartitionedNodes2, [Root], []),
    KpNode2 = ?MOD:write_new_root(Vtree2, KpNodes2),
    {Depths2, KvNodes2} = get_kvnodes(Fd, KpNode2#kp_node.childpointer),
    etap:is(sets:size(sets:from_list(Depths2)), 1, "Tree is balanced (b)"),
    etap:is(lists:sort(KvNodes2), lists:sort(Nodes1 ++ Nodes3),
            "All nodes were inserted (b)"),

    couch_file:close(Fd).


test_partition_nodes() ->
    Less = fun(A, B) -> A < B end,

    Mbb1 = [{-700, -600}, {-30, -20}],
    Mbb2 = [{-600, -500}, {-25, -10}],
    Mbb3 = [{-550, -400}, {-10, 0}],
    Mbb4 = [{-300, -200}, {0, 40}],
    Mbb5 = [{-200, -150}, {30, 80}],

    Mbb6 = [{-740, -720}, {-30, -25}],
    Mbb7 = [{-600, -500}, {-20, -15}],
    Mbb8 = [{-550, -450}, {-25, 10}],
    Mbb9 = [{-150, -120}, {50, 60}],

    Mbb10 = [{300, 500}, {500, 600}],
    Mbb11 = [{-1000, 0}, {-50, 50}],

    Mbb12 = [{5000, 9000}, {7000, 9000}],
    Mbb13 = [{0, 1}, {100, 101}],
    Mbb14 = [{-5000, -9000}, {-7000, -9000}],


    KpNodes = [#kp_node{key=Mbb} || Mbb <- [Mbb1, Mbb2, Mbb3, Mbb4, Mbb5]],
    ToPartition = [#kv_node{key=Mbb} || Mbb <- [Mbb7, Mbb9, Mbb8, Mbb6]],

    Partitioned1 = ?MOD:partition_nodes(KpNodes, ToPartition, Less),
    etap:is(lists:sort(lists:append(Partitioned1)),
            lists:sort(ToPartition),
            "All nodes were partitioned"),
    etap:is(Partitioned1,
            [[#kv_node{key=Mbb6}], [#kv_node{key=Mbb8}, #kv_node{key=Mbb7}],
             [], [], [#kv_node{key=Mbb9}]],
            "All nodes were correctly partitioned (a)"),
    etap:is(?MOD:partition_nodes(KpNodes, [#kv_node{key=Mbb8}], Less),
            [[], [#kv_node{key=Mbb8}], [], [], []],
            "One node was correctly partitioned"),

    KpNodes2 = [#kp_node{key=Mbb} || Mbb <- [Mbb10, Mbb11]],
    [H, Partitioned2] = ?MOD:partition_nodes(KpNodes2, ToPartition, Less),
    etap:is(H, [], "All nodes were correctly partitioned (b1)"),
    etap:is(lists:sort(Partitioned2), lists:sort(ToPartition),
            "All nodes were correctly partitioned (b2)"),

    KpNodes3 = [#kp_node{key=Mbb} || Mbb <- [Mbb12, Mbb13, Mbb14]],
    Partitioned3 = ?MOD:partition_nodes(KpNodes3, ToPartition, Less),
    etap:is(lists:nth(1, Partitioned3), [],
            "All nodes were correctly partitioned (c1)"),
    etap:is(lists:sort(lists:nth(2, Partitioned3)), lists:sort(ToPartition),
            "All nodes were correctly partitioned (c2)"),
    etap:is(lists:nth(1, Partitioned3), [],
            "All nodes were correctly partitioned (c3)").


test_write_new_root() ->
    Less = fun(A, B) -> A < B end,
    Fd = vtree_test_util:create_file(?FILENAME),

    Vtree1 = #vtree{
      fill_min = 2,
      fill_max = 4,
      less = Less,
      fd = Fd
     },

    NodesKp = vtree_test_util:generate_kpnodes(5),

    Root1 = ?MOD:write_new_root(Vtree1, [hd(NodesKp)]),
    etap:is(Root1, hd(NodesKp), "Single node is new root"),

    Root2 = ?MOD:write_new_root(Vtree1, tl(NodesKp)),
    Root2Children = vtree_io:read_node(Fd, Root2#kp_node.childpointer),
    etap:is(Root2Children, tl(NodesKp),
            "A new root node was written (one new level)"),

    Root3 = ?MOD:write_new_root(Vtree1, NodesKp),
    Root3Children = vtree_io:read_node(Fd, Root3#kp_node.childpointer),
    ChildPointer = [C#kp_node.childpointer || C <- Root3Children],
    Root3ChildrenChildren = lists:append(
                              [vtree_io:read_node(Fd, C#kp_node.childpointer)
                               || C <- Root3Children]),
    etap:is(lists:sort(Root3ChildrenChildren), lists:sort(NodesKp),
            "A new root node was written (two new levels)"),

    NodesKv = vtree_test_util:generate_kvnodes(4),
    MbbOKv = (hd(NodesKv))#kv_node.key,

    Root4 = ?MOD:write_new_root(Vtree1, NodesKv),
    Root4Children = vtree_io:read_node(Fd, Root4#kp_node.childpointer),
    etap:is(Root4Children, NodesKv,
            "A new root node (for KV-nodes) was written (one new level)"),

    couch_file:close(Fd).


test_insert_into_nodes() ->
    Tests = [
             {2, 4},
             {3, 6},
             {10, 30}
            ],
    lists:foreach(fun({FillMin, FillMax}) ->
                          insert_into_nodes(FillMin, FillMax)
                  end, Tests).

insert_into_nodes(FillMin, FillMax) ->
    Less = fun(A, B) -> A < B end,

    Vtree = #vtree{
      fill_min = FillMin,
      fill_max = FillMax,
      less = Less
     },

    Nodes1 = vtree_test_util:generate_kpnodes(4),
    Nodes2 = vtree_test_util:generate_kpnodes(20),
    MbbO = (hd(Nodes1))#kp_node.key,

    Inserted = ?MOD:insert_into_nodes(Vtree, [Nodes1], MbbO, Nodes2),
    etap:is(length(lists:append(Inserted)), 24,
            "All nodes got inserted"),
    NodesFilledOk = lists:all(
                      fun(N) ->
                              length(N) >= Vtree#vtree.fill_min andalso
                                  length(N) =< Vtree#vtree.fill_max
                      end, Inserted),
    etap:ok(NodesFilledOk,
            "All child nodes have the right number of nodes"),
    etap:ok(length(Inserted) >= 24/FillMax andalso
            length(Inserted) =< 24/FillMin,
            "Right number of child nodes").


test_write_nodes() ->
    % This tests depends on the values of the MBBs, hence reset the seed
    random:seed(1, 11, 91),
    Tests = [
             {4, 1, "Less then the maximum number of nodes were written"},
             {5, 1, "The maximum number of nodes were written"},
             {6, 2, "One more than maximum number of nodes were written"},
             {8, 2, "A bit more than maximum number of nodes were written"},
             % The expected 9 nodes depend on the choose_subtree algorithm
             {30, 9, "Way more than maximum number of nodes were written"}
            ],
    lists:foreach(fun({Insert, Expected, Message}) ->
                          write_nodes(Insert, Expected, Message)
                  end, Tests).

write_nodes(Insert, Expected, Message) ->
    Less = fun(A, B) -> A < B end,
    Fd = vtree_test_util:create_file(?FILENAME),

    Vtree = #vtree{
      fill_min = 2,
      fill_max = 5,
      less = Less,
      fd = Fd
     },

    Nodes = vtree_test_util:generate_kpnodes(Insert),
    MbbO = (hd(Nodes))#kp_node.key,
    WrittenNodes = ?MOD:write_nodes(Vtree, Nodes, MbbO),
    etap:is(length(WrittenNodes), Expected, Message),
    etap:ok(lists:all(fun(#kp_node{}) -> true; (_) -> false end, WrittenNodes),
            "The return values are KP-nodes"),

    couch_file:close(Fd).


test_write_multiple_nodes() ->
    Less = fun(A, B) -> A < B end,
    Fd1a = vtree_test_util:create_file(?FILENAME),

    Vtree1 = #vtree{
      fill_min = 2,
      fill_max = 4,
      less = Less,
      fd = Fd1a
     },

    Nodes1a = vtree_test_util:generate_kvnodes(5),
    Nodes1b = vtree_test_util:generate_kvnodes(8),
    WrittenNodes1 = ?MOD:write_multiple_nodes(Vtree1, [Nodes1a, Nodes1b]),
    couch_file:close(Fd1a),
    Fd1b = vtree_test_util:create_file(?FILENAME),
    {ok, WrittenNodes1a} = vtree_io:write_node(Fd1b, Nodes1a, Less),
    {ok, WrittenNodes1b} = vtree_io:write_node(Fd1b, Nodes1b, Less),
    % The #kp_node.mmb_orig isn't set bt vtree_io, hence set it manually
    etap:is(WrittenNodes1,
            [WrittenNodes1a#kp_node{mbb_orig=WrittenNodes1a#kp_node.key},
             WrittenNodes1b#kp_node{mbb_orig=WrittenNodes1b#kp_node.key}],
            "Multiple KV-nodes were correctly written"),
    couch_file:close(Fd1b),

    Fd2a = vtree_test_util:create_file(?FILENAME),
    Vtree2 = Vtree1#vtree{fd = Fd2a},
    Nodes2a = vtree_test_util:generate_kpnodes(5),
    Nodes2b = vtree_test_util:generate_kpnodes(8),
    WrittenNodes2 = ?MOD:write_multiple_nodes(Vtree2, [Nodes2a, Nodes2b]),
    couch_file:close(Fd2a),
    Fd2b = vtree_test_util:create_file(?FILENAME),
    {ok, WrittenNodes2a} = vtree_io:write_node(Fd2b, Nodes2a, Less),
    {ok, WrittenNodes2b} = vtree_io:write_node(Fd2b, Nodes2b, Less),
    % The #kp_node.mmb_orig isn't set bt vtree_io, hence set it manually
    etap:is(WrittenNodes2,
            [WrittenNodes2a#kp_node{mbb_orig=WrittenNodes2a#kp_node.key},
             WrittenNodes2b#kp_node{mbb_orig=WrittenNodes2b#kp_node.key}],
            "Multiple KP-nodes were correctly written"),

    couch_file:close(Fd2b).


test_split_node() ->
    Vtree = #vtree{
      fill_min = 3,
      fill_max = 6
     },
    KvNodes = vtree_test_util:generate_kvnodes(7),
    KpNodes = vtree_test_util:generate_kpnodes(7),
    KvMbbO = (hd(KvNodes))#kv_node.key,
    KpMbbO = (hd(KpNodes))#kp_node.key,
    NodesFilledOkFun = fun(N) ->
                               length(N) >= Vtree#vtree.fill_min andalso
                                   length(N) =< Vtree#vtree.fill_max
                       end,

    {KvNodesA, KvNodesB} = ?MOD:split_node(Vtree, KvNodes, KvMbbO),
    KvNodesFilledOk = lists:all(NodesFilledOkFun, [KvNodesA, KvNodesB]),
    etap:ok(KvNodesFilledOk,
            "Both partitions contain the right number of nodes"),
    etap:is(lists:sort(lists:append([KvNodesA, KvNodesB])),
            lists:sort(KvNodes),
            "All nodes are still there after the split"),

    {KpNodesA, KpNodesB} = ?MOD:split_node(Vtree, KpNodes, KpMbbO),
    KpNodesFilledOk = lists:all(NodesFilledOkFun, [KpNodesA, KpNodesB]),
    etap:ok(KpNodesFilledOk,
            "Both partitions contain the right number of nodes"),
    etap:is(lists:sort(lists:append([KpNodesA, KpNodesB])),
            lists:sort(KpNodes),
            "All nodes are still there after the split").


% Return a 2-tuple with a list of the depths of the KV-nodes and the
% KV-nodes themselves
get_kvnodes(Fd, RootPos) ->
    Children = vtree_io:read_node(Fd, RootPos),
    get_kvnodes(Fd, Children, 0, {[], []}).
get_kvnodes(_Fd, [], _Depth, Acc) ->
    Acc;
get_kvnodes(_Fd, [#kv_node{}|_]=Children, Depth, {Depths, Nodes}) ->
    {[Depth|Depths], Children ++ Nodes};
get_kvnodes(Fd, [#kp_node{}=Node|Rest], Depth, Acc) ->
    Children = vtree_io:read_node(Fd, Node#kp_node.childpointer),
    % Move down
    Acc2 = get_kvnodes(Fd, Children, Depth+1, Acc),
    % Move sideways
    get_kvnodes(Fd, Rest, Depth, Acc2).


test_add_to_nth() ->
    ListOfLists = [[a, b], [c, d, e], [f, g], [h]],
    etap:is(?MOD:add_to_nth(3, z, ListOfLists),
            [[a, b], [c, d, e], [z, f, g], [h]],
            "Element was added succesfully (a)"),
    etap:is(?MOD:add_to_nth(1, y, ListOfLists),
            [[y, a, b], [c, d, e], [f, g], [h]],
            "Element was added succesfully (b)"),
    etap:is(?MOD:add_to_nth(2, x, ListOfLists),
            [[a, b], [x, c, d, e], [f, g], [h]],
            "Element was added succesfully (c)"),
    etap:is(?MOD:add_to_nth(4, w, ListOfLists),
            [[a, b], [c, d, e], [f, g], [w, h]],
            "Element was added succesfully (d)"),
    try
        ?MOD:add_to_nth(10, w, ListOfLists)
    catch
        error:badarg ->
            etap:ok(true, "Error was thrown")
    end.


test_get_key() ->
    [KvNode] = vtree_test_util:generate_kvnodes(1),
    [KpNode] = vtree_test_util:generate_kpnodes(1),

    etap:is(?MOD:get_key(KvNode), KvNode#kv_node.key,
            "Returns the key of a KV-node"),
    etap:is(?MOD:get_key(KpNode), KpNode#kp_node.key,
            "Returns the key of a KP-node").
