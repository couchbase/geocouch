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

-module(vtree_bulk).
-include_lib("eunit/include/eunit.hrl").

-define(LOG_DEBUG(Msg), io:format(user, "DEBUG: ~p~n", [Msg])).

-record(seedtree_root, {
    tree = [] :: list(),
    outliers = [] :: list()
}).
-record(seedtree_leaf, {
    orig = [] :: list(),
    new = [] :: list()
}).

-type seedtree_root() :: tuple().
-type seedtree_node() :: tuple().

% @doc Lookup a bounding box in the tree. Return the path to the node. Every
%     list element is the child node position, e.g. [1,2,0] means:
%     2nd child at depth 0, 3rd child at depth 1 and 1st child at depth 2
-spec lookup_path(Fd::file:io_device(), RootPos::integer(),
        Bbox::[number()], MaxDepth::integer()) -> [integer()].
lookup_path(Fd, RootPos, Bbox, MaxDepth) ->
    lookup_path(Fd, RootPos, Bbox, MaxDepth, 0).

-spec lookup_path(Fd::file:io_device(), RootPos::integer(),
        Bbox::[number()], MaxDepth::integer(), Depth::integer()) ->
        [integer()].
lookup_path(Fd, RootPos, Bbox, MaxDepth, Depth) ->
    {ok, Parent} = couch_file:pread_term(Fd, RootPos),
    {ParentMbr, _ParentMeta, EntriesPos} = Parent,
    case vtree:within(Bbox, ParentMbr) of
    true when Depth == MaxDepth ->
        ok;
    true when Depth < MaxDepth ->
        {Go, Val} = vtree:foldl_stop(fun(EntryPos, Acc) ->
            case lookup_path(Fd, EntryPos, Bbox, MaxDepth, Depth+1) of
            % No matching node so far, go on...
            -1 ->
                {ok, Acc+1};
            % ...you read the maximum depth -> start returning the path to
            % this node...
            ok ->
                {stop, [Acc+1]};
            % ...keep on returning the upwards path to the matching node.
            PrevVal ->
                {stop, [Acc+1|PrevVal]}
            end
        end, -1, EntriesPos),
        case Go of
            % a matching entry (child node) was found
            stop -> Val;
            % stopped automatically without finding a matching entry
            ok -> -1
        end;
    false ->
        -1
    end.

-type omt_node() :: tuple().
% @doc OMT bulk loading. MaxNodes is the number of maximum children per node
%     Based on (but modified):
%     OMT: Overlap Minimizing Top-down Bulk Loading Algorithm for R-tree
-spec omt_load(Nodes::[omt_node()], MaxNodes::integer()) -> omt_node().
omt_load(Nodes, MaxNodes) ->
    omt_load(Nodes, MaxNodes, 0).
-spec omt_load(Nodes::[omt_node()], MaxNodes::integer(), Dimension::integer())
    -> omt_node().
omt_load(Nodes, MaxNodes, _Dimension) when length(Nodes) =< MaxNodes ->
    [Nodes];
omt_load(Nodes, MaxNodes, Dimension) ->
    NodesNum = length(Nodes),
    % Height of the final tree
    Height = log_n_ceil(MaxNodes, NodesNum),
    % Maximum number of children in a direct subtree of the root node
    % with the minimum depth that is needed to store all nodes
    ChildrenSubNum = math:pow(MaxNodes, Height-1),
    % Number of children
    RootChildrenNum = ceiling(NodesNum/ChildrenSubNum),
    % Entries per child
    EntriesNum = ceiling(NodesNum/RootChildrenNum),

    % NOTE vmx: currently all nodes have only 2 demnsions => "rem 2"
    Sorted = omt_sort_nodes(Nodes, (Dimension rem 2)+1),
    _Chunked = chunk_list(fun(InnerNodes) ->
        omt_load(InnerNodes, MaxNodes, Dimension+1)
    end, Sorted, EntriesNum).

omt_load_test() ->
    Nodes1 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeData}} =
            vtree_test:random_node({I,20+I*250,30}),
        Node = {NodeMbr, NodeMeta, {NodeId, NodeData}},
        [Node|Acc]
    end, [], lists:seq(1,20)),
    Omt1 = omt_load(Nodes1, 4),
    ?assertEqual(2, length(Omt1)),
    ?assertEqual(3, length(lists:nth(1, Omt1))),
    Omt2 = omt_load(Nodes1, 2),
    ?assertEqual(2, length(Omt2)),
    ?assertEqual(2, length(lists:nth(1, Omt2))),
    ?assertEqual(2, length(lists:nth(1, lists:nth(1, Omt2)))),
    ?assertEqual(2, length(lists:nth(1, lists:nth(1, lists:nth(1, Omt2))))),
    Omt4 = omt_load(Nodes1, 40),
    ?assertEqual(1, length(Omt4)).

% @doc Sort nodes by a certain dimension (which is the first element of the
%     node tuple)
-spec omt_sort_nodes(Nodes::[omt_node()], Dimension::integer()) -> [omt_node()].
omt_sort_nodes(Nodes, Dimension) ->
    lists:sort(fun({Mbr1, _, _}, {Mbr2, _, _}) ->
        Val1 = element(Dimension, Mbr1),
        Val2 = element(Dimension, Mbr2),
        Val1 =< Val2
    end, Nodes).
    
omt_sort_nodes_test() ->
    Nodes1 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeData}} =
            vtree_test:random_node({I,20+I*300,30-I*54}),
        Node = {NodeMbr, NodeMeta, {NodeId, NodeData}},
        [Node|Acc]
    end, [], lists:seq(1,6)),
    Sorted1 = omt_sort_nodes(Nodes1, 1),
    ?assertEqual([lists:nth(2, Nodes1),
        lists:nth(1, Nodes1),
        lists:nth(6, Nodes1),
        lists:nth(5, Nodes1),
        lists:nth(4, Nodes1),
        lists:nth(3, Nodes1)], Sorted1),
    Sorted2 = omt_sort_nodes(Nodes1, 2),
    ?assertEqual([lists:nth(3, Nodes1),
        lists:nth(5, Nodes1),
        lists:nth(6, Nodes1),
        lists:nth(2, Nodes1),
        lists:nth(4, Nodes1),
        lists:nth(1, Nodes1)], Sorted2).

% @doc Insert an new item into the seed tree
%     Reference:
%     Bulk insertion for R-trees by seeded clustering
-spec seedtree_insert(Tree::seedtree_root(), Node::tuple()) -> seedtree_root().
seedtree_insert(#seedtree_root{tree=Tree, outliers=Outliers}=Root, Node) ->
    case seedtree_insert_children([Tree], Node) of
    {ok, [Tree2]} ->
        Root#seedtree_root{tree=Tree2};
    {not_inserted, _} ->
        Root#seedtree_root{outliers=[Node|Outliers]}
    end.
-spec seedtree_insert_children(Children::[seedtree_node()],
        Node::seedtree_node()) ->
        {ok, seedtree_node()} | {not_inserted}.
seedtree_insert_children([], Node) ->
    {not_inserted, Node};
seedtree_insert_children(#seedtree_leaf{new=Old}=Children, Node) when
        not is_list(Children) ->
    %?debugMsg("done."),
    %?debugVal(Children),
    New = [Node|Old],
    Children2 = Children#seedtree_leaf{new=New},
    {ok, Children2};
seedtree_insert_children([H|T], Node) ->
    %?debugVal(H),
    %?debugVal(T),
    {Mbr, Meta, Children} = H,
    {NodeMbr, _, _Data} = Node,
    case vtree:within(NodeMbr, Mbr) of
    true ->
        {Status, Children2} = seedtree_insert_children(Children, Node),
        {Status, [{Mbr, Meta, Children2}|T]};
    false ->
        {Status, T2} = seedtree_insert_children(T, Node),
        {Status, [H|T2]}
    end.

seedtree_insert_test() ->
    {ok, {RootPos, Fd}} = vtree_test:build_random_tree("/tmp/randtree.bin", 20),
    ?debugVal(RootPos),
    SeedTree = seedtree(Fd, RootPos, 2),
    {NodeId, {NodeMbr, NodeMeta, NodeData}} = vtree_test:random_node(),
    Node = {NodeMbr, NodeMeta, {NodeId, NodeData}},
    SeedTree2 = seedtree_insert(SeedTree, Node),
    SeedTree2Tree = SeedTree2#seedtree_root.tree,
    ?assertEqual({{4,43,980,986},{node,inner},[
        {{4,43,980,960},{node,inner},[
            {{4,43,865,787},{node,inner},{seedtree_leaf,[6688,7127,7348],
                [{{66,132,252,718},{node,leaf},
                    {<<"Node718132">>,<<"Value718132">>}}]}},
            {{220,45,980,960},{node,inner},{seedtree_leaf,[6286,3391],[]}}]},
        {{27,163,597,986},{node,inner},[
            {{37,163,597,911},{node,inner},{seedtree_leaf,[3732,5606],[]}},
            {{27,984,226,986},{node,inner},{seedtree_leaf,[5039],[]}}]}]},
        SeedTree2Tree),
    NodeNotInTree3 = {{2,3,4,5}, NodeMeta, {<<"notintree">>, datafoo}},
    SeedTree3 = seedtree_insert(SeedTree, NodeNotInTree3),
    Outliers3 = SeedTree3#seedtree_root.outliers,
    ?assertEqual([{{2,3,4,5},{node,leaf},{<<"notintree">>,datafoo}}],
        Outliers3),
    NodeNotInTree4 = {{-2,300.5,4.4,50.45},{node,leaf},
        {<<"notintree2">>,datafoo2}},
    SeedTree4 = seedtree_insert(SeedTree2, NodeNotInTree4),
    Outliers4 = SeedTree4#seedtree_root.outliers,
    ?assertEqual(
        [{{-2,300.5,4.4,50.45},{node,leaf},{<<"notintree2">>,datafoo2}}],
        Outliers4
    ),
    SeedTree5 = seedtree_insert(SeedTree3, NodeNotInTree4),
    Outliers5 = SeedTree5#seedtree_root.outliers,
    ?assertEqual(
        [{{-2,300.5,4.4,50.45},{node,leaf},{<<"notintree2">>,datafoo2}},
         {{2,3,4,5},{node,leaf},{<<"notintree">>,datafoo}}],
        Outliers5
    ),
    Node6 = {{342,456,959,513}, NodeMeta, {<<"intree01">>, datafoo3}},
    SeedTree6 = seedtree_insert(SeedTree, Node6),
    SeedTree6Tree = SeedTree6#seedtree_root.tree,
    ?assertEqual({{4,43,980,986},{node,inner},[
        {{4,43,980,960},{node,inner},[
            {{4,43,865,787},{node,inner},{seedtree_leaf,[6688,7127,7348],[]}},
            {{220,45,980,960},{node,inner},{seedtree_leaf,[6286,3391],
                [{{342,456,959,513},{node,leaf},
                    {<<"intree01">>, datafoo3}}]}}]},
        {{27,163,597,986},{node,inner},[
            {{37,163,597,911},{node,inner},{seedtree_leaf,[3732,5606],[]}},
            {{27,984,226,986},{node,inner},{seedtree_leaf,[5039],[]}}]}]},
        SeedTree6Tree),
    SeedTree7 = seedtree_insert(SeedTree2, Node6),
    SeedTree7Tree = SeedTree7#seedtree_root.tree,
    ?assertEqual({{4,43,980,986},{node,inner},[
        {{4,43,980,960},{node,inner},[
            {{4,43,865,787},{node,inner},{seedtree_leaf,[6688,7127,7348],
                [{{66,132,252,718},{node,leaf},
                    {<<"Node718132">>,<<"Value718132">>}}]}},
            {{220,45,980,960},{node,inner},{seedtree_leaf,[6286,3391],
                [{{342,456,959,513},{node,leaf},
                    {<<"intree01">>, datafoo3}}]}}]},
        {{27,163,597,986},{node,inner},[
            {{37,163,597,911},{node,inner},{seedtree_leaf,[3732,5606],[]}},
            {{27,984,226,986},{node,inner},{seedtree_leaf,[5039],[]}}]}]},
        SeedTree7Tree).
%    SeedTree3 = lists:foldl(fun(I, STree) ->
%        {NodeId, {NodeMbr, NodeMeta, NodeData}} =
%            vtree_test:random_node({I,20,30}),
%        Node = {NodeMbr, NodeMeta, {NodeId, NodeData}},
%        seedtree_insert(STree, Node)
%    end, SeedTree, lists:seq(1,100)),
%    ?LOG_DEBUG(SeedTree3).

% @doc Put an on disk tree into memory and prepare it to store new nodes in
%     the leafs
-spec seedtree(Fd::file:io_device(), RootPos::integer(),
        MaxDepth::integer()) -> seedtree_root().
seedtree(Fd, RootPos, MaxDepth) ->
    Tree = seedtree(Fd, RootPos, MaxDepth, 0),
    #seedtree_root{tree=Tree}.
-spec seedtree(Fd::file:io_device(), RootPos::integer(),
        MaxDepth::integer(), Depth::integer()) -> seedtree_node().
seedtree(Fd, RootPos, MaxDepth, Depth) when Depth == MaxDepth ->
    {ok, Parent} = couch_file:pread_term(Fd, RootPos),
    {ParentMbr, ParentMeta, EntriesPos} = Parent,
    {ParentMbr, ParentMeta, #seedtree_leaf{orig=EntriesPos}};
seedtree(Fd, RootPos, MaxDepth, Depth) ->
    {ok, Parent} = couch_file:pread_term(Fd, RootPos),
    {ParentMbr, ParentMeta, EntriesPos} = Parent,
    Children = lists:foldl(fun(EntryPos, Acc) ->
        Child = seedtree(Fd, EntryPos, MaxDepth, Depth+1),
        [Child|Acc]
    end, [], EntriesPos),
    {ParentMbr, ParentMeta, lists:reverse(Children)}.

seedtree_test() ->
    {ok, {RootPos, Fd}} = vtree_test:build_random_tree("/tmp/randtree.bin", 20),
    ?debugVal(RootPos),
    SeedTree1 = seedtree(Fd, RootPos, 1),
    ?assertEqual({seedtree_root, {{4,43,980,986}, {node,inner},
        [{{4,43,980,960}, {node,inner},{seedtree_leaf,[7518,6520],[]}},
         {{27,163,597,986},{node,inner},{seedtree_leaf,[6006,5494],[]}}]},[]},
        SeedTree1),
    SeedTree2 = seedtree(Fd, RootPos, 2),
    ?assertEqual({seedtree_root, {{4,43,980,986}, {node,inner},
        [{{4,43,980,960}, {node,inner},
            [{{4,43,865,787},{node,inner},{seedtree_leaf,[6688,7127,7348],[]}},
             {{220,45,980,960},{node,inner},{seedtree_leaf,[6286,3391],[]}}]},
         {{27,163,597,986}, {node,inner},
            [{{37,163,597,911},{node,inner},{seedtree_leaf,[3732,5606],[]}},
             {{27,984,226,986},{node,inner},{seedtree_leaf,[5039],[]}}]}]},[]},
        SeedTree2).

% @doc Find the first node that encloses the input MBR completely at a certain
%     level. Return the the position relative to other children (starting
%     with 0).
-spec find_enclosing(Fd::file:io_device(), RootPos::integer(),
        Level::integer(), InputMbr::list()) -> integer().
find_enclosing(Fd, RootPos, Level, InputMbr) ->
    ok.

lookup_path_test() ->
    {ok, {RootPos, Fd}} = vtree_test:build_random_tree("/tmp/randtree.bin", 20),
    ?debugVal(RootPos),
    % Not inside 1st level
    EntryPath1 = lookup_path(Fd, RootPos, {1,3,2,4}, 2),
    ?assertEqual(-1, EntryPath1),

    % Inside 1st level but not inside 2nd level
    EntryPath2 = lookup_path(Fd, RootPos, {19,120,950,510}, 2),
    ?assertEqual(-1, EntryPath2),

    % Found in 2nd level
    EntryPath3 = lookup_path(Fd, RootPos, {542,356,698,513}, 2),
    ?assertEqual([0,0], EntryPath3),
    % Found in 3rd level (would also match [1,0,1], but we take the first
    % matching child)
    EntryPath4 = lookup_path(Fd, RootPos, {458,700,487,865}, 3),
    ?assertEqual([0,1,0], EntryPath4),

    EntryPath5 = lookup_path(Fd, RootPos, {542,656,598,813}, 3),
    ?assertEqual([0,1,0], EntryPath5),
    EntryPath6 = lookup_path(Fd, RootPos, {42,456,698,512}, 3),
    ?assertEqual([0,0,2], EntryPath6),
    EntryPath7 = lookup_path(Fd, RootPos, {342,456,959,513}, 3),
    ?assertEqual([0,1,1], EntryPath7).


%find_enclosing_test() ->
%    {ok, {RootPos, _Fd}} = vtree_test:build_random_tree("/tmp/randtree.bin", 1000),
%    %?LOG_DEBUG(RootNode).
%    ?debugVal(RootPos).


%%%%%% Helpers %%%%

% @doc Returns the ceiling of log_N(X)
-spec log_n_ceil(N::integer(), X::integer()) -> integer().
log_n_ceil(N, X) ->
    ceiling(math:log(X) / math:log(N)).

% From http://www.trapexit.org/Floating_Point_Rounding (2010-10-18)
-spec ceiling(X::number()) -> integer().
ceiling(X) when X < 0 ->
    trunc(X);
ceiling(X) ->
    T = trunc(X),
    case X - T == 0 of
        true -> T;
        false -> T + 1
    end.

% From http://www.trapexit.org/Floating_Point_Rounding (2010-10-18)
-spec floor(X::number()) -> integer().
floor(X) when X < 0 ->
    T = trunc(X),
    case X - T == 0 of
        true -> T;
        false -> T - 1
    end;
floor(X) -> 
    trunc(X).

% @doc split a list of elements into equally sized chunks (last element might
%     contain less elements)
-spec chunk_list(List::list(), Size::integer()) -> [list()].
chunk_list(List, Size) when Size == 0 ->
    [List];
chunk_list(List, Size) ->
    chunk_list(List, Size, 0, [], []).
chunk_list([], _Size, _Cnt, Chunk, Result) when length(Chunk) > 0 ->
    lists:reverse([lists:reverse(Chunk)|Result]);
chunk_list([], _Size, _Cnt, _Chunk, Result) ->
    lists:reverse(Result);
chunk_list([H|T], Size, Cnt, Chunk, Result) when Cnt < Size ->
    chunk_list(T, Size, Cnt+1, [H|Chunk], Result);
chunk_list([H|T], Size, _Cnt, Chunk, Result) ->
    chunk_list(T, Size, 1, [H], [lists:reverse(Chunk)|Result]).

% @doc split a list of elements into equally sized chunks (last element might
%     contain less elements). Applies Fun to every chunk and inserts the
%     return value of this function into the result list.
-spec chunk_list(Fun::fun(), List::list(), Size::integer()) -> [list()].
chunk_list(_Fun, List, Size) when Size == 0 ->
    [List];
chunk_list(Fun, List, Size) ->
    chunk_list(Fun, List, Size, 0, [], []).
% List is processed, but some items are left in the last chunk
chunk_list(Fun, [], _Size, _Cnt, Chunk, Result) when length(Chunk) > 0 ->
    Entry = Fun(lists:reverse(Chunk)),
    %lists:reverse(Entry ++ Result);
    lists:reverse([Entry|Result]);
chunk_list(_Fun, [], _Size, _Cnt, _Chunk, Result) ->
    lists:reverse(Result);
chunk_list(Fun, [H|T], Size, Cnt, Chunk, Result) when Cnt < Size ->
    chunk_list(Fun, T, Size, Cnt+1, [H|Chunk], Result);
% Chunk is complete, add chunk to result
chunk_list(Fun, [H|T], Size, _Cnt, Chunk, Result) ->
    Entry = Fun(lists:reverse(Chunk)),
    %chunk_list(Fun, T, Size, 1, [H], Entry ++ Result).
    chunk_list(Fun, T, Size, 1, [H], [Entry|Result]).

% XXX vmx: tests with function (check_list/6 ) are missing
chunk_list_test() ->
    List1 = [1,2,3,4,5],
    Chunked1 = chunk_list(List1, 2),
    ?assertEqual([[1,2],[3,4],[5]], Chunked1),
    Chunked2 = chunk_list(List1, 3),
    ?assertEqual([[1,2,3],[4,5]], Chunked2),
    Chunked3 = chunk_list(List1, 4),
    ?assertEqual([[1,2,3,4],[5]], Chunked3),
    Chunked4 = chunk_list(List1, 5),
    ?assertEqual([[1,2,3,4,5]], Chunked4),
    Chunked5 = chunk_list(List1, 6),
    ?assertEqual([[1,2,3,4,5]], Chunked5),
    Chunked6 = chunk_list(List1, 0),
    ?assertEqual([[1,2,3,4,5]], Chunked6),
    Chunked7 = chunk_list(List1, 1),
    ?assertEqual([[1],[2],[3],[4],[5]], Chunked7).
