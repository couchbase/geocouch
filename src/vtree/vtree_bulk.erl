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

% @doc Insert an new item into the seed tree
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
