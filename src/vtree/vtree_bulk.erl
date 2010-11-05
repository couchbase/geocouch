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

-export([omt_load/2, omt_write_tree/2]).

% XXX vmx: check if tree has correct meta information set for every node
%    (type=inner/type=leaf)

-define(LOG_DEBUG(Msg), io:format(user, "DEBUG: ~p~n", [Msg])).

% Nodes maximum filling grade (TODO vmx: shouldn't be hard-coded)
%-define(MAX_FILLED, 40).
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
% @doc OMT bulk loading. MaxNodes is the number of maximum children per node.
%     Returns the OMT tree and the height of the tree
%     Based on (but modified):
%     OMT: Overlap Minimizing Top-down Bulk Loading Algorithm for R-tree
-spec omt_load(Nodes::[omt_node()], MaxNodes::integer()) ->
        {omt_node(), integer()}.
omt_load([], _MaxNodes) ->
    [];
omt_load(Nodes, MaxNodes) ->
    NodesNum = length(Nodes),
    % Height of the final tree
    Height = log_n_ceil(MaxNodes, NodesNum),
    Omt = omt_load(Nodes, MaxNodes, 0, Height-1, 0),
    {Omt, Height}.
% all nodes need to be on the same level
-spec omt_load(Nodes::[omt_node()], MaxNodes::integer(), Dimension::integer(),
        LeafDepth::integer(), Depth::integer()) -> omt_node().
omt_load(Nodes, MaxNodes, _Dimension, LeafDepth, Depth) when
        length(Nodes) =< MaxNodes ->
    lists:foldl(fun(_I, Acc) ->
       [Acc]
    end, Nodes, lists:seq(1,LeafDepth-Depth));
omt_load(Nodes, MaxNodes, Dimension, LeafDepth, Depth) ->
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
        omt_load(InnerNodes, MaxNodes, Dimension+1, LeafDepth, Depth+1)
    end, Sorted, EntriesNum).


% @doc Write an OMT in memory tree to disk. Returns a list of tuples that
%     contain the MBR and
%     either:
%     - the position of the children in the file
%     or:
%     - the actual node value (if the tree consists only of one node).
-spec omt_write_tree(Fd::file:io_device(), Tree::list()) -> {ok, integer()}.
omt_write_tree(Fd, Tree) ->
    %?debugVal(Tree),
    %{level_done, [{_Mbr, Pos}]} = omt_write_tree(Fd, [Tree], 0, []),
    %{level_done, Foo} = omt_write_tree(Fd, Tree, 0, []),
    Return = case omt_write_tree(Fd, Tree, 0, []) of
    {level_done, Nodes} ->
        Nodes;
        %[{Mbr, #node{type=inner}, Pos} || {Mbr, Pos} <- Nodes];
    {leaf_nodes, MbrAndPos} ->
        %[{Mbr, Node} || {Mbr, _Meta, _Value}=Node <- Nodes]
        %Nodes
        [MbrAndPos]
    end,
    %Return = omt_write_tree(Fd, Tree, 0, []),
%?debugVal(Return),
    {ok, lists:reverse(Return)}.
% no more siblings
-spec omt_write_tree(Fd::file:io_device(), Tree::list(), Depth::integer(),
        Acc::list()) -> {ok, integer()}.
omt_write_tree(_Fd, [], _Depth, Acc) ->
    {no_siblings, Acc};
% leaf node
omt_write_tree(Fd, [H|_T]=Leafs, Depth, _Acc) when is_tuple(H) ->
    %{leaf_nodes, Leafs};
    Mbr = vtree:calc_nodes_mbr(Leafs),
    {ok, Pos} = couch_file:append_term(
            Fd, {Mbr, #node{type=leaf}, Leafs}),
    {leaf_nodes, {Mbr, Pos}};
omt_write_tree(Fd, [H|T], Depth, Acc) ->
    {_, Acc2} = case omt_write_tree(Fd, H, Depth+1, []) of
    {no_siblings, Siblings} ->
        {ok, Siblings};
%    {leaf_nodes, Leafs} ->
%        Mbr = vtree:calc_nodes_mbr(Leafs),
%        {ok, Pos} = couch_file:append_term(Fd,
%                            {Mbr, #node{type=leaf}, Leafs}),
%        {ok, [{Mbr, Pos}|Acc]};
    {leaf_nodes, MbrAndPos} ->
        {ok, [MbrAndPos|Acc]};
    {level_done, Level} ->
        % NOTE vmx: reversing Level is probably not neccessary
        {Mbrs, Children} = lists:unzip(lists:reverse(Level)),
        Mbr = vtree:calc_mbr(Mbrs),
        {ok, Pos} = couch_file:append_term(Fd,
                            {Mbr, #node{type=inner}, Children}),
        {ok, [{Mbr, Pos}|Acc]}
    end,
    {_, Acc3} = omt_write_tree(Fd, T, Depth, Acc2),
    %?debugVal(Depth),
    %?debugVal(Acc3),
    {level_done, Acc3}.


-spec omt_write_tree2(Fd::file:io_device(), Tree::list()) -> {ok, integer()}.
omt_write_tree2(Fd, Tree) ->
    {level_done, [{_Mbr, Pos}]} = omt_write_tree2(Fd, [Tree], []),
    {ok, Pos}.
% no more siblings
-spec omt_write_tree2(Fd::file:io_device(), Tree::list(), Acc::list()) ->
        {ok, integer()}.
omt_write_tree2(_Fd, [], Acc) ->
    {no_siblings, Acc};
% leaf node
omt_write_tree2(_Fd, [H|_T]=Leafs, _Acc) when is_tuple(H) ->
    {leaf_nodes, Leafs};
omt_write_tree2(Fd, [H|T], Acc) ->
    {_, Acc2} = case omt_write_tree2(Fd, H, []) of
    {no_siblings, Siblings} ->
        {ok, Siblings};
    {leaf_nodes, Leafs} ->
        Mbr = vtree:calc_nodes_mbr(Leafs),
        {ok, Pos} = couch_file:append_term(Fd,
                            {Mbr, #node{type=leaf}, Leafs}),
        {ok, [{Mbr, Pos}|Acc]};
    {level_done, Level} ->
        % NOTE vmx: reversing Level is probably not neccessary
        {Mbrs, Children} = lists:unzip(lists:reverse(Level)),
        Mbr = vtree:calc_mbr(Mbrs),
        {ok, Pos} = couch_file:append_term(Fd,
                            {Mbr, #node{type=inner}, Children}),
        {ok, [{Mbr, Pos}|Acc]}
    end,
    {_, Acc3} = omt_write_tree2(Fd, T, Acc2),
    {level_done, Acc3}.


% @doc Sort nodes by a certain dimension (which is the first element of the
%     node tuple)
-spec omt_sort_nodes(Nodes::[omt_node()], Dimension::integer()) -> [omt_node()].
% NOTE vmx: in the future the dimensions won't be stored in tuples, but
% in lists.
omt_sort_nodes(Nodes, Dimension) ->
    lists:sort(fun(Node1, Node2) ->
        Mbr1 = element(1, Node1),
        Mbr2 = element(1, Node2),
        Val1 = element(Dimension, Mbr1),
        Val2 = element(Dimension, Mbr2),
        Val1 =< Val2
    end, Nodes).


% @doc Insert several new items into the seed tree
%     Based on:
%     Bulk insertion for R-trees by seeded clustering
-spec seedtree_insert_list(Tree::seedtree_root(), _Nodes::[tuple()]) ->
        seedtree_root().
seedtree_insert_list(Root, []) ->
    Root;
seedtree_insert_list(Root, [H|T]=_Nodes) ->
    Root2 = seedtree_insert(Root, H),
    seedtree_insert_list(Root2, T).

% @doc Insert an new item into the seed tree
%     Based on:
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
% XXX TODO vmx: also keep the current depth, so we can determine whether
%     we need to recluster or not (3rd case in the paper).
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


% @doc Put an on disk tree into memory and prepare it to store new nodes in
%     the leafs. It contains a list of nodes in the leafs, not OMT trees, yet.
-spec seedtree_init(Fd::file:io_device(), RootPos::integer(),
        MaxDepth::integer()) -> seedtree_root().
seedtree_init(Fd, RootPos, MaxDepth) ->
    Tree = seedtree_init(Fd, RootPos, MaxDepth, 0),
    #seedtree_root{tree=Tree, height=MaxDepth}.
-spec seedtree_init(Fd::file:io_device(), RootPos::integer(),
        MaxDepth::integer(), Depth::integer()) -> seedtree_node().
% It's "Depth+1" as the root already contains several nodes (it's the nature
% of a R-Tree).
seedtree_init(Fd, RootPos, MaxDepth, Depth) when Depth+1 == MaxDepth ->
    {ok, Parent} = couch_file:pread_term(Fd, RootPos),
    {ParentMbr, ParentMeta, EntriesPos} = Parent,
    {ParentMbr, ParentMeta, #seedtree_leaf{orig=EntriesPos}};
seedtree_init(Fd, RootPos, MaxDepth, Depth) ->
    {ok, Parent} = couch_file:pread_term(Fd, RootPos),
    {ParentMbr, ParentMeta, EntriesPos} = Parent,
    Children = lists:foldl(fun(EntryPos, Acc) ->
        Child = seedtree_init(Fd, EntryPos, MaxDepth, Depth+1),
        [Child|Acc]
    end, [], EntriesPos),
    {ParentMbr, ParentMeta, lists:reverse(Children)}.

% @doc Find the first node that encloses the input MBR completely at a certain
%     level. Return the the position relative to other children (starting
%     with 0).
%-spec find_enclosing(Fd::file:io_device(), RootPos::integer(),
%        Level::integer(), InputMbr::list()) -> integer().
%find_enclosing(Fd, RootPos, Level, InputMbr) ->
%    ok.

%find_enclosing_test() ->
%    {ok, {RootPos, _Fd}} = vtree_test:build_random_tree("/tmp/randtree.bin", 1000),
%    %?LOG_DEBUG(RootNode).
%    ?debugVal(RootPos).


%% @doc Build a seedtree which can be written to disk as new vtree
%-spec seedtree_build(Fd::file:io_device(), RootPos::integer(),
%        MaxDepth::integer()) -> seedtree_root().
%seedtree_build(Fd, RootPos, MaxDepth) ->
%    Seedtree = seedtree_init(Fd, RootPos, MaxDepth),
%    seedtree_write(Fd, Seedtree).
    % XXX vmx: prepare the seed tree. Build appropriate OMT trees. Take into
    %     account if they are too small/big. The results is a tree that has
    %     a structure that could be written to disk,

    % XXX vmx: next step is reading that seedtree and do the post-processing
    %     (chapter 7 in the paper) on the nodes before actually writing them.
    %     Write the nodes after the processing. The tree can be written
    %     bottom up. New splits might happen while writing it.

% @doc Write an new vtree. TargetDepth is the height of the vtree where
%     the new nodes should be inserted in.
seedtree_write(Fd, Seedtree, TargetHeight) ->
    Tree = Seedtree#seedtree_root.tree,
?debugVal(length(Seedtree#seedtree_root.outliers)),
?debugVal(Seedtree#seedtree_root.outliers),
    ?LOG_DEBUG(Tree),
    % InsertHeight is the height the input tree needs to have so that it
    % can be inserted into the target tree
    %InsertHeight = TargetHeight - Seedtree#seedtree_root.height + 1,
    InsertHeight = TargetHeight - Seedtree#seedtree_root.height,
    ?debugVal(TargetHeight),
    ?debugVal(InsertHeight),
    %seedtree_write(Fd, [Tree], InsertHeight, []).
    %{level_done, [{_Mbr, Pos}]} = seedtree_write(Fd, [Tree], InsertHeight, []),
    {level_done, Level} = seedtree_write(Fd, [Tree], InsertHeight, []),
    ?debugMsg("almost done."),
    ?debugVal(Level),
    Root = if
    is_tuple(Level) ->
        Level;
    true ->
        ParentMbr = vtree:calc_nodes_mbr(Level),
        ChildrenPos = write_nodes(Fd, Level),
        {ParentMbr, #node{type=inner}, ChildrenPos}
    end,
    ?debugVal(Root),
%    {ok, Pos} = couch_file:append_term(Fd, Parent),
    {ok, Pos} = couch_file:append_term(Fd, Root),
    %{ok, Acc} = seedtree_write(Fd, [Tree], InsertHeight, []),
    %{level_done, [{_,_,[Pos]}]} = seedtree_write(Fd, [Tree], InsertHeight, []),

    % Insert outliers one by one
    {Pos2, TreeHeight} = lists:foldl(fun(
            {Mbr, Meta, {DocId, Value}}, {CurPos, _}) ->
        {ok, _NewMbr, CurPos2, TreeHeight} = vtree:insert(
                Fd, CurPos, DocId, {Mbr, Meta, Value}),
        {CurPos2, TreeHeight}
    end, {Pos, 0}, Seedtree#seedtree_root.outliers),

    {ok, Pos2}.
seedtree_write(_Fd, [], InsertHeight, Acc) ->
    ?debugMsg("No more siblings!!!!!!!!!!!!!!!!!!"),
    {no_more_siblings, Acc};
%seedtree_write(_Fd, [{_,_,Children}=H|_T]=Leafs, _Acc) when is_tuple(Children) ->
%seedtree_write(_Fd, [{_,_,Children}=H|_T]=Leafs, _Acc) when is_tuple(H) ->
%seedtree_write(_Fd, [{_,_,Children}=H|_T]=Leafs, _Acc) when is_tuple(Children) ->
%    ?debugVal(Leafs),
%    ?debugVal(Children),
%    {leafs, Leafs};
% No new nodes to insert
seedtree_write(_Fd, #seedtree_leaf{orig=Orig, new=[]}, _InsertHeight, _Acc) ->
    ?debugMsg("No new leafs!!!!!!!!!!!!!!!!!!"),
    {leafs, Orig};
% New nodes to insert
% This function returns a new list of children, as some children were
% rewritten due to repacking. The MBR doesn't change (that's the nature of
% this algorithm).
seedtree_write(Fd, #seedtree_leaf{orig=Orig, new=New}, InsertHeight, _Acc) ->
    ?debugMsg("leaf!!!!!!!!!!!!!!!!!!"),
    ?debugVal(Orig),
    ?debugVal(New),
    NewNum = length(New),
    %% Height of the OMT tree
    %Height = log_n_ceil(?MAX_FILLED, NewNum),
    % We can build the OMT tree now, as it is needed in any case
    {OmtTree, OmtHeight} = omt_load(New, ?MAX_FILLED),
    ?debugVal(OmtHeight),
    ?debugVal(OmtTree),

    % It's (Height + 1), as the OMT trees root node always needs to be a
    % sinlgle position in file (but the omt_load returns a root node with
    % multiple children).
    %HeightDiff = InsertHeight - (OmtHeight + 1),
    HeightDiff = InsertHeight - OmtHeight,
    ?debugVal(HeightDiff),
    NewChildrenPos2 = if
    % Input tree can be inserted as-is into the target tree
    HeightDiff == 0 ->
        NewChildren = seedtree_write_insert(Fd, Orig, OmtTree),
        MbrAndPos = seedtree_write_finish(NewChildren);
    HeightDiff > 0 ->
        ok = foo;
    % insert tree is too high => use its children
    HeightDiff < 0 ->
        % flatten the OMT tree until it has the expected height to be
        % inserted into the target tree (one subtree at a time).
        OmtTrees = lists:foldl(fun(_I, Acc) ->
            lists:append(Acc)
        end, OmtTree, lists:seq(1, abs(HeightDiff))),
        ?debugVal(length(OmtTrees)),
        ?debugVal(OmtTrees),

        % Insert the OMT trees one by one
        MbrAndPos2 = seedtree_write_insert(Fd, Orig, OmtTrees),
        {NewMbrs, NewPos} = lists:unzip(MbrAndPos2),
        ?debugVal({NewMbrs, NewPos}),

        % XXX vmx: Case (and test) is missing when node overflowed
        %     (>?MAX_FILLED^2 children). seedtree_write_finish/1 will
        %     probably go crazy
        MbrAndPos = seedtree_write_finish(lists:zip(NewMbrs, NewPos))
    end,
    % XXX vmx: Check NewChildrenPos2 for length. It might overflow the node
    %    => splitting the node, propagate it upwards etc.
    % XXX vmx: The idea will be to propage changes up, but only one level
    %    and not up to the root (as changes might also come from other
    %    siblings).
    {new_leaf, NewChildrenPos2};
seedtree_write(Fd, [{Mbr, Meta, Children}=H|T], InsertHeight, Acc) ->
    %?debugVal(H),
    %?debugVal(Children),
    %{ok, Acc2} = case seedtree_write(Fd, Children, []) of
    {ok, Acc2} = case seedtree_write(Fd, Children, InsertHeight, []) of
    {no_more_siblings, Siblings} ->
       ?debugMsg("No more siblings"),
       ?debugVal(Siblings),
       {ok, Siblings};
    {leafs, Orig} ->
       ?debugMsg("Leafs"),
       ?debugVal(Acc),
       {ok, [{Mbr, Meta, Orig}|Acc]};
    % This one might return more that one new node => level_done needs
    % to handle a possible overflow
    {new_leaf, Nodes} ->
       ?debugMsg("Returning new leaf"),

       % Nodes2 is a list of tuples that contain an MBR and positions
       % in file where children of the node with this MBR was written
       Nodes2 = lists:foldl(fun({NodeMbr, NodePos}, Acc2) ->
           [{NodeMbr, Meta, NodePos}|Acc2]
       end, Acc, Nodes),
       %{ok, [{Mbr, Meta, NewChildren}|Acc]};
       ?debugVal(Nodes2),
       %?debugVal(Acc),
       %{ok, [Nodes2|Acc]};
       {ok, Nodes2};
    % Node can be written as-is, as the number of children is low enough
    {level_done, Level} when length(Level) =< ?MAX_FILLED->
        ?debugMsg("Level done (fits in)"),
        ?debugVal(Level),
        ?debugVal(Acc),
        %{Mbrs, Meta, Children} = lists:unzip3(lists:reverse(Level)),
        % XXX vmx: why is it a 3-tuple and not a 2-tuple? because
        %     of no_siblings? Are nodes not written yet?
        ParentMbr = vtree:calc_nodes_mbr(Level),
%?debugVal(Children),
        ChildrenPos = write_nodes(Fd, Level),
        % XXX vmx: Not sure if type=inner is always right
        Parent = {ParentMbr, #node{type=inner}, ChildrenPos},
%        {ok, Pos} = couch_file:append_term(Fd, Parent),
        %{ok, [{Mbr, Pos}|Acc]}

        %{ok, [{foo, pos}|Acc]}
        %{ok, [Parent|Acc]}
        %{ok, [Pos|Acc]}
        %{ok, [{ParentMbr, Pos}|Acc]}m
        {ok, [Parent|Acc]};
        %{ok, Parent}
        %{ok, [Parent|LevelAcc]}
    % Node needs to be split. Normal split algorithm (like Ang/Tan) could be
    % used. We use OMT.
    {level_done, Level} when length(Level) > ?MAX_FILLED ->
        ?debugMsg("Level done (doesn't fit in)"),
        ?debugVal(Level),
        {Level2, _} = omt_load(Level, ?MAX_FILLED),

        Parents = lists:foldl(fun(Level3, LevelAcc) ->
            ParentMbr = vtree:calc_nodes_mbr(Level3),
            ChildrenPos = write_nodes(Fd, Level3),
            % XXX vmx: Not sure if type=inner is always right
            Parent = {ParentMbr, #node{type=inner}, ChildrenPos},
            [Parent|LevelAcc]
        end, Acc, Level2),
        ?debugVal(Parents),

        %{ok, [Parents|Acc]}
        {ok, Parents}
    end,
    ?debugVal(T),
    {Info, Acc3} = seedtree_write(Fd, T, InsertHeight, Acc2),
%    ?debugVal(Info),
    {level_done, Acc3}.
%    {Info, Acc3} = case seedtree_write(Fd, T, InsertHeight, Acc2) of
%    {level_done, Level2, LevelAcc2} ->
%        ?debugMsg("Level done 222222222222222222222"),
%        ?debugVal(Level2),
%        ?debugVal(Acc),
%        ?debugVal(Acc2),
%        %{Mbrs2, Children2} = lists:unzip(lists:reverse(Level2)),
%        ParentMbr2 = vtree:calc_nodes_mbr(Level2),
%        ChildrenPos2 = write_nodes(Fd, Level2),
%        % XXX vmx: Not sure if type=inner is always right
%        Parent2 = {ParentMbr2, #node{type=inner}, ChildrenPos2},
%        ?debugVal(Parent2),
%        %{ok, Pos2} = couch_file:append_term(Fd, Parent2),
%        %{ok, [{ParentMbr, #node{type=inner}, Pos}|Acc]};
%        %{ok, [Parent2|Acc2]};
%        %{ok, Parent2};
%        %{ok, [Parent2|Acc]};
%        {ok, [Parent2|LevelAcc2]};
%    {no_more_siblings, Siblings2} ->
%        ?debugMsg("No more siblings 222222222222222222222"),
%        ?debugVal(Siblings2),
%        %{ok, Siblings2}  
%        {level_done, Siblings2, []}  
%    end,
%    ?debugVal(Acc3),
%    %{level_done, Acc3}.
%    %{ok, Acc3}.
%    {Info, Acc3}.

% @doc Writes new nodes into the existing tree. The nodes, resp. the
% height of the OMT tree, must fit into the target tree.
% Orig are the original child nodes, New are the nodes that should be
% inserted.
% Returns a tuple with the MBR and the position of the root node in the file
-spec seedtree_write_insert(Fd::file:io_device(), Orig::list(),
        OmtTree::omt_node()) -> [{tuple(), integer()}].
seedtree_write_insert(Fd, Orig, OmtTree) ->
    % write the OmtTree to to disk.
    {ok, OmtMbrAndPos} = omt_write_tree(Fd, OmtTree),
    ?debugVal(OmtMbrAndPos),

    % Get the enclosing MBR of the OMT nodes
    {OmtMbrs, _OmtPos} = lists:unzip(OmtMbrAndPos),
    OmtMbr = vtree:calc_mbr(OmtMbrs),

    % We do some repacking for better perfomance. First get the children
    % of the target node where the new nodes will be inserted in
    TargetChildren = load_nodes(Fd, Orig),
    ?debugVal(TargetChildren),

    % Transform the target nodes from normal ones to tuples where the
    % meta information is replaced with the position of the node in
    % the file.
    TargetMbrAndPos = lists:zipwith(fun({Mbr, _, Children}, Pos) ->
        {Mbr, Pos, Children}
    end, TargetChildren, Orig),
    ?debugVal(TargetMbrAndPos),

    % Get all nodes that are within or intersect with the input tree
    % root node
    {Disjoint, NotDisjoint} = lists:partition(fun({Mbr, _Pos, _}) ->
        vtree:disjoint(Mbr, OmtMbr)
    end, TargetMbrAndPos),
    ?debugVal(Disjoint),
    ?debugVal(NotDisjoint),

    % Get the children of those nodes, that intersect the input MBR,
    % so that they can be repacked for better query performance
    PosToRepack = [Children || {_Mbr, _Pos, Children} <- NotDisjoint],
    ?debugVal(PosToRepack),

    % And reduce the disjoint nodes to MBR and position in file only
    DisjointMbrAndPos = [{Mbr, Pos} || {Mbr, Pos, _Children} <- Disjoint],

    % Get the actual nodes that should be repacked
    NodesToRepack = case lists:append(PosToRepack) of
    PosToRepack2 when is_tuple(hd(PosToRepack2)) ->
        % Reached leaf nodes
        PosToRepack2;
     PosToRepack2 ->
        load_nodes(Fd, PosToRepack2)
    end,
    %PosToRepack2 = lists:append(PosToRepack),
    %NodesToRepack = load_nodes(Fd, PosToRepack2),
    ?debugVal(NodesToRepack),

    % Transform the nodes to repack from normal ones to tuples (consisting
    % of the MBR and the position where the node in the file (or the nodes
    % themselves if they are leaf nodes)
    ToRepackMbrAndPos = lists:zipwith(fun({Mbr, _, _}, Pos) ->
        {Mbr, Pos}
    end, NodesToRepack, PosToRepack2),
    ?debugVal(ToRepackMbrAndPos),

    % Building an OMT tree is used as split algorithm. The advantage over
    % a normal one (line Ang/Tan) is that it can split several levels at
    % one time (and not only split into two partitions)
    {NewNodes, NewNodesHeight} = omt_load(
        ToRepackMbrAndPos ++ OmtMbrAndPos, ?MAX_FILLED),
    ?debugVal(NewNodes),
    ?debugVal(NewNodesHeight),

    % The tree has a height of 3 if an additional split is needed. If
    % the tree has a height of 2 an additional split might not be needed,
    % but still can be needed (especially if there a disjoint nodes).
    % Therefore collect those nodes and see if the split is needed when
    % all nodes can be considered.
    NewNodes2 = case NewNodesHeight of
    2 ->
        NewNodes;
    3 -> 
        lists:append(NewNodes)
    end,
    ?debugVal(NewNodes2),
    ?debugVal(length(NewNodes2)),

    % Loop through those new nodes, calculate MBRs and write them to disk.
    % Return the Mbr and positions in file of the newly written nodes.
    % And prepend them to the disjoint nodes
    %NewNodesMbrAndPos = lists:foldl(fun(Nodes, Acc) ->
    NewChildren = lists:foldl(fun(Nodes, Acc) ->
%        ?debugVal(Nodes),
        {Mbrs, Pos} = lists:unzip(Nodes),
        ParentMbr = vtree:calc_mbr(Mbrs),
        {ok, ParentPos} = couch_file:append_term(
                Fd, {ParentMbr, #node{type=inner}, Pos}),
        [{ParentMbr, ParentPos}|Acc]
    end, DisjointMbrAndPos, NewNodes2),
    ?debugVal(NewChildren),
    NewChildren.

% @doc Creates new parent nodes out of a list of tuples containing MBRs and
%     positions (splits as needed). Returns MBR and position in file of the
%     new parent node(s)
-spec seedtree_write_finish(NewChildren::[{tuple(), integer()}]) ->
        [{tuple(), integer()}].
seedtree_write_finish(NewChildren) ->
    % There might be an overflow of nodes. Now a normal split algorithm
    % (like Ang/Tan) could be used. But we might also just use the OMT
    % algorithm.
    %NewNodes4 = case length(NewNodes3) > ?MAX_FILLED of
    %NewChildren = Disjoint ++ NewNodes3, 
    NewChildren3 = case length(NewChildren) > ?MAX_FILLED of
    true ->
        {NewChildren2, _} = omt_load(NewChildren, ?MAX_FILLED),
        NewChildren2;
    false ->
        [NewChildren]
    end,
    ?debugVal(NewChildren3),

    % XXX vmx: it is implicitly expected that NewChildren3 doesn't
    %     contain a nested list, though this could happen if NewChildren
    %     is bigger than math:pow(?MAX_FILLED, 2).


    % Calculate the common MBR. The result is a tuple of the MBR and
    % the children positions in file it encloses
    NewChildrenMbrAndPos = lists:reverse(lists:foldl(fun(Nodes, Acc) ->
        {Mbrs, Poss} = lists:unzip(Nodes),
        Mbr = vtree:calc_mbr(Mbrs),
        [{Mbr, Poss}|Acc]
    end, [], NewChildren3)),
    ?debugVal(NewChildrenMbrAndPos),

    % Add the new nodes to the old disjoint ones
    %NewChildrenPos = Disjoint ++ lists:reverse(NewNodes3),
    %?debugVal(length(NewChildrenPos)),
    %{new_leaf, NewChildrenPos};
    %NewChildrenPos;

    % Return new nodes (might be several ones)
    NewChildrenMbrAndPos.

% XXX vmx: rename to _test when it should be run again
seedtree_write_case1_test() ->
    % Test "Case 1: input R-tree fits in the target node" (chapter 6.1)
    % Test 1.1: input tree fits in
    % NOTE vmx: seedtree's maximum height should/must be height of the
    %    targetree - 2
    % XXX vmx: This test creates a strange root node
    TargetTreeNodeNum = 25,
    {ok, {Fd, {RootPos, TargetTreeHeight}}} = vtree_test:build_random_tree(
            "/tmp/seedtree_write.bin", TargetTreeNodeNum),

    ?debugVal(RootPos),
    Nodes1 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeData}} =
            vtree_test:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {NodeId, NodeData}},
        [Node|Acc]
    end, [], lists:seq(1,20)),

    Seedtree1 = seedtree_init(Fd, RootPos, 1),
    ?debugVal(Seedtree1),
    Seedtree2 = seedtree_insert_list(Seedtree1, Nodes1),
    {ok, ResultPos1} = seedtree_write(Fd, Seedtree2, TargetTreeHeight),
    ?debugVal(ResultPos1),
    {ok, Lookup1} = vtree:lookup(Fd, ResultPos1, {0,0,1001,1001}),
    ?assertEqual(45, length(Lookup1)),

    % Test 1.2: input tree produces splits (seedtree height=1)
    TargetTreeNodeNum2 = 64,
    TargetTreeHeight2 = log_n_ceil(?MAX_FILLED, TargetTreeNodeNum2),
    {Nodes2, Fd2, RootPos2} = create_random_nodes_and_packed_tree(
        12, TargetTreeNodeNum2, ?MAX_FILLED),

    Seedtree2_1 = seedtree_init(Fd2, RootPos2, 1),
    ?debugVal(Seedtree2_1),
    Seedtree2_2 = seedtree_insert_list(Seedtree2_1, Nodes2),
    {ok, ResultPos2} = seedtree_write(Fd2, Seedtree2_2, TargetTreeHeight2),
    ?debugVal(ResultPos2),
    {ok, Lookup2} = vtree:lookup(Fd2, ResultPos2, {0,0,1001,1001}),
    ?assertEqual(76, length(Lookup2)),

    % Test 1.3: input tree produces splits (recursively) (seedtree height=2)
    % It would create a root node with 5 nodes
    TargetTreeNodeNum3 = 196,
    TargetTreeHeight3 = log_n_ceil(4, TargetTreeNodeNum3),
    ?debugVal(TargetTreeHeight3),
    {Nodes3, Fd3, RootPos3} = create_random_nodes_and_packed_tree(
        12, TargetTreeNodeNum3, 4),
    Seedtree3_1 = seedtree_init(Fd3, RootPos3, 2),
    ?debugVal(Seedtree3_1),
    Seedtree3_2 = seedtree_insert_list(Seedtree3_1, Nodes3),
    {ok, ResultPos3} = seedtree_write(Fd3, Seedtree3_2, TargetTreeHeight3),
    ?debugVal(ResultPos3),
    {ok, Lookup3} = vtree:lookup(Fd3, ResultPos3, {0,0,1001,1001}),
    ?assertEqual(208, length(Lookup3)),

    % Test 1.4: input tree produces splits (recursively) (seedtree height=3)
    TargetTreeNodeNum4 = 900,
    TargetTreeHeight4 = log_n_ceil(4, TargetTreeNodeNum4),
    ?debugVal(TargetTreeHeight4),
    {Nodes4, Fd4, RootPos4} = create_random_nodes_and_packed_tree(
        14, TargetTreeNodeNum4, 4),
    Seedtree4_1 = seedtree_init(Fd4, RootPos4, 3),
    ?debugVal(Seedtree4_1),
    Seedtree4_2 = seedtree_insert_list(Seedtree4_1, Nodes4),
    {ok, ResultPos4} = seedtree_write(Fd4, Seedtree4_2, TargetTreeHeight4),
    ?debugVal(ResultPos4),
    {ok, Lookup4} = vtree:lookup(Fd4, ResultPos4, {0,0,1001,1001}),
    ?assertEqual(914, length(Lookup4)),

    % Test 1.5: adding new data with height=4 (seedtree height=1)
    TargetTreeNodeNum5 = 800,
    TargetTreeHeight5 = log_n_ceil(4, TargetTreeNodeNum5),
    ?debugVal(TargetTreeHeight5),
    {Nodes5, Fd5, RootPos5} = create_random_nodes_and_packed_tree(
        251, TargetTreeNodeNum5, 4),
    Seedtree5_1 = seedtree_init(Fd5, RootPos5, 1),
    ?debugVal(Seedtree5_1),
    Seedtree5_2 = seedtree_insert_list(Seedtree5_1, Nodes5),
    {ok, ResultPos5} = seedtree_write(Fd5, Seedtree5_2, TargetTreeHeight5),
    ?debugVal(ResultPos5),
    {ok, Lookup5} = vtree:lookup(Fd5, ResultPos5, {0,0,1001,1001}),
    ?assertEqual(1051, length(Lookup5)).


seedtree_write_case2_test() ->
    % Test "Case 2: input R-tree is not shorter than the level of the
    % target node" (chapter 6.2)
    % Test 2.1: input tree is too high (1 level)
    TargetTreeNodeNum = 25,
    % NOTE vmx: wonder why this tree is not really dense
    {ok, {Fd, {RootPos, TargetTreeHeight}}} = vtree_test:build_random_tree(
            "/tmp/seedtree_write.bin", TargetTreeNodeNum),
    ?debugVal(TargetTreeHeight),
    ?debugVal(RootPos),
    Nodes1 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeData}} =
            vtree_test:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {[NodeId, <<"bulk">>], NodeData}},
        [Node|Acc]
    end, [], lists:seq(1,80)),

    Seedtree1 = seedtree_init(Fd, RootPos, 1),
    ?debugVal(Seedtree1),
    Seedtree2 = seedtree_insert_list(Seedtree1, Nodes1),
    {ok, ResultPos1} = seedtree_write(Fd, Seedtree2, TargetTreeHeight),
    ?debugVal(ResultPos1),
    {ok, Lookup1} = vtree:lookup(Fd, ResultPos1, {0,0,1001,1001}),
    ?assertEqual(105, length(Lookup1)),

    % Test 2.2: input tree is too high (2 levels)
    Nodes2 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeData}} =
            vtree_test:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {[NodeId, <<"bulk">>], NodeData}},
        [Node|Acc]
    end, [], lists:seq(1,300)),

    Seedtree2_2 = seedtree_insert_list(Seedtree1, Nodes2),
    {ok, ResultPos2_1} = seedtree_write(Fd, Seedtree2_2, TargetTreeHeight),
    ?debugVal(ResultPos2_1),
    {ok, Lookup2_1} = vtree:lookup(Fd, ResultPos2_1, {0,0,1001,1001}),
    ?assertEqual(325, length(Lookup2_1)),

    % Test 2.3: input tree is too high (1 level) and procudes split
    % (seedtree height=1)
    TargetTreeNodeNum3 = 64,
    TargetTreeHeight3 = log_n_ceil(?MAX_FILLED, TargetTreeNodeNum3),
    {Nodes3, Fd3, RootPos3} = create_random_nodes_and_packed_tree(
        50, TargetTreeNodeNum3, ?MAX_FILLED),

    Seedtree3_1 = seedtree_init(Fd3, RootPos3, 1),
    ?debugVal(Seedtree3_1),
    Seedtree3_2 = seedtree_insert_list(Seedtree3_1, Nodes3),
    {ok, ResultPos3} = seedtree_write(Fd3, Seedtree3_2, TargetTreeHeight3),
    ?debugVal(ResultPos3),
    {ok, Lookup3} = vtree:lookup(Fd3, ResultPos3, {0,0,1001,1001}),
    ?assertEqual(114, length(Lookup3)),

    % Test 2.4: input tree is too high (1 level) and produces multiple
    % splits (recusively) (seedtree height=2)
    % XXX vmx: not really sure if there's more than one split
    TargetTreeNodeNum4 = 196,
    TargetTreeHeight4 = log_n_ceil(4, TargetTreeNodeNum4),
    ?debugVal(TargetTreeHeight4),
    {Nodes4, Fd4, RootPos4} = create_random_nodes_and_packed_tree(
        50, TargetTreeNodeNum4, 4),
    Seedtree4_1 = seedtree_init(Fd4, RootPos4, 2),
    ?debugVal(Seedtree4_1),
    Seedtree4_2 = seedtree_insert_list(Seedtree4_1, Nodes4),
    {ok, ResultPos4} = seedtree_write(Fd4, Seedtree4_2, TargetTreeHeight4),
    ?debugVal(ResultPos4),
    {ok, Lookup4} = vtree:lookup(Fd4, ResultPos4, {0,0,1001,1001}),
    ?assertEqual(246, length(Lookup4)),

    % Test 2.5: input tree is too hight (2 level) and produces multiple
    % splits (recusively) (seedtree height=2)
    % XXX vmx: not really sure if there's more than one split
    TargetTreeNodeNum5 = 196,
    TargetTreeHeight5 = log_n_ceil(4, TargetTreeNodeNum5),
    ?debugVal(TargetTreeHeight5),
    {Nodes5, Fd5, RootPos5} = create_random_nodes_and_packed_tree(
        100, TargetTreeNodeNum5, 4),
    Seedtree5_1 = seedtree_init(Fd5, RootPos5, 2),
    ?debugVal(Seedtree5_1),
    Seedtree5_2 = seedtree_insert_list(Seedtree5_1, Nodes5),
    {ok, ResultPos5} = seedtree_write(Fd5, Seedtree5_2, TargetTreeHeight5),
    ?debugVal(ResultPos5),
    {ok, Lookup5} = vtree:lookup(Fd5, ResultPos5, {0,0,1001,1001}),
    ?assertEqual(296, length(Lookup5)).


% @doc Loads nodes from file
-spec load_nodes(Fd::file:io_device(), Positions::[integer()]) -> list().
load_nodes(Fd, Positions) ->
    load_nodes(Fd, Positions, []).
-spec load_nodes(Fd::file:io_device(), Positions::[integer()], Acc::list()) ->
        list().
load_nodes(_Fd, [], Acc) ->
    lists:reverse(Acc);
load_nodes(Fd, [H|T], Acc) ->
    {ok, Node} = couch_file:pread_term(Fd, H),
    load_nodes(Fd, T, [Node|Acc]).

% @doc Write nodes to file, return their positions
-spec write_nodes(Fd::file:io_device(), Nodes::[tuple()]) -> list().
write_nodes(Fd, Nodes) ->
    write_nodes(Fd, Nodes, []).
-spec write_nodes(Fd::file:io_device(), Nodes::[tuple()], Acc::list()) ->
        list().
write_nodes(_Fd, [], Acc) ->
    lists:reverse(Acc);
write_nodes(Fd, [H|T], Acc) ->
    {ok, Pos} = couch_file:append_term(Fd, H),
    write_nodes(Fd, T, [Pos|Acc]).



%%%%% Helpers %%%%%

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
%-spec floor(X::number()) -> integer().
%floor(X) when X < 0 ->
%    T = trunc(X),
%    case X - T == 0 of
%        true -> T;
%        false -> T - 1
%    end;
%floor(X) -> 
%    trunc(X).

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
        {NodeId, {NodeMbr, NodeMeta, NodeData}} =
            vtree_test:random_node({I,20+I*250,30}),
        Node = {NodeMbr, NodeMeta, {NodeId, NodeData}},
        [Node|Acc]
    end, [], lists:seq(1, TreeNodeNum)),
    {Omt, _OmtHeight} = omt_load(OmtNodes, MaxFilled),
    {ok, MbrAndPosList} = omt_write_tree(Fd, Omt),
    %?debugVal(MbrAndPosList),
    {Mbrs, Poss} = lists:unzip(MbrAndPosList),
    Mbr = vtree:calc_mbr(Mbrs),
    {ok, RootPos} = couch_file:append_term(Fd, {Mbr, #node{type=inner}, Poss}),

    ?debugVal(RootPos),
    Nodes = lists:foldl(fun(I, Acc) ->
        %{NodeId, {NodeMbr, NodeMeta, NodeData}} =
        %    vtree_test:random_node({I,23+I*309,45}),
        %Node = {NodeMbr, NodeMeta, {NodeId, NodeData}},
        Node = {{200+I,250+I,300+I,350+I}, #node{type=leaf},
            {"Node-" ++ integer_to_list(I), "Value-" ++ integer_to_list(I)}},
        [Node|Acc]
    end, [], lists:seq(1, NodesNum)),

    {Nodes, Fd, RootPos}.


%%%%% Tests %%%%%


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


lookup_path_test() ->
    {ok, {Fd, {RootPos, _}}} = vtree_test:build_random_tree(
            "/tmp/randtree.bin", 20),
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


omt_load_test() ->
    Nodes1 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeData}} =
            vtree_test:random_node({I,20+I*250,30}),
        Node = {NodeMbr, NodeMeta, {NodeId, NodeData}},
        [Node|Acc]
    end, [], lists:seq(1,9)),
    {Omt1, _OmtHeight1} = omt_load(Nodes1, 2),
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
    ?assert(is_tuple(L4a)),
    ?assert(is_tuple(L4b)),
    ?assert(is_tuple(L4c)),
    ?assert(is_tuple(L4d)),
    ?assert(is_tuple(L4e)),
    ?assert(is_tuple(L4f)),
    ?assert(is_tuple(L4g)),
    ?assert(is_tuple(L4h)),
    ?assert(is_tuple(L4i)),

    {Omt2, _OmtHeight2} = omt_load(Nodes1, 4),
    [M1a, M1b, M1c] = Omt2,
    [M2a, M2b, M2c] = M1a,
    [M2d, M2e, M2f] = M1b,
    [M2g, M2h, M2i] = M1c,
    ?assert(is_tuple(M2a)),
    ?assert(is_tuple(M2b)),
    ?assert(is_tuple(M2c)),
    ?assert(is_tuple(M2d)),
    ?assert(is_tuple(M2e)),
    ?assert(is_tuple(M2f)),
    ?assert(is_tuple(M2g)),
    ?assert(is_tuple(M2h)),
    ?assert(is_tuple(M2i)),

    {Omt3, _Omtheight3} = omt_load(Nodes1, 12),
    ?assertEqual(9, length(Omt3)).



omt_write_tree_test() ->
    Filename = "/tmp/omt.bin",
    Nodes1 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeData}} =
            vtree_test:random_node({I,20+I*250,30}),
        Node = {NodeMbr, NodeMeta, {NodeId, NodeData}},
        [Node|Acc]
    end, [], lists:seq(1,9)),
    {Omt1, _OmtHeight1} = omt_load(Nodes1, 2),

    Fd = case couch_file:open(Filename, [create, overwrite]) of
    {ok, Fd2} ->
        Fd2;
    {error, _Reason} ->
        io:format("ERROR: Couldn't open file (~s) for tree storage~n",
                  [Filename])
    end,
    
    {ok, MbrAndPosList} = omt_write_tree(Fd, Omt1),
    RootPosList = [Pos || {Mbr, Pos} <- MbrAndPosList],
    ?debugVal(RootPosList),

    % just test if the number of children match on all levels
    ?assertEqual(2, length(RootPosList)),
    {ok, L1a} = couch_file:pread_term(Fd, lists:nth(1, RootPosList)),
    ?assertEqual(2, length(element(3, L1a))),
    {ok, L1b} = couch_file:pread_term(Fd, lists:nth(2, RootPosList)),
    ?assertEqual(2, length(element(3, L1b))),
    {ok, L2a} = couch_file:pread_term(Fd, lists:nth(1, element(3, L1a))),
    ?assertEqual(2, length(element(3, L2a))),
    {ok, L2b} = couch_file:pread_term(Fd, lists:nth(2, element(3, L1a))),
    ?assertEqual(1, length(element(3, L2b))),
    {ok, L2c} = couch_file:pread_term(Fd, lists:nth(1, element(3, L1b))),
    ?assertEqual(1, length(element(3, L2c))),
    {ok, L2d} = couch_file:pread_term(Fd, lists:nth(2, element(3, L1b))),
    ?assertEqual(1, length(element(3, L2d))),
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
    ?assert(is_tuple(L4a)),
    ?assert(is_tuple(L4b)),
    ?assert(is_tuple(L4c)),
    ?assert(is_tuple(L4d)),
    ?assert(is_tuple(L4e)),
    ?assert(is_tuple(L4f)),
    ?assert(is_tuple(L4g)),
    ?assert(is_tuple(L4h)),
    ?assert(is_tuple(L4i)),


    {Omt2, _OmtHeight2} = omt_load(Nodes1, 4),
    {ok, MbrAndPosList2} = omt_write_tree(Fd, Omt2),
    RootPosList2 = [Pos || {Mbr, Pos} <- MbrAndPosList2],
    ?debugVal(RootPosList2),
    ?assertEqual(3, length(RootPosList2)),
    {ok, M1aNode} = couch_file:pread_term(Fd, lists:nth(1, RootPosList2)),
    {_, _, M1a} = M1aNode,
    ?assertEqual(3, length(M1a)),
    {ok, M1bNode} = couch_file:pread_term(Fd, lists:nth(2, RootPosList2)),
    {_, _, M1b} = M1bNode,
    ?assertEqual(3, length(M1b)),
    {ok, M1cNode} = couch_file:pread_term(Fd, lists:nth(3, RootPosList2)),
    {_, _, M1c} = M1cNode,
    ?assertEqual(3, length(M1c)),
    [M2a, M2b, M2c] = M1a,
    [M2d, M2e, M2f] = M1b,
    [M2g, M2h, M2i] = M1c,
    ?assert(is_tuple(M2a)),
    ?assert(is_tuple(M2b)),
    ?assert(is_tuple(M2c)),
    ?assert(is_tuple(M2d)),
    ?assert(is_tuple(M2e)),
    ?assert(is_tuple(M2f)),
    ?assert(is_tuple(M2g)),
    ?assert(is_tuple(M2h)),
    ?assert(is_tuple(M2i)),

    % Test round-trip: some nodes => OMT tree, write to disk, load from disk
    {ok, LeafNodes1} = vtree:lookup(
            Fd, lists:nth(1, RootPosList), {0,0,1001,1001}),
    {ok, LeafNodes2} = vtree:lookup(
            Fd, lists:nth(2, RootPosList), {0,0,1001,1001}),
    LeafNodes3 = lists:foldl(fun(LeafNode, Acc) ->
        {NodeMbr, NodeId, NodeData} = LeafNode,
        Node = {NodeMbr, #node{type=leaf}, {NodeId, NodeData}},
        [Node|Acc]
    end, [], LeafNodes1 ++ LeafNodes2),
    {Omt3, _OmtHeight3} = omt_load(LeafNodes3, 2),
    ?assertEqual(Omt1, Omt3).


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


seedtree_insert_test() ->
    {ok, {Fd, {RootPos, _}}} = vtree_test:build_random_tree(
            "/tmp/randtree.bin", 20),
    ?debugVal(RootPos),
    SeedTree = seedtree_init(Fd, RootPos, 3),
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


seedtree_insert_list_test() ->
    {ok, {Fd, {RootPos, _}}} = vtree_test:build_random_tree(
            "/tmp/randtree.bin", 20),
    ?debugVal(RootPos),
    SeedTree = seedtree_init(Fd, RootPos, 3),
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
    NodeNotInTree4 = {{-2,300.5,4.4,50.45},{node,leaf},
        {<<"notintree2">>,datafoo2}},
    SeedTree5 = seedtree_insert_list(SeedTree,
        [NodeNotInTree3, NodeNotInTree4]),
    Outliers5 = SeedTree5#seedtree_root.outliers,
    ?assertEqual(
        [{{-2,300.5,4.4,50.45},{node,leaf},{<<"notintree2">>,datafoo2}},
         {{2,3,4,5},{node,leaf},{<<"notintree">>,datafoo}}],
        Outliers5
    ),

    Node6 = {{342,456,959,513}, NodeMeta, {<<"intree01">>, datafoo3}},
    SeedTree7 = seedtree_insert_list(SeedTree, [Node, Node6]),
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



seedtree_init_test() ->
    {ok, {Fd, {RootPos, _}}} = vtree_test:build_random_tree(
            "/tmp/randtree.bin", 20),
    ?debugVal(RootPos),
    SeedTree1 = seedtree_init(Fd, RootPos, 2),
    ?assertEqual({seedtree_root, {{4,43,980,986}, {node,inner},
        [{{4,43,980,960}, {node,inner},{seedtree_leaf,[7518,6520],[]}},
         {{27,163,597,986},{node,inner},{seedtree_leaf,[6006,5494],[]}}]},
         [], 2},
        SeedTree1),
    SeedTree2 = seedtree_init(Fd, RootPos, 3),
    ?assertEqual({seedtree_root, {{4,43,980,986}, {node,inner},
        [{{4,43,980,960}, {node,inner},
            [{{4,43,865,787},{node,inner},{seedtree_leaf,[6688,7127,7348],[]}},
             {{220,45,980,960},{node,inner},{seedtree_leaf,[6286,3391],[]}}]},
         {{27,163,597,986}, {node,inner},
            [{{37,163,597,911},{node,inner},{seedtree_leaf,[3732,5606],[]}},
             {{27,984,226,986},{node,inner},{seedtree_leaf,[5039],[]}}]}]},
         [], 3},
        SeedTree2).
