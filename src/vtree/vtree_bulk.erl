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

-include("couch_db.hrl").

-ifndef(makecheck).
-define(MAX_FILLED, 40).
-else.
-define(MAX_FILLED, 4).
-compile(export_all).
-endif.

-export([omt_load/2, omt_write_tree/2, bulk_load/4]).

-export([log_n_ceil/2]).

% XXX vmx: check if tree has correct meta information set for every node
%    (type=inner/type=leaf)


% The seedtree is kept in memory, therefore it makes sense to restrict the
% maximum height of it.
-define(MAX_SEEDTREE_HEIGHT, 3).


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
    new = [] :: list(),
    % position in file (of this node)
    pos = -1 :: integer()
}).

-type mbr() :: {number(), number(), number(), number()}.
-type vtree_node() :: {mbr(), tuple(), list()}.
-type seedtree_root() :: tuple().
-type seedtree_node() :: tuple().
-type omt_node() :: tuple().
-type omt_tree() :: [omt_node()] | [omt_tree()].

% @doc Bulk load a tree. Returns the position of the root node and the height
% of the tree
-spec bulk_load(Fd::file:io_device(), RootPos::integer(),
        TargetTreeHeight::integer(), Nodes::[vtree_node()]) ->
        {ok, integer(), integer()}.
% No nodes to load
bulk_load(_Fd, RootPos, TargetTreeHeight, []) ->
    {ok, RootPos, TargetTreeHeight};
% Tree is empty
bulk_load(Fd, _RootPos, TargetTreeHeight, Nodes) when TargetTreeHeight==0 ->
    % Tree is empty => bulk load it
    {Omt, TreeHeight} = omt_load(Nodes, ?MAX_FILLED),
    {ok, MbrAndPosList} = omt_write_tree(Fd, Omt),

    {_Mbrs, PosList} = lists:unzip(MbrAndPosList),
    NewNodes = load_nodes(Fd, PosList),
    {ok, NewPos, _} = case length(NewNodes) of
    % single node as root
    1 ->
        Written = couch_file:append_term(Fd, hd(NewNodes)),
        ok = couch_file:flush(Fd),
        Written;
    % multiple nodes
    _ ->
        write_parent(Fd, NewNodes)
    end,
    {ok, NewPos, TreeHeight};
bulk_load(Fd, RootPos, TargetTreeHeight, Nodes) ->
    SeedtreeHeight = floor(TargetTreeHeight/2),
    Seedtree = seedtree_init(Fd, RootPos, SeedtreeHeight),
    Seedtree2 = seedtree_insert_list(Seedtree, Nodes),

    % There might be a huge about of outliers, insert them in a different
    % way. Create an OMT tree and insert the whole treet into the existing
    % one at the approriate height.
    Outliers = Seedtree2#seedtree_root.outliers,
    OutliersNum = length(Outliers),
    Seedtree3 = if
    OutliersNum > 100 ->
        Seedtree2#seedtree_root{outliers=[]};
    true ->
        Seedtree2
    end,

    {ok, Result, NewHeight, _HeightDiff} = seedtree_write(
            Fd, Seedtree3, TargetTreeHeight - Seedtree3#seedtree_root.height),

    {NewPos, NewHeight2, NewMbr} = case length(Result) of
    % single node as root
    1 ->
        {ok, ResultPos, _} = couch_file:append_term(Fd, hd(Result)),
        ok = couch_file:flush(Fd),
        {ResultPos, NewHeight, element(1, hd(Result))};
    % multiple nodes
    _ ->
        {ok, ResultPos, _} = write_parent(Fd, Result),
        {ResultPos, NewHeight+1, vtree:calc_nodes_mbr(Result)}
    end,

    % Insert outliers as whole subtree
    {NewPos2, NewHeight3} = if
    OutliersNum > 100 ->
        insert_outliers(Fd, NewPos, NewMbr, NewHeight2, Outliers);
    true ->
        {NewPos, NewHeight2}
    end,

    %?debugVal(NewPos2),
    {ok, NewPos2, NewHeight3}.


% @doc If there is a huge number of outliers, we bulk load them into a new
% tree and insert that tree directly to the original target tree. Returns
% the position of the root node in file and the height of the tree.
-spec insert_outliers(Fd::file:io_device(), TargetPos::integer(),
        TargetMbr::mbr(), TargetHeight::integer(),
        Nodes::[vtree_node()]) -> {integer(), integer()}.
insert_outliers(Fd, TargetPos, TargetMbr, TargetHeight, Nodes) ->
    {Omt, OmtHeight} = omt_load(Nodes, ?MAX_FILLED),
    {ok, MbrAndPosList} = omt_write_tree(Fd, Omt),

    {Mbrs, PosList} = lists:unzip(MbrAndPosList),
    MergedMbr = vtree:calc_mbr([TargetMbr|Mbrs]),

    % OmtHeight - 1 because this node might contain several children
    case TargetHeight - (OmtHeight - 1) of
    % Both, the new bulk loaded tree and targt tree have the same height
    % => create new common root
    Diff when Diff == 0 ->
    %?debugMsg("same height"),
        if
        length(MbrAndPosList) + 1 =< ?MAX_FILLED ->
            % XXX vmx: is using type=inner always valid?
            NewRootNode = {MergedMbr, #node{type=inner}, [TargetPos|PosList]},
            {ok, NewOmtPos, _} = couch_file:append_term(Fd, NewRootNode),
            ok = couch_file:flush(Fd),
            {NewOmtPos, TargetHeight+1};
        % split the node and create new root node
        true ->
            % NOTE vmx: These are not a normal nodes. The last element in the
            %     tuple is not the position of the children, but the position
            %     of the node itself.
            % XXX vmx: is type=inner always right?
            NewChildren = [{Mbr, #node{type=inner}, Pos} || {Mbr, Pos} <-
                    [{TargetMbr, TargetPos}|MbrAndPosList]],
            NodeToSplit = {MergedMbr, #node{type=inner}, NewChildren},
            {_SplittedMbr, Node1, Node2} = vtree:split_node(NodeToSplit),
            {ok, NewOmtPos, _} = write_parent(Fd, [Node1|[Node2]]),
            {NewOmtPos, TargetHeight+2}
        end;
    % insert new tree into target tree
    Diff when Diff > 0 ->
    %?debugMsg("target tree higher"),
        {ok, _, SubPos, Inc} = insert_subtree(
                Fd, TargetPos, MbrAndPosList, Diff-1),
        {SubPos, TargetHeight+Inc};
    % insert target tree into new tree
    Diff when Diff < 0 ->
    %?debugMsg("target tree smaller"),
        {OmtRootMbrs, OmtRootPosList} = lists:unzip(MbrAndPosList),
        OmtRootNode = {vtree:calc_mbr(OmtRootMbrs), #node{type=inner},
                OmtRootPosList},
        {ok, NewOmtPos, _} = couch_file:append_term(Fd, OmtRootNode),
        ok = couch_file:flush(Fd),
        {ok, _, SubPos, Inc} = insert_subtree(
                Fd, NewOmtPos, [{TargetMbr, TargetPos}], abs(Diff)),
        {SubPos, OmtHeight+Inc}
    end.

% @doc OMT bulk loading. MaxNodes is the number of maximum children per node.
%     Returns the OMT tree and the height of the tree
%     Based on (but modified):
%     OMT: Overlap Minimizing Top-down Bulk Loading Algorithm for R-tree
-spec omt_load(Nodes::[omt_node()], MaxNodes::integer()) ->
        {omt_tree(), integer()}.
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

    % NOTE vmx: currently all nodes have only 2 dimensions => "rem 2"
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
-spec omt_write_tree(Fd::file:io_device(), Tree::omt_tree()) ->
        {ok, [{mbr(), integer()}]} | {ok, [{mbr(), tuple()}]}.
omt_write_tree(Fd, Tree) ->
    Return = case omt_write_tree(Fd, Tree, 0, []) of
    {level_done, Nodes} ->
        Nodes;
    {leaf_nodes, MbrAndPos} ->
        [MbrAndPos]
    end,
    {ok, lists:reverse(Return)}.
% no more siblings
-spec omt_write_tree(Fd::file:io_device(), Tree::list(), Depth::integer(),
        Acc::list()) -> {ok, integer()}.
omt_write_tree(_Fd, [], _Depth, Acc) ->
    {no_siblings, Acc};
% leaf node
omt_write_tree(Fd, [H|_T]=Leafs, _Depth, _Acc) when is_tuple(H) ->
    % Don't write leafs nodes to disk now, they will be written later on.
    % Instead return a list of of tuples with the node's MBR and the node
    % itself
    Mbr = vtree:calc_nodes_mbr(Leafs),
    % We can not only pass in single nodes, but also subtrees. If the third
    % element of the tuple is a tuple, it's a leaf node, if it is a list,
    % it's an inner node
    Node = case is_tuple(element(3, H)) of
    true ->
        {Mbr, #node{type=leaf}, Leafs};
    false ->
        PosList = lists:map(fun(N) ->
            {ok, Pos, _} = couch_file:append_term(Fd, N),
            ok = couch_file:flush(Fd),
            Pos
        end, Leafs),
        {Mbr, #node{type=inner}, PosList}
    end,
    {ok, Pos, _} = couch_file:append_term(Fd, Node),
    ok = couch_file:flush(Fd),
    {leaf_nodes, {Mbr, Pos}};
omt_write_tree(Fd, [H|T], Depth, Acc) ->
    {_, Acc2} = case omt_write_tree(Fd, H, Depth+1, []) of
    {no_siblings, Siblings} ->
        {ok, Siblings};
    {leaf_nodes, MbrAndPos} ->
        {ok, [MbrAndPos|Acc]};
    {level_done, Level} ->
        % NOTE vmx: reversing Level is probably not neccessary
        {Mbrs, Children} = lists:unzip(lists:reverse(Level)),
        Mbr = vtree:calc_mbr(Mbrs),
        Meta = #node{type=inner},
        {ok, Pos, _} = couch_file:append_term(Fd, {Mbr, Meta, Children}),
        ok = couch_file:flush(Fd),
        {ok, [{Mbr, Pos}|Acc]}
    end,
    {_, Acc3} = omt_write_tree(Fd, T, Depth, Acc2),
    {level_done, Acc3}.


% @doc Sort nodes by a certain dimension (which is the first element of the
%     node tuple)
-spec omt_sort_nodes(Nodes::[omt_node()], Dimension::integer()) ->
        [omt_node()].
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
-spec seedtree_insert_list(Tree::seedtree_root(), _Nodes::[tuple()]) ->
        seedtree_root().
seedtree_insert_list(Root, []) ->
    Root;
seedtree_insert_list(Root, [H|T]=_Nodes) ->
    Root2 = seedtree_insert(Root, H),
    seedtree_insert_list(Root2, T).

% @doc Insert a new item into the seed tree
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

% @doc Do the actual insertion of the nodes into the seedtree
-spec seedtree_insert_children(Children::[seedtree_node()],
        Node::seedtree_node()) ->
        {ok, seedtree_node()} | {not_inserted}.
seedtree_insert_children([], Node) ->
    {not_inserted, Node};
seedtree_insert_children(#seedtree_leaf{new=Old}=Children, Node) when
        not is_list(Children) ->
    New = [Node|Old],
    Children2 = Children#seedtree_leaf{new=New},
    {ok, Children2};
seedtree_insert_children([H|T], Node) ->
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
%     the leafs. If MaxDepth is bigger than the tree, the lowest possible
%     level is returned.
-spec seedtree_init(Fd::file:io_device(), RootPos::integer(),
        MaxDepth::integer()) -> seedtree_root().
seedtree_init(Fd, RootPos, MaxDepth) ->
    Tree = seedtree_init(Fd, RootPos, MaxDepth, 0),
    %#seedtree_root{tree=Tree, height=MaxDepth}.
    Height = erlang:min(MaxDepth, ?MAX_SEEDTREE_HEIGHT),
    #seedtree_root{tree=Tree, height=Height}.
-spec seedtree_init(Fd::file:io_device(), RootPos::integer(),
        MaxDepth::integer(), Depth::integer()) -> seedtree_node().
% It's "Depth+1" as the root already contains several nodes (it's the nature
% of a R-Tree).
seedtree_init(Fd, RootPos, MaxDepth, Depth) when
        (Depth+1 == MaxDepth) or (Depth+1 == ?MAX_SEEDTREE_HEIGHT) ->
    %?debugVal(RootPos),
    {ok, Parent} = couch_file:pread_term(Fd, RootPos),
    {ParentMbr, ParentMeta, EntriesPos} = Parent,
    {ParentMbr, ParentMeta, #seedtree_leaf{orig=EntriesPos, pos=RootPos}};
seedtree_init(Fd, RootPos, MaxDepth, Depth) ->
    {ok, Parent} = couch_file:pread_term(Fd, RootPos),
    {ParentMbr, ParentMeta, EntriesPos} = Parent,
    case is_tuple(hd(EntriesPos)) of
    % we reached leaf level => return
    true ->
        % create seedtree leaf
        {ParentMbr, ParentMeta, #seedtree_leaf{orig=EntriesPos, pos=RootPos}};
    false ->
        Children = lists:foldl(fun(EntryPos, Acc) ->
            Child = seedtree_init(Fd, EntryPos, MaxDepth, Depth+1),
            [Child|Acc]
        end, [], EntriesPos),
        {ParentMbr, ParentMeta, lists:reverse(Children)}
    end.


% @doc Write an new vtree. InsertHeight is the height where the seedtree
% should be inserted to. Returns the root nodes, the height of the tree and
% the height difference to the old tree.
-spec seedtree_write(Fd::file:io_device(), Seetree::seedtree_root(),
        InsertHeight::integer()) -> {ok, [vtree_node()], integer(), integer()}.
seedtree_write(Fd, Seedtree, InsertHeight) ->
    Tree = Seedtree#seedtree_root.tree,
    TargetHeight = InsertHeight + Seedtree#seedtree_root.height,
    {level_done, Level} = seedtree_write(Fd, [Tree], InsertHeight, []),

    {RootNodes, NewHeight} = if
    length(Level) =< ?MAX_FILLED ->
        {Level, TargetHeight};
    true ->
        {Level2, Level2Height} = omt_load(Level, ?MAX_FILLED),
        {ok, ParentMbrAndPos} = omt_write_tree(Fd, Level2),
        Parents = lists:map(fun({_, Pos}) ->
            {ok, Node} = couch_file:pread_term(Fd, Pos),
            Node
        end, ParentMbrAndPos),
        {Parents, TargetHeight+Level2Height-1}
    end,

    {RootNodes2, HeightDiff} = case Seedtree#seedtree_root.outliers of
    [] ->
        {RootNodes, 0};
    Outliers ->
        % insert outliers by creating a temporary root node...
        {ok, TmpRootPos, _} = write_parent(Fd, RootNodes),
        {TmpRootPos2, TmpHeight} = lists:foldl(fun(Outlier, {CurPos, _}) ->
            {Mbr, Meta, {DocId, {Geom, Value}}} = Outlier,
            {ok, _NewMbr, CurPos2, TreeHeight} = vtree:insert(
                    Fd, CurPos, DocId, {Mbr, Meta, Geom, Value}),
            {CurPos2, TreeHeight}
        end, {TmpRootPos, 0}, Outliers),

        {ok, OldRoot} = couch_file:pread_term(Fd, TmpRootPos2),
        % ...and get the original children back again
        {_, _, RootNodes2Pos} = OldRoot,

        LevelDiff = TmpHeight-1-NewHeight,
        ChildrenNodes = load_nodes(Fd, RootNodes2Pos),
        {ChildrenNodes, LevelDiff}
    end,
    {ok, RootNodes2, NewHeight+HeightDiff, HeightDiff}.

-spec seedtree_write(Fd::file:io_device(), Seetree::seedtree_root(),
        InsertHeight::integer(), Acc::[vtree_node()]) ->
        {ok, [vtree_node()], integer(), integer()}.
seedtree_write(_Fd, [], _InsertHeight, Acc) ->
    {no_more_siblings, Acc};
% No new nodes to insert
seedtree_write(_Fd, #seedtree_leaf{orig=Orig, new=[]}, _InsertHeight, _Acc) ->
    {leafs, Orig};
% New nodes to insert
% This function returns a new list of children, as some children were
% rewritten due to repacking. The MBR doesn't change (that's the nature of
% this algorithm).
seedtree_write(Fd, #seedtree_leaf{orig=Orig, new=New, pos=ParentPos},
        InsertHeight, _Acc) ->
    NewNum = length(New),
    OmtHeight = log_n_ceil(?MAX_FILLED, NewNum),
    HeightDiff = InsertHeight - (OmtHeight - 1),
    NewChildrenPos2 = if
    % Input tree can be inserted as-is into the target tree
    HeightDiff == 0 ->
    %?debugMsg("insert as is"),
        {OmtTree, OmtHeight} = omt_load(New, ?MAX_FILLED),
        NewChildren = seedtree_write_insert(Fd, Orig, OmtTree, OmtHeight),
        _MbrAndPos = seedtree_write_finish(NewChildren);
    % insert tree is too small => expand seedtree
    HeightDiff > 0 ->
    %?debugMsg("insert is too small"),
        % Create new seedtree
        % HeightDiff+1 as we like to load the level of the children
        Seedtree = seedtree_init(Fd, ParentPos, HeightDiff+1),
        Seedtree2 = seedtree_insert_list(Seedtree, New),
        {ok, NodesList, _NewHeight, NewHeightDiff} = seedtree_write(
                Fd, Seedtree2, InsertHeight-Seedtree2#seedtree_root.height+1),
        % NOTE vmx (2011-01-04) This makes one test fail. I have to admit I
        %     can't see the point of this code atm. The calculation of the
        %     insertion was based on the height of the resulting OMT tree, so
        %     why should some levels be stripped?
        % There was a node overflow. Though we are currently somewhere within
        % the tree, so we need to return a node that has the original height.
        % Therefore recreate the overflowed node.
        NodesList2 = case NewHeightDiff > 0 of
        false ->
            NodesList;
        true ->
            lists:foldl(fun(_I, Acc) ->
                PosList = lists:append([Pos || {_, _, Pos} <- Acc]),
                load_nodes(Fd, PosList)
            end, NodesList, lists:seq(1, NewHeightDiff))
        end,
        _MbrAndPos = [{Mbr, Pos} || {Mbr, _, Pos} <- NodesList2];
        %MbrAndPos = [{Mbr, Pos} || {Mbr, _, Pos} <- NodesList];
    % insert tree is too high => use its children
    HeightDiff < 0 ->
    %?debugMsg("insert is too high"),
        {OmtTree, OmtHeight} = omt_load(New, ?MAX_FILLED),
        % flatten the OMT tree until it has the expected height to be
        % inserted into the target tree (one subtree at a time).
        OmtTrees = lists:foldl(fun(_I, Acc) ->
            lists:append(Acc)
        end, OmtTree, lists:seq(1, abs(HeightDiff))),
        % (OmtHeight + HeightDiff) as OMT tree was flattened (HeighTDiff is
        % negative)
        MbrAndPos2 = seedtree_write_insert(Fd, Orig, OmtTrees,
                (OmtHeight + HeightDiff)),
        {NewMbrs, NewPos} = lists:unzip(MbrAndPos2),
        _MbrAndPos = seedtree_write_finish(lists:zip(NewMbrs, NewPos))
    end,
    {new_leaf, NewChildrenPos2};
seedtree_write(Fd, [{Mbr, Meta, Children}|T], InsertHeight, Acc) ->
    {ok, Acc2} = case seedtree_write(Fd, Children, InsertHeight, []) of
    {no_more_siblings, Siblings} ->
       {ok, Siblings};
    {leafs, Orig} ->
       {ok, [{Mbr, Meta, Orig}|Acc]};
    % This one might return more that one new node => level_done needs
    % to handle a possible overflow
    {new_leaf, Nodes} ->
       Nodes2 = lists:foldl(fun({NodeMbr, NodePos}, Acc2) ->
           [{NodeMbr, Meta, NodePos}|Acc2]
       end, Acc, Nodes),
       {ok, Nodes2};
    % Node can be written as-is, as the number of children is low enough
    {level_done, Level} when length(Level) =< ?MAX_FILLED->
        ParentMbr = vtree:calc_nodes_mbr(Level),
        ChildrenPos = write_nodes(Fd, Level),
        % XXX vmx: Not sure if type=inner is always right
        Parent = {ParentMbr, #node{type=inner}, ChildrenPos},
        {ok, [Parent|Acc]};
    {level_done, Level} when length(Level) > ?MAX_FILLED ->
        {Level2, Level2Height} = omt_load(Level, ?MAX_FILLED),
        Level3 = if
        Level2Height > 2 ->
            % Lets's try to flatten the list in case it is too deep
            lists:foldl(fun(_I, ToAppend) ->
                lists:append(ToAppend)
            end, Level2, lists:seq(3, Level2Height));
        true ->
            Level2
        end,

        Parents = lists:foldl(fun(Level4, LevelAcc) ->
            ParentMbr = vtree:calc_nodes_mbr(Level4),
            ChildrenPos = write_nodes(Fd, Level4),
            % XXX vmx: Not sure if type=inner is always right
            Parent = {ParentMbr, #node{type=inner}, ChildrenPos},
            [Parent|LevelAcc]
        end, Acc, Level3),
        {ok, Parents}
    end,
    {_, Acc3} = seedtree_write(Fd, T, InsertHeight, Acc2),
    {level_done, Acc3}.

% @doc Writes new nodes into the existing tree. The nodes, resp. the
% height of the OMT tree, must fit into the target tree.
% "Orig" are the original child nodes, "New" are the nodes that should be
% inserted.
% Returns a tuple with the MBR and the position of the root node in the file.
% The result may be a node that overflows. seedtree_write_finish/1 will fix
% that.
-spec seedtree_write_insert(Fd::file:io_device(), Orig::[vtree_node()],
        OmtTree::omt_node(), OmtHeight::integer()) -> [{mbr(), integer()}].
% Leaf node. We won't do the repacking dance, as we are on the lowest
% level already. Thus we just insert the new nodes
seedtree_write_insert(_Fd, Orig, OmtTree, OmtHeight) when is_tuple(hd(Orig)) ->
    OmtTree2 = if
    OmtHeight > 1 ->
        % Lets's try to flatten the list in case it is too deep
        lists:foldl(fun(_I, ToAppend) ->
            lists:append(ToAppend)
        end, OmtTree, lists:seq(2, OmtHeight));
    true ->
        OmtTree
    end,
    NewNodes = Orig ++ OmtTree2,

    % create a list with tuples consisting of MBR and the actual node
    [{Mbr, Node} || {Mbr, _, _}=Node <- NewNodes];
% Inner node, do some repacking.
seedtree_write_insert(Fd, Orig, OmtTree, _OmtHeight) ->
    % Write the OmtTree to to disk.
    % The tree might contain more than the maximum
    % number of allowed nodes per node. This overflow will be solved
    % at the end, when all child nodes get merged.
    {ok, OmtMbrAndPos} = omt_write_tree(Fd, OmtTree),

    % Get the enclosing MBR of the OMT nodes
    {OmtMbrs, OmtPosList} = lists:unzip(OmtMbrAndPos),
    OmtMbr = vtree:calc_mbr(OmtMbrs),

    % We do some repacking for better perfomance. First get the children
    % of the target node where the new nodes will be inserted in
    OrigNodes = load_nodes(Fd, Orig),

    % Transform the original nodes from normal ones to tuples where the
    % meta information is replaced with the position of the node in
    % the file.
    OrigMbrAndPos = lists:zipwith(fun({Mbr, _, Children}, Pos) ->
        {Mbr, Pos, Children}
    end, OrigNodes, Orig),

    % Get all nodes that are within or intersect with the input tree
    % root node(s)
    {Disjoint, NotDisjoint} = lists:partition(fun({Mbr, _Pos, _}) ->
        vtree:disjoint(Mbr, OmtMbr)
    end, OrigMbrAndPos),

    % For better performance, we now take the children of the nodes that will
    % be inserted, as well as all the children from the nodes that are not
    % disjoint. With this pool of nodes, we create new nodes that enclose
    % them more tightly.

    % First get the position of children of the not disjoint nodes
    NdChildrenPos = lists:append(
            [Children || {_Mbr, _Pos, Children} <- NotDisjoint]),

    % Get the position of the children of the nodes that will be inserted
    % as well.
    OmtChildren = load_nodes(Fd, OmtPosList),
    OmtChildrenPos = lists:append(
            [Children || {_Mbr, _Pos, Children} <- OmtChildren]),

    NewChildrenPos = NdChildrenPos ++ OmtChildrenPos,
    % Transform the new children's position to a tuple containing their MBR
    % and the position.
    NewChildrenMbrAndPos = case is_tuple(hd(NewChildrenPos)) of
    % We might have hit leaf nodes. They already contain the MBR we need to
    % find.
    true ->
        [{Mbr, Node} || {Mbr, _, _}=Node <- NewChildrenPos];
    % Else we need to load the nodes to get their MBR
    false ->
        NewChildrenChildren = load_nodes(Fd, NewChildrenPos),
        lists:zipwith(fun({Mbr, _, _}, Pos) ->
            {Mbr, Pos}
        end, NewChildrenChildren, NewChildrenPos)
    end,

    % Create nodes that are packed to the maximum. This might return a several
    % levels deep OMT tree if there is a huge number of nodes.
    {NewNodesOmt, NewNodesHeight} = omt_load(
            NewChildrenMbrAndPos, ?MAX_FILLED),

    NewNodes = case NewNodesHeight of
    1 ->
        [NewNodesOmt];
    2 ->
        NewNodesOmt;
    _ ->
        lists:foldl(fun(_I, Acc) ->
            lists:append(Acc)
        end, NewNodesOmt, lists:seq(3, NewNodesHeight))
    end,

    % Write those new nodes
    NewChildren = lists:map(fun(Nodes) ->
        {Mbrs, Pos} = lists:unzip(Nodes),
        ParentMbr = vtree:calc_mbr(Mbrs),
        Meta = case is_tuple(hd(Pos)) of
            true -> #node{type=leaf};
            false -> #node{type=inner}
        end,
        {ok, ParentPos, _} = couch_file:append_term(Fd, {ParentMbr, Meta, Pos}),
        ok = couch_file:flush(Fd),
        {ParentMbr, ParentPos}
    end, NewNodes),

    DisjointMbrAndPos = [{Mbr, Pos} || {Mbr, Pos, _} <- Disjoint],
    DisjointMbrAndPos ++ NewChildren.


% @doc Creates new parent nodes out of a list of tuples containing MBRs and
%     positions (splits as needed). Returns MBR and position in file of the
%     new parent node(s)
-spec seedtree_write_finish(NewChildren::[{mbr(), integer()}]) ->
        [{mbr(), integer()}].
seedtree_write_finish(NewChildren) ->
    NewChildren3 = case length(NewChildren) > ?MAX_FILLED of
    true ->
        {NewChildren2, OmtHeight} = omt_load(NewChildren, ?MAX_FILLED),
        if
        OmtHeight > 2 ->
            % Lets's try to flatten the list in case it is too deep
            lists:foldl(fun(_I, ToAppend) ->
                lists:append(ToAppend)
            end, NewChildren2, lists:seq(3, OmtHeight));
        true ->
            NewChildren2
        end;
    false ->
        [NewChildren]
    end,

    % Calculate the common MBR. The result is a tuple of the MBR and
    % the children positions in file it encloses
    NewChildrenMbrAndPos = lists:reverse(lists:foldl(fun(Nodes, Acc) ->
        {Mbrs, PosList} = lists:unzip(Nodes),
        % Poss might be actual nodes, if it is a leaf node. Then they
        % are already wrapped in a list.
        PosList2 = if
            is_list(hd(PosList)) -> hd(PosList);
            true -> PosList
        end,
        Mbr = vtree:calc_mbr(Mbrs),
        [{Mbr, PosList2}|Acc]
    end, [], NewChildren3)),

    % Return new nodes (might be several ones)
    NewChildrenMbrAndPos.


% @doc Loads nodes from file
-spec load_nodes(Fd::file:io_device(), Positions::[integer()]) ->
        [vtree_node()].
load_nodes(Fd, Positions) ->
    load_nodes(Fd, Positions, []).
-spec load_nodes(Fd::file:io_device(), Positions::[integer()],
        Acc::[vtree_node()]) -> [vtree_node()].
load_nodes(_Fd, [], Acc) ->
    lists:reverse(Acc);
load_nodes(Fd, [H|T], Acc) ->
    {ok, Node} = couch_file:pread_term(Fd, H),
    load_nodes(Fd, T, [Node|Acc]).

% @doc Write nodes to file, return their positions
-spec write_nodes(Fd::file:io_device(), Nodes::[vtree_node()]) -> [integer()].
write_nodes(Fd, Nodes) ->
    write_nodes(Fd, Nodes, []).
-spec write_nodes(Fd::file:io_device(), Nodes::[vtree_node()],
        Acc::[vtree_node()]) -> [integer()].
write_nodes(_Fd, [], Acc) ->
    lists:reverse(Acc);
write_nodes(Fd, [H|T], Acc) ->
    {ok, Pos, _} = couch_file:append_term(Fd, H),
    ok = couch_file:flush(Fd),
    write_nodes(Fd, T, [Pos|Acc]).

% @doc Write a list of of nodes to disk with corresponding parent node. Return
%     the postion of the parent node in the file and the number of bytes
%     written.
-spec write_parent(Fd::file:io_device(), Nodes::[vtree_node()]) ->
        {ok, integer(), integer()}.
write_parent(Fd, Nodes) ->
    ParentMbr = vtree:calc_nodes_mbr(Nodes),
    ChildrenPos = write_nodes(Fd, Nodes),
    {ok, ParentPos, Length} = couch_file:append_term(
         Fd, {ParentMbr, #node{type=inner}, ChildrenPos}),
    ok = couch_file:flush(Fd),
    {ok, ParentPos, Length}.

% XXX vmx: insert_subtree and potentially other functions should be moved
%     from the vtree_bulk to the vtree module
% @doc inserts a subtree into an vtree at a specific level. Returns the
% MBR, position in the file of the new root node and the increase in height
% of the tree.
-spec insert_subtree(Fd::file:io_device(), RootPos::integer(),
        Subtree::[{mbr(), integer()}], Level::integer()) ->
        {ok, mbr(), integer(), integer()}.
insert_subtree(Fd, RootPos, Subtree, Level) ->
    case insert_subtree(Fd, RootPos, Subtree, Level, 0) of
    {splitted, NodeMbr, {_Node1Mbr, NodePos1}, {_Node2Mbr, NodePos2}, Inc} ->
        Parent = {NodeMbr, #node{type=inner}, [NodePos1, NodePos2]},
        {ok, Pos, _} = couch_file:append_term(Fd, Parent),
        ok = couch_file:flush(Fd),
        {ok, NodeMbr, Pos, Inc+1};
    {ok, NewMbr, NewPos, Inc} ->
        {ok, NewMbr, NewPos, Inc}
    end.

% @doc Returns either ok and MBR, position in file and the height of the tree,
% or splitted and the enclosing MBR, MBR and position in file of the
% individual nodes and the increase in height of the tree.
-spec insert_subtree(Fd::file:io_device(), RootPos::integer(),
        Subtree::[{mbr(), integer()}], Level::integer(), Depth::integer()) ->
        {ok, mbr(), integer(), integer()} |
        {splitted, mbr(), {mbr(), integer()}, {mbr(), integer()}, integer()}.
insert_subtree(Fd, RootPos, Subtree, Level, Depth) when Depth==Level ->
    {SubtreeMbrs, SubtreePosList} = lists:unzip(Subtree),
    {ok, Parent} = couch_file:pread_term(Fd, RootPos),
    {ParentMbr, ParentMeta, EntriesPos} = Parent,
    MergedMbr = vtree:calc_mbr([ParentMbr|SubtreeMbrs]),
    ChildrenPos = SubtreePosList ++ EntriesPos,
    if
    length(ChildrenPos) =< ?MAX_FILLED ->
        NewNode = {MergedMbr, ParentMeta, ChildrenPos},
        {ok, Pos, _} = couch_file:append_term(Fd, NewNode),
        ok = couch_file:flush(Fd),
        {ok, MergedMbr, Pos, 0};
    true ->
        Children = load_nodes(Fd, ChildrenPos),

        % Transform the the nodes, so that we don't need to write them again.
        % They are now a tuple with their MBR, Meta and *their* position
        % in the file (as opposed to the position of their children)
        Children2 = [{Mbr, Meta, Pos} || {{Mbr, Meta, _}, Pos} <-
                lists:zip(Children, ChildrenPos)],

        % The maximum number of nodes can be 2*MAX_FILLED, therefore we can't
        % use the Ang/Tan split algorithm, but use the OMT algorithm to split
        % the nodes into two partitions.
        {[Part1, Part2], _OmtHeight} = omt_load(Children2, ?MAX_FILLED),

        Mbr1 = vtree:calc_nodes_mbr(Part1),
        Mbr2 = vtree:calc_nodes_mbr(Part2),
        % Get the original position in file of the nodes
        Part1Pos = [Pos || {_Mbr, _Meta, Pos} <- Part1],
        Part2Pos = [Pos || {_Mbr, _Meta, Pos} <- Part2],
        Node1 =  {Mbr1, #node{type=inner}, Part1Pos},
        Node2 =  {Mbr2, #node{type=inner}, Part2Pos},
        {ok, Pos1, _} = couch_file:append_term(Fd, Node1),
        {ok, Pos2, _} = couch_file:append_term(Fd, Node2),
        ok = couch_file:flush(Fd),

        {splitted, MergedMbr, {Mbr1, Pos1}, {Mbr2, Pos2}, 0}
    end;
insert_subtree(Fd, RootPos, Subtree, Level, Depth) ->
    %{SubtreeMbr, SubtreePos} = Subtree,
    {SubtreeMbrs, _SubtreePosList} = lists:unzip(Subtree),
    % Calculate the MBR that enclosed both nodes, as both nodes should end up
    % in the same target node
    SubtreeMbr = vtree:calc_mbr(SubtreeMbrs),
    {ok, Parent} = couch_file:pread_term(Fd, RootPos),
    {ParentMbr, _ParentMeta, EntriesPos} = Parent,
    {{_LeastMbr, LeastPos}, LeastRest} = least_expansion(
            Fd, SubtreeMbr, EntriesPos),
    LeastRestPos = [Pos || {_Mbr, Pos} <- LeastRest],
    case insert_subtree(Fd, LeastPos, Subtree, Level, Depth+1) of
    {ok, NewMbr, NewPos, Inc} ->
        MergedMbr = vtree:merge_mbr(ParentMbr, NewMbr),
        NewNode = {MergedMbr, #node{type=inner}, [NewPos|LeastRestPos]},
        {ok, Pos, _} = couch_file:append_term(Fd, NewNode),
        ok = couch_file:flush(Fd),
        {ok, NewMbr, Pos, Inc};
    {splitted, ChildMbr, {Child1Mbr, ChildPos1}, {Child2Mbr, ChildPos2}, Inc} ->
        MergedMbr = vtree:merge_mbr(ParentMbr, ChildMbr),
        LeastRestPos = [Pos || {_Mbr, Pos} <- LeastRest],
        if
        % Both nodes of the split fit in the current inner node
        %length(EntriesPos)+2 =< ?MAX_FILLED ->
        length(LeastRestPos)+2 =< ?MAX_FILLED ->
            ChildrenPos = [ChildPos1, ChildPos2] ++ LeastRestPos,
            NewNode = {MergedMbr, #node{type=inner}, ChildrenPos},
            {ok, Pos, _} = couch_file:append_term(Fd, NewNode),
            ok = couch_file:flush(Fd),
            {ok, MergedMbr, Pos, Inc};
        % We need to split the inner node
        true ->
            Child1 = {Child1Mbr, #node{type=inner}, ChildPos1},
            Child2 = {Child2Mbr, #node{type=inner}, ChildPos2},

            % NOTE vmx: for vtree:split/1 the nodes need to have a special
            %     format a tuple with their MBR, Meta and *their* position in
            %     the file (as opposed to the position of their children)
            Children = [{Mbr, Meta, Pos} || {{Mbr, Meta, _}, Pos} <-
                    lists:zip(load_nodes(Fd, LeastRestPos), LeastRestPos)],
            Children2 = [Child1, Child2] ++ Children,
            {SplittedMbr, {Node1Mbr, _, _}=Node1, {Node2Mbr, _, _}=Node2}
                    = vtree:split_node({MergedMbr, #node{type=inner}, Children2}),
            {ok, Pos1, _} = couch_file:append_term(Fd, Node1),
            {ok, Pos2, _} = couch_file:append_term(Fd, Node2),
            ok = couch_file:flush(Fd),
            {splitted, SplittedMbr, {Node1Mbr, Pos1}, {Node2Mbr, Pos2}, Inc}
        end
    end.


% XXX vmx: needs tests
% @doc Find the node that needs least expension for the given MBR and returns
% the MBR and position of the node together with the original children MBRs and
% positions in the file.
-spec least_expansion(Fd::file:io_device(), NewMbr::mbr(),
        PosList::[integer()]) -> {{mbr, integer()}, [{mbr, integer()}]}.
least_expansion(Fd, NewMbr, PosList) ->
    Nodes = load_nodes(Fd, PosList),
    NodesAndPos = lists:zip(Nodes, PosList),
    MbrAndPos = [{Mbr, Pos} || {{Mbr, _, _}, Pos} <- NodesAndPos],

    % Find node that matches best (least expansion of MBR)
    {_, _, Nth} = lists:foldl(fun({Mbr, _Pos}, {MinExp, Nth2, Cnt}) ->
        MergedMbr = vtree:merge_mbr(NewMbr, Mbr),
        % if there is a element which need less expansion, put the info
        % into the accumulator
        case vtree:area(MergedMbr) - vtree:area(Mbr) of
        Exp when Exp < MinExp orelse (MinExp==-1) ->
            {Exp, Cnt, Cnt+1};
        _ ->
            {MinExp, Nth2, Cnt+1}
        end
    end, {-1, 0, 0}, MbrAndPos),

    % Remove the child where the node will be inserted
    {C1, C2} = lists:split(Nth-1, MbrAndPos),
    {hd(C2), C1 ++ tl(C2)}.


%%%%% Helpers %%%%%

% @doc Returns the ceiling of log_N(X). Returns 1 for X==1.
-spec log_n_ceil(N::integer(), X::integer()) -> integer().
log_n_ceil(_N, 1) ->
    1;
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
