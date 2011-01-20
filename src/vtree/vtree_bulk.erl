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

-include("couch_db.hrl").

-export([omt_load/2, omt_write_tree/2, bulk_load/4]).

-export([log_n_ceil/2]).

% XXX vmx: check if tree has correct meta information set for every node
%    (type=inner/type=leaf)

% Nodes maximum filling grade (TODO vmx: shouldn't be hard-coded)
-define(MAX_FILLED, 40).
%-define(MAX_FILLED, 4).

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
bulk_load(Fd, RootPos, TargetTreeHeight, []) ->
    {ok, RootPos, TargetTreeHeight};
% Tree is empty
bulk_load(Fd, RootPos, TargetTreeHeight, Nodes) when TargetTreeHeight==0 ->
    % Tree is empty => bulk load it
    {Omt, TreeHeight} = omt_load(Nodes, ?MAX_FILLED),
    {ok, MbrAndPosList} = omt_write_tree(Fd, Omt),

    {_Mbrs, PosList} = lists:unzip(MbrAndPosList),
    NewNodes = load_nodes(Fd, PosList),
    {ok, NewPos} = case length(NewNodes) of
        % single node as root
        1 -> couch_file:append_term(Fd, hd(NewNodes));
        % multiple nodes
        _ -> write_parent(Fd, NewNodes)
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
        {ok, ResultPos} = couch_file:append_term(Fd, hd(Result)),
        {ResultPos, NewHeight, element(1, hd(Result))};
    % multiple nodes
    _ ->
        {ok, ResultPos} = write_parent(Fd, Result),
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
            {ok, NewOmtPos} = couch_file:append_term(Fd, NewRootNode),
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
            {SplittedMbr, {Node1Mbr, _, _}=Node1, {Node2Mbr, _, _}=Node2} =
                    vtree:split_node(NodeToSplit),
            {ok, NewOmtPos} = write_parent(Fd, [Node1|[Node2]]),
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
        {ok, NewOmtPos} = couch_file:append_term(Fd, OmtRootNode),
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
omt_write_tree(Fd, [H|_T]=Leafs, Depth, _Acc) when is_tuple(H) ->
    % Don't write leafs nodes to disk now, they will be written later on.
    % Instead return a list of of tuples with the node's MBR and the node
    % itself
    Mbr = vtree:calc_nodes_mbr(Leafs),
    {ok, Pos} = couch_file:append_term(
            Fd, {Mbr, #node{type=leaf}, Leafs}),
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
        {ok, Pos} = couch_file:append_term(Fd, {Mbr, Meta, Children}),
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
        {Level2, _Level2Height} = omt_load(Level, ?MAX_FILLED),

        Parents = lists:foldl(fun(Level3, LevelAcc) ->
            ParentMbr = vtree:calc_nodes_mbr(Level3),
            ChildrenPos = write_nodes(Fd, Level3),
            % XXX vmx: Not sure if type=inner is always right
            Parent = {ParentMbr, #node{type=inner}, ChildrenPos},
            [Parent|LevelAcc]
        end, [], Level2),
        {Parents, TargetHeight+1}
    end,

    {RootNodes2, HeightDiff} = case Seedtree#seedtree_root.outliers of
    [] ->
        {RootNodes, 0};
    Outliers ->
        % insert outliers by creating a temporary root node...
        {ok, TmpRootPos} = write_parent(Fd, RootNodes),
        {ok, TmpRootNode} = couch_file:pread_term(Fd, TmpRootPos),
        {TmpRootPos2, TmpHeight} = lists:foldl(fun(Outlier, {CurPos, _}) ->
            {Mbr, Meta, {DocId, Value}} = Outlier,
            {ok, _NewMbr, CurPos2, TreeHeight} = vtree:insert(
                    Fd, CurPos, DocId, {Mbr, Meta, Value}),
            {CurPos2, TreeHeight}
        end, {TmpRootPos, 0}, Seedtree#seedtree_root.outliers),

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
seedtree_write(_Fd, [], InsertHeight, Acc) ->
    {no_more_siblings, Acc};
% No new nodes to insert
seedtree_write(_Fd, #seedtree_leaf{orig=Orig, new=[]}, _InsertHeight, _Acc) ->
    {leafs, Orig};
% New nodes to insert
% This function returns a new list of children, as some children were
% rewritten due to repacking. The MBR doesn't change (that's the nature of
% this algorithm).
seedtree_write(Fd, #seedtree_leaf{orig=Orig, new=New, pos=ParentPos},
        InsertHeight, Acc) ->
    NewNum = length(New),
    OmtHeight = log_n_ceil(?MAX_FILLED, NewNum),
    HeightDiff = InsertHeight - (OmtHeight - 1),
    NewChildrenPos2 = if
    % Input tree can be inserted as-is into the target tree
    HeightDiff == 0 ->
    %?debugMsg("insert as is"),
        {OmtTree, OmtHeight} = omt_load(New, ?MAX_FILLED),
        NewChildren = seedtree_write_insert(Fd, Orig, OmtTree, OmtHeight),
        MbrAndPos = seedtree_write_finish(NewChildren);
    % insert tree is too small => expand seedtree
    HeightDiff > 0 ->
    %?debugMsg("insert is too small"),
        % Create new seedtree
        % HeightDiff+1 as we like to load the level of the children
        Seedtree = seedtree_init(Fd, ParentPos, HeightDiff+1),
        Seedtree2 = seedtree_insert_list(Seedtree, New),
        {ok, NodesList, NewHeight, NewHeightDiff} = seedtree_write(
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
        MbrAndPos = [{Mbr, Pos} || {Mbr, _, Pos} <- NodesList2];
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
        MbrAndPos = seedtree_write_finish(lists:zip(NewMbrs, NewPos))
    end,
    {new_leaf, NewChildrenPos2};
seedtree_write(Fd, [{Mbr, Meta, Children}=H|T], InsertHeight, Acc) ->
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
    {Info, Acc3} = seedtree_write(Fd, T, InsertHeight, Acc2),
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
seedtree_write_insert(Fd, Orig, OmtTree, OmtHeight) when is_tuple(hd(Orig)) ->
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
seedtree_write_insert(Fd, Orig, OmtTree, OmtHeight) ->
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
        {ok, ParentPos} = couch_file:append_term(Fd, {ParentMbr, Meta, Pos}),
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
    {ok, Pos} = couch_file:append_term(Fd, H),
    write_nodes(Fd, T, [Pos|Acc]).

% @doc Write a list of of nodes to disk with corresponding parent node. Return
%     the postion of the parent node in the file.
-spec write_parent(Fd::file:io_device(), Nodes::[vtree_node()]) ->
        {ok, integer()}.
write_parent(Fd, Nodes) ->
    ParentMbr = vtree:calc_nodes_mbr(Nodes),
    ChildrenPos = write_nodes(Fd, Nodes),
    {ok, ParentPos} = couch_file:append_term(
         Fd, {ParentMbr, #node{type=inner}, ChildrenPos}),
    {ok, ParentPos}.


% @doc Create a new root node for a list of tuples containing MBR and postion
% in the file. Returns the new enclosing MBR and position in the file.
-spec write_root(Fd::file:io_device(),
        MbrAndPos::[{mbr(), integer()}]) -> {mbr(), integer()}.
write_root(Fd, MbrAndPos) ->
    {Mbrs, PosList} = lists:unzip(MbrAndPos),
    ParentMbr = vtree:calc_mbr(Mbrs),
    {ok, ParentPos} = couch_file:append_term(
         Fd, {ParentMbr, #node{type=inner}, PosList}),
    {ParentMbr, ParentPos}.

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
    {splitted, NodeMbr, {Node1Mbr, NodePos1}, {Node2Mbr, NodePos2}, Inc} ->
        Parent = {NodeMbr, #node{type=inner}, [NodePos1, NodePos2]},
        {ok, Pos} = couch_file:append_term(Fd, Parent),
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
        {ok, Pos} = couch_file:append_term(Fd, NewNode),
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
        {ok, Pos1} = couch_file:append_term(Fd, Node1),
        {ok, Pos2} = couch_file:append_term(Fd, Node2),

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
    LeastRestPos = [Pos || {Mbr, Pos} <- LeastRest],
    case insert_subtree(Fd, LeastPos, Subtree, Level, Depth+1) of
    {ok, NewMbr, NewPos, Inc} ->
        MergedMbr = vtree:merge_mbr(ParentMbr, NewMbr),
        NewNode = {MergedMbr, #node{type=inner}, [NewPos|LeastRestPos]},
        {ok, Pos} = couch_file:append_term(Fd, NewNode),
        {ok, NewMbr, Pos, Inc};
    {splitted, ChildMbr, {Child1Mbr, ChildPos1}, {Child2Mbr, ChildPos2}, Inc} ->
        MergedMbr = vtree:merge_mbr(ParentMbr, ChildMbr),
        LeastRestPos = [Pos || {Mbr, Pos} <- LeastRest],
        if
        % Both nodes of the split fit in the current inner node
        %length(EntriesPos)+2 =< ?MAX_FILLED ->
        length(LeastRestPos)+2 =< ?MAX_FILLED ->
            ChildrenPos = [ChildPos1, ChildPos2] ++ LeastRestPos,
            NewNode = {MergedMbr, #node{type=inner}, ChildrenPos},
            {ok, Pos} = couch_file:append_term(Fd, NewNode),
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
            {ok, Pos1} = couch_file:append_term(Fd, Node1),
            {ok, Pos2} = couch_file:append_term(Fd, Node2),
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
    {_, _, Nth} = lists:foldl(fun({Mbr, Pos}, {MinExp, Nth2, Cnt}) ->
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
log_n_ceil(N, 1) ->
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
        {[vtree_node()], file:io_device(), integer()}.
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
    {Mbrs, Poss} = lists:unzip(MbrAndPosList),
    Mbr = vtree:calc_mbr(Mbrs),
    {ok, RootPos} = couch_file:append_term(Fd, {Mbr, #node{type=inner}, Poss}),

    Nodes = lists:foldl(fun(I, Acc) ->
        Node = {{200+I,250+I,300+I,350+I}, #node{type=leaf},
            {"Node-" ++ integer_to_list(I), "Value-" ++ integer_to_list(I)}},
        [Node|Acc]
    end, [], lists:seq(1, NodesNum)),

    {Nodes, Fd, RootPos}.


%%%%% Tests %%%%%


%-ifdef(runnall).

bulk_load_test() ->
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
        {NodeId, {NodeMbr, NodeMeta, NodeData}} =
            vtree_test:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {[NodeId, <<"Bulk1">>], NodeData}},
        [Node|Acc]
    end, [], lists:seq(1,7)),

    {ok, Pos1, Height1} = bulk_load(Fd, 0, 0, Nodes1),
    ?debugVal(Pos1),
    ?assertEqual(2, Height1),
    {ok, Lookup1} = vtree:lookup(Fd, Pos1, {0,0,1001,1001}),
    ?assertEqual(7, length(Lookup1)),
    LeafDepths1 = vtreestats:leaf_depths(Fd, Pos1),
    ?assertEqual([1], LeafDepths1),

    % Load some more nodes
    Nodes2 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeData}} =
            vtree_test:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {[NodeId, <<"Bulk1">>], NodeData}},
        [Node|Acc]
    end, [], lists:seq(1,20)),

    {ok, Pos2, Height2} = bulk_load(Fd, Pos1, Height1, Nodes2),
    ?debugVal(Pos2),
    ?assertEqual(4, Height2),
    {ok, Lookup2} = vtree:lookup(Fd, Pos2, {0,0,1001,1001}),
    ?assertEqual(27, length(Lookup2)),
    LeafDepths2 = vtreestats:leaf_depths(Fd, Pos2),
    ?assertEqual([3], LeafDepths2),

    % Load some more nodes
    Nodes20 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeData}} =
            vtree_test:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {[NodeId, <<"Bulk1">>], NodeData}},
        [Node|Acc]
    end, [], lists:seq(1,1)),

    % Load some more nodes to find a bug where the tree gets unbalanced
    BulkSize3 = [20, 100, 18],
    Results3 = lists:foldl(fun(Size, Acc2) ->
        {RootPos, RootHeight, _, _} = hd(Acc2),
        Nodes = lists:foldl(fun(I, Acc) ->
           {NodeId, {NodeMbr, NodeMeta, NodeData}} =
                vtree_test:random_node({I,27+I*329,45}),
            Node = {NodeMbr, NodeMeta, {[NodeId, <<"Bulk1">>], NodeData}},
          [Node|Acc]
        end, [], lists:seq(1, Size)),

        {ok, Pos, Height} = bulk_load(Fd, RootPos, RootHeight, Nodes),
        ?debugVal(Pos),
        %?assertEqual(2, Height3),
        {ok, Lookup} = vtree:lookup(Fd, Pos, {0,0,1001,1001}),
        ?debugVal(length(Lookup)),
        %?assertEqual(14, length(Lookup2)),
        LeafDepths = vtreestats:leaf_depths(Fd, Pos),
        %?assertEqual([1], LeafDepths2),
        [{Pos, Height, LeafDepths, length(Lookup)}|Acc2]
    end, [{Pos2, Height2, [0], 0}], BulkSize3),
    ?debugVal(Results3),
    Results3_2 = [{H, Ld, Num} || {_, H, Ld, Num} <-
            tl(lists:reverse(Results3))],
    ?assertEqual([{4,[3],47},{5,[4],147},{5,[4],165}],Results3_2),

    BulkSize4 = [20, 17, 8, 64, 100],
    Results4 = lists:foldl(fun(Size, Acc2) ->
        {RootPos, RootHeight, _, _} = hd(Acc2),
        Nodes = lists:foldl(fun(I, Acc) ->
            {NodeId, {NodeMbr, NodeMeta, NodeData}} =
                vtree_test:random_node({I,27+I*329,45}),
            Node = {NodeMbr, NodeMeta, {[NodeId, <<"Bulk1">>], NodeData}},
           [Node|Acc]
        end, [], lists:seq(1, Size)),
        {ok, Pos, Height} = bulk_load(Fd, RootPos, RootHeight, Nodes),
        %?debugVal(Pos),
        %?debugVal(Height),
        {ok, Lookup} = vtree:lookup(Fd, Pos, {0,0,1001,1001}),
        %?debugVal(length(Lookup)),
        LeafDepths = vtreestats:leaf_depths(Fd, Pos),
       [{Pos, Height, LeafDepths, length(Lookup)}|Acc2]
    end, [{Pos2, Height2, [0], 0}], BulkSize4),
    ?debugVal(Results4),
    Results4_2 = [{H, Ld, Num} || {_, H, Ld, Num} <-
            tl(lists:reverse(Results4))],
    ?assertEqual([{4,[3],47},{4,[3],64},{4,[3],72},{5,[4],136},{6,[5],236}],
            Results4_2),
    ok.


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
    Fd = case couch_file:open(Filename, [create, overwrite]) of
    {ok, Fd2} ->
        Fd2;
    {error, _Reason} ->
        io:format("ERROR: Couldn't open file (~s) for tree storage~n",
                  [Filename])
    end,

    Nodes1 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeData}} =
            vtree_test:random_node({I,20+I*250,30}),
        Node = {NodeMbr, NodeMeta, {NodeId, NodeData}},
        [Node|Acc]
    end, [], lists:seq(1,9)),

    % Test 1: OMT tree with MAX_FILLED = 2
    {Omt1, _OmtHeight1} = omt_load(Nodes1, 2),
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


    % Test 2: OMT tree with MAX_FILLEd = 4
    {Omt2, _OmtHeight2} = omt_load(Nodes1, 4),
    {ok, MbrAndPosList2} = omt_write_tree(Fd, Omt2),
    RootPosList2 = [Pos || {Mbr, Pos} <- MbrAndPosList2],
    ?assertEqual(3, length(RootPosList2)),

    %[M1aNode, M1bNode, M1cNode] = RootPosList2,
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


    % Test 3: OMT tree with MAX_FILLEd = 4 and only one node
    Node3 = [{{68,132,678,722},#node{type=leaf},{<<"Node-1">>,<<"Value-1">>}}],

    {Omt3, OmtHeight3} = omt_load(Node3, 4),
    ?debugVal(Omt3),
    ?debugVal(OmtHeight3),
    {ok, MbrAndPosList3} = omt_write_tree(Fd, Omt3),
    ?debugVal(MbrAndPosList3),
    RootPosList3 = [Pos || {Mbr, Pos} <- MbrAndPosList3],
    ?debugVal(RootPosList3),
    {ok, Lookup3} = vtree:lookup(Fd, lists:nth(1,RootPosList3), {0,0,1001,1001}),
    ?debugVal(Lookup3),

    ?assertEqual(3, length(RootPosList2)),


    % Test 4: round-trip: some nodes => OMT tree, write to disk, load from disk
    {ok, LeafNodes1} = vtree:lookup(
            Fd, lists:nth(1, RootPosList), {0,0,1001,1001}),
    {ok, LeafNodes2} = vtree:lookup(
            Fd, lists:nth(2, RootPosList), {0,0,1001,1001}),
    LeafNodes3 = lists:foldl(fun(LeafNode, Acc) ->
        {NodeMbr, NodeId, NodeData} = LeafNode,
        Node = {NodeMbr, #node{type=leaf}, {NodeId, NodeData}},
        [Node|Acc]
    end, [], LeafNodes1 ++ LeafNodes2),
    {Omt4, _OmtHeight4} = omt_load(LeafNodes3, 2),
    ?assertEqual(Omt1, Omt4).


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
                    {<<"Node718132">>,<<"Value718132">>}}],7518}},
            {{220,45,980,960},{node,inner},{seedtree_leaf,[6286,3391],[],6520}}]},
        {{27,163,597,986},{node,inner},[
            {{37,163,597,911},{node,inner},{seedtree_leaf,[3732,5606],[],6006}},
            {{27,984,226,986},{node,inner},{seedtree_leaf,[5039],[],5494}}]}]},
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
    {_, _, SeedTree6TreeLeaf} = SeedTree6#seedtree_root.tree,

    SeedTree6Tree = SeedTree6#seedtree_root.tree,
    ?assertEqual({{4,43,980,986},{node,inner},[
        {{4,43,980,960},{node,inner},[
            {{4,43,865,787},{node,inner},{seedtree_leaf,[6688,7127,7348],[],7518}},
            {{220,45,980,960},{node,inner},{seedtree_leaf,[6286,3391],
                [{{342,456,959,513},{node,leaf},
                    {<<"intree01">>, datafoo3}}],6520}}]},
        {{27,163,597,986},{node,inner},[
            {{37,163,597,911},{node,inner},{seedtree_leaf,[3732,5606],[],6006}},
            {{27,984,226,986},{node,inner},{seedtree_leaf,[5039],[],5494}}]}]},
        SeedTree6Tree),
    SeedTree7 = seedtree_insert(SeedTree2, Node6),
    SeedTree7Tree = SeedTree7#seedtree_root.tree,
    ?assertEqual({{4,43,980,986},{node,inner},[
        {{4,43,980,960},{node,inner},[
            {{4,43,865,787},{node,inner},{seedtree_leaf,[6688,7127,7348],
                [{{66,132,252,718},{node,leaf},
                    {<<"Node718132">>,<<"Value718132">>}}],7518}},
            {{220,45,980,960},{node,inner},{seedtree_leaf,[6286,3391],
                [{{342,456,959,513},{node,leaf},
                    {<<"intree01">>, datafoo3}}],6520}}]},
        {{27,163,597,986},{node,inner},[
            {{37,163,597,911},{node,inner},{seedtree_leaf,[3732,5606],[],6006}},
            {{27,984,226,986},{node,inner},{seedtree_leaf,[5039],[],5494}}]}]},
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
                    {<<"Node718132">>,<<"Value718132">>}}],7518}},
            {{220,45,980,960},{node,inner},{seedtree_leaf,[6286,3391],[],6520}}]},
        {{27,163,597,986},{node,inner},[
            {{37,163,597,911},{node,inner},{seedtree_leaf,[3732,5606],[],6006}},
            {{27,984,226,986},{node,inner},{seedtree_leaf,[5039],[],5494}}]}]},
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
                    {<<"Node718132">>,<<"Value718132">>}}],7518}},
            {{220,45,980,960},{node,inner},{seedtree_leaf,[6286,3391],
                [{{342,456,959,513},{node,leaf},
                    {<<"intree01">>, datafoo3}}],6520}}]},
        {{27,163,597,986},{node,inner},[
            {{37,163,597,911},{node,inner},{seedtree_leaf,[3732,5606],[],6006}},
            {{27,984,226,986},{node,inner},{seedtree_leaf,[5039],[],5494}}]}]},
        SeedTree7Tree).

seedtree_init_test() ->
    {ok, {Fd, {RootPos, _}}} = vtree_test:build_random_tree(
            "/tmp/randtree.bin", 20),
    ?debugVal(RootPos),
    SeedTree1 = seedtree_init(Fd, RootPos, 2),
    ?assertEqual({seedtree_root, {{4,43,980,986}, {node,inner},
        [{{4,43,980,960}, {node,inner},{seedtree_leaf,[7518,6520],[],7579}},
         {{27,163,597,986},{node,inner},{seedtree_leaf,[6006,5494],[],6174}}]},
         [], 2},
        SeedTree1),
    SeedTree2 = seedtree_init(Fd, RootPos, 3),
    ?assertEqual({seedtree_root, {{4,43,980,986}, {node,inner},
        [{{4,43,980,960}, {node,inner},
            [{{4,43,865,787},{node,inner},{seedtree_leaf,[6688,7127,7348],[],7518}},
             {{220,45,980,960},{node,inner},{seedtree_leaf,[6286,3391],[],6520}}]},
         {{27,163,597,986}, {node,inner},
            [{{37,163,597,911},{node,inner},{seedtree_leaf,[3732,5606],[],6006}},
             {{27,984,226,986},{node,inner},{seedtree_leaf,[5039],[],5494}}]}]},
         [], 3},
        SeedTree2),

    % If you init a seedtree with MaxDepth bigger than the height of the
    % original tree, use the lowest level
    SeedTree3 = seedtree_init(Fd, RootPos, 5),
    SeedTree4 = seedtree_init(Fd, RootPos, 15),
    ?assertEqual(SeedTree3#seedtree_root.tree, SeedTree4#seedtree_root.tree).


seedtree_write_single_test() ->
    % insert single new node
    TargetTreeNodeNum = 7,
    {ok, {Fd, {RootPos, TargetTreeHeight}}} = vtree_test:build_random_tree(
            "/tmp/seedtree_write.bin", TargetTreeNodeNum),

    ?debugVal(RootPos),
    Nodes1 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeData}} =
            vtree_test:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {[NodeId, <<"bulk">>], NodeData}},
        [Node|Acc]
    end, [], lists:seq(1,1)),

    Seedtree1 = seedtree_init(Fd, RootPos, 1),
    ?debugVal(Seedtree1),
    Seedtree2 = seedtree_insert_list(Seedtree1, Nodes1),
    %?debugVal(Seedtree2),
    {ok, Result1, Height1, HeightDiff1} = seedtree_write(
            Fd, Seedtree2, TargetTreeHeight - Seedtree2#seedtree_root.height),
    ?debugVal(Result1),
    ?assertEqual(2, Height1),
    {ok, ResultPos1} = couch_file:append_term(Fd, hd(Result1)),
    ?debugVal(ResultPos1),
    {ok, Lookup1} = vtree:lookup(Fd, ResultPos1, {0,0,1001,1001}),
    ?assertEqual(8, length(Lookup1)),
    LeafDepths = vtreestats:leaf_depths(Fd, ResultPos1),
    ?assertEqual([1], LeafDepths).


seedtree_write_case1_test() ->
    % Test "Case 1: input R-tree fits in the target node" (chapter 6.1)
    % Test 1.1: input tree fits in
    % NOTE vmx: seedtree's maximum height should/must be height of the
    %    targetree - 2
    % XXX vmx: This test creates a strange root node
    % NOTE vmx: Not sure if that's still the case
    TargetTreeNodeNum = 25,
    {ok, {Fd, {RootPos, TargetTreeHeight}}} = vtree_test:build_random_tree(
            "/tmp/seedtree_write.bin", TargetTreeNodeNum),

    ?debugVal(RootPos),
    Nodes1 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeData}} =
            vtree_test:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {[NodeId, <<"bulk">>], NodeData}},
        [Node|Acc]
    end, [], lists:seq(1,20)),

    Seedtree1 = seedtree_init(Fd, RootPos, 1),
    ?debugVal(Seedtree1),
    Seedtree2 = seedtree_insert_list(Seedtree1, Nodes1),
    {ok, Result1, Height1, HeightDiff1} = seedtree_write(
            Fd, Seedtree2, TargetTreeHeight - Seedtree2#seedtree_root.height),
    ?debugVal(Result1),
    ?assertEqual(4, Height1),
    {ok, ResultPos1} = couch_file:append_term(Fd, hd(Result1)),
    ?debugVal(ResultPos1),
    {ok, Lookup1} = vtree:lookup(Fd, ResultPos1, {0,0,1001,1001}),
    ?assertEqual(45, length(Lookup1)),
    LeafDepths1 = vtreestats:leaf_depths(Fd, ResultPos1),
    ?assertEqual([3], LeafDepths1),

    % Test 1.2: input tree produces splits (seedtree height=1)
    TargetTreeNodeNum2 = 64,
    TargetTreeHeight2 = log_n_ceil(?MAX_FILLED, TargetTreeNodeNum2),
    {Nodes2, Fd2, RootPos2} = create_random_nodes_and_packed_tree(
        12, TargetTreeNodeNum2, ?MAX_FILLED),

    Seedtree2_1 = seedtree_init(Fd2, RootPos2, 1),
    ?debugVal(Seedtree2_1),
    Seedtree2_2 = seedtree_insert_list(Seedtree2_1, Nodes2),
    {ok, Result2, Height2, HeightDiff2} = seedtree_write(
            Fd2, Seedtree2_2,
            TargetTreeHeight2 - Seedtree2_2#seedtree_root.height),
    ?debugVal(Result2),
    ?assertEqual(3, Height2),
    {ok, ResultPos2} = write_parent(Fd2, Result2),
    ?debugVal(ResultPos2),
    {ok, Lookup2} = vtree:lookup(Fd2, ResultPos2, {0,0,1001,1001}),
    ?assertEqual(76, length(Lookup2)),
    LeafDepths2 = vtreestats:leaf_depths(Fd2, ResultPos2),
    ?assertEqual([3], LeafDepths2),


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
    {ok, Result3, Height3, HeightDiff3} = seedtree_write(
            Fd3, Seedtree3_2,
            TargetTreeHeight3 - Seedtree3_2#seedtree_root.height),
    ?debugVal(Result3),
    ?assertEqual(4, Height3),
    {ok, ResultPos3} = write_parent(Fd3, Result3),
    ?debugVal(ResultPos3),
    {ok, Lookup3} = vtree:lookup(Fd3, ResultPos3, {0,0,1001,1001}),
    ?assertEqual(208, length(Lookup3)),
    LeafDepths3 = vtreestats:leaf_depths(Fd3, ResultPos3),
    ?assertEqual([4], LeafDepths3),

    % Test 1.4: input tree produces splits (recursively) (seedtree height=3)
    TargetTreeNodeNum4 = 900,
    TargetTreeHeight4 = log_n_ceil(4, TargetTreeNodeNum4),
    ?debugVal(TargetTreeHeight4),
    {Nodes4, Fd4, RootPos4} = create_random_nodes_and_packed_tree(
        14, TargetTreeNodeNum4, 4),
    Seedtree4_1 = seedtree_init(Fd4, RootPos4, 3),
    ?debugVal(Seedtree4_1),
    Seedtree4_2 = seedtree_insert_list(Seedtree4_1, Nodes4),
    {ok, Result4, Height4, HeightDiff4} = seedtree_write(
            Fd4, Seedtree4_2,
            TargetTreeHeight4 - Seedtree4_2#seedtree_root.height),
    ?debugVal(Result4),
    ?assertEqual(5, Height4),
    {ok, ResultPos4} = write_parent(Fd4, Result4),
    ?debugVal(ResultPos4),
    {ok, Lookup4} = vtree:lookup(Fd4, ResultPos4, {0,0,1001,1001}),
    ?assertEqual(914, length(Lookup4)),
    LeafDepths4 = vtreestats:leaf_depths(Fd4, ResultPos4),
    ?assertEqual([5], LeafDepths4),

    % Test 1.5: adding new data with height=4 (seedtree height=1)
    TargetTreeNodeNum5 = 800,
    TargetTreeHeight5 = log_n_ceil(4, TargetTreeNodeNum5),
    ?debugVal(TargetTreeHeight5),
    {Nodes5, Fd5, RootPos5} = create_random_nodes_and_packed_tree(
        251, TargetTreeNodeNum5, 4),
    Seedtree5_1 = seedtree_init(Fd5, RootPos5, 1),
    ?debugVal(Seedtree5_1),
    Seedtree5_2 = seedtree_insert_list(Seedtree5_1, Nodes5),
    {ok, Result5, Height5, HeightDiff5} = seedtree_write(
            Fd5, Seedtree5_2,
            TargetTreeHeight5 - Seedtree5_2#seedtree_root.height),
    ?debugVal(Result5),
    ?assertEqual(5, Height5),
    {ok, ResultPos5} = write_parent(Fd5, Result5),
    ?debugVal(ResultPos5),
    {ok, Lookup5} = vtree:lookup(Fd5, ResultPos5, {0,0,1001,1001}),
    ?assertEqual(1051, length(Lookup5)),
    LeafDepths5 = vtreestats:leaf_depths(Fd5, ResultPos5),
    ?assertEqual([5], LeafDepths5).

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
    {ok, Result1, Height1, HeightDiff1} = seedtree_write(
            Fd, Seedtree2, TargetTreeHeight - Seedtree2#seedtree_root.height),
    ?debugVal(Result1),
    ?assertEqual(4, Height1),
    {ok, ResultPos1} = write_parent(Fd, Result1),
    ?debugVal(ResultPos1),
    {ok, Lookup1} = vtree:lookup(Fd, ResultPos1, {0,0,1001,1001}),
    ?assertEqual(105, length(Lookup1)),
    LeafDepths1 = vtreestats:leaf_depths(Fd, ResultPos1),
    ?assertEqual([4], LeafDepths1),

    % Test 2.2: input tree is too high (2 levels)
    Nodes2 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeData}} =
            vtree_test:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {[NodeId, <<"bulk">>], NodeData}},
        [Node|Acc]
    end, [], lists:seq(1,300)),

    Seedtree2_2 = seedtree_insert_list(Seedtree1, Nodes2),
    {ok, Result2, Height2, HeightDiff2} = seedtree_write(
            Fd, Seedtree2_2,
            TargetTreeHeight - Seedtree2_2#seedtree_root.height),
    ?debugVal(Result2),
    ?assertEqual(5, Height2),
    {ok, ResultPos2} = write_parent(Fd, Result2),
    ?debugVal(ResultPos2),
    {ok, Lookup2} = vtree:lookup(Fd, ResultPos2, {0,0,1001,1001}),
    ?assertEqual(325, length(Lookup2)),
    LeafDepths2 = vtreestats:leaf_depths(Fd, ResultPos2),
    ?assertEqual([5], LeafDepths2),

    % Test 2.3: input tree is too high (1 level) and procudes split
    % (seedtree height=1)
    TargetTreeNodeNum3 = 64,
    TargetTreeHeight3 = log_n_ceil(?MAX_FILLED, TargetTreeNodeNum3),
    {Nodes3, Fd3, RootPos3} = create_random_nodes_and_packed_tree(
        50, TargetTreeNodeNum3, ?MAX_FILLED),

    Seedtree3_1 = seedtree_init(Fd3, RootPos3, 1),
    ?debugVal(Seedtree3_1),
    Seedtree3_2 = seedtree_insert_list(Seedtree3_1, Nodes3),
    {ok, Result3, Height3, HeightDiff3} = seedtree_write(
            Fd3, Seedtree3_2,
            TargetTreeHeight3 - Seedtree3_2#seedtree_root.height),
    ?debugVal(Result3),
    ?assertEqual(3, Height3),
    {ok, ResultPos3} = write_parent(Fd3, Result3),
    ?debugVal(ResultPos3),
    {ok, Lookup3} = vtree:lookup(Fd3, ResultPos3, {0,0,1001,1001}),
    ?assertEqual(114, length(Lookup3)),
    LeafDepths3 = vtreestats:leaf_depths(Fd3, ResultPos3),
    ?assertEqual([3], LeafDepths3),

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
    {ok, Result4, Height4, HeightDiff4} = seedtree_write(
            Fd4, Seedtree4_2,
            TargetTreeHeight4 - Seedtree4_2#seedtree_root.height),
    ?debugVal(Result4),
    ?assertEqual(4, Height4),
    {ok, ResultPos4} = write_parent(Fd4, Result4),
    ?debugVal(ResultPos4),
    {ok, Lookup4} = vtree:lookup(Fd4, ResultPos4, {0,0,1001,1001}),
    ?assertEqual(246, length(Lookup4)),
    LeafDepths4 = vtreestats:leaf_depths(Fd4, ResultPos4),
    ?assertEqual([4], LeafDepths4),

    % Test 2.5: input tree is too high (2 levels) and produces multiple
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
    {ok, Result5, Height5, HeightDiff5} = seedtree_write(
            Fd5, Seedtree5_2,
            TargetTreeHeight5 - Seedtree5_2#seedtree_root.height),
    ?debugVal(Result5),
    ?assertEqual(4, Height5),
    {ok, ResultPos5} = write_parent(Fd5, Result5),
    ?debugVal(ResultPos5),
    {ok, Lookup5} = vtree:lookup(Fd5, ResultPos5, {0,0,1001,1001}),
    ?assertEqual(296, length(Lookup5)),
    LeafDepths5 = vtreestats:leaf_depths(Fd5, ResultPos5),
    ?assertEqual([4], LeafDepths5),

    % Test 2.6: input tree is too high (2 levels). Insert a OMT tree that
    % leads to massive overflow
    TargetTreeNodeNum6 = 20,
    TargetTreeHeight6 = log_n_ceil(4, TargetTreeNodeNum6),
    ?debugVal(TargetTreeHeight6),
    {Nodes6, Fd6, RootPos6} = create_random_nodes_and_packed_tree(
        200, TargetTreeNodeNum6, 4),
    Seedtree6_1 = seedtree_init(Fd6, RootPos6, 2),
    ?debugVal(Seedtree6_1),
    Seedtree6_2 = seedtree_insert_list(Seedtree6_1, Nodes6),
    {ok, Result6, Height6, HeightDiff6} = seedtree_write(
            Fd6, Seedtree6_2,
            TargetTreeHeight6 - Seedtree6_2#seedtree_root.height),
    ?debugVal(Result6),
    ?assertEqual(4, Height6),
    {ok, ResultPos6} = write_parent(Fd6, Result6),
    ?debugVal(ResultPos6),
    {ok, Lookup6} = vtree:lookup(Fd6, ResultPos6, {0,0,1001,1001}),
    ?assertEqual(220, length(Lookup6)),
    LeafDepths6 = vtreestats:leaf_depths(Fd6, ResultPos6),
    ?assertEqual([4], LeafDepths6).

seedtree_write_case3_test() ->
    % Test "Case 3: input R-tree is shorter than the level of the child level of the target node" (chapter 6.3)
    % Test 3.1: input tree is too small (1 level)
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
    end, [], lists:seq(1,12)),

    Seedtree1 = seedtree_init(Fd, RootPos, 1),
    ?debugVal(Seedtree1),
    Seedtree2 = seedtree_insert_list(Seedtree1, Nodes1),
    {ok, Result1, Height1, HeightDiff1} = seedtree_write(
            Fd, Seedtree2, TargetTreeHeight - Seedtree2#seedtree_root.height),
    ?debugVal(Result1),
    ?assertEqual(4, Height1),
    {ok, ResultPos1} = couch_file:append_term(Fd, hd(Result1)),
    {ok, Lookup1} = vtree:lookup(Fd, ResultPos1, {0,0,1001,1001}),
    ?assertEqual(37, length(Lookup1)),
    LeafDepths = vtreestats:leaf_depths(Fd, ResultPos1),
    ?assertEqual([3], LeafDepths),

    % Test 3.2: input tree is too small (2 levels)
    TargetTreeNodeNum2 = 80,
    % NOTE vmx: wonder why this tree is not really dense
    {ok, {Fd2, {RootPos2, TargetTreeHeight2}}} = vtree_test:build_random_tree(
            "/tmp/seedtree_write.bin", TargetTreeNodeNum2),
    ?debugVal(TargetTreeHeight2),
    ?debugVal(RootPos2),

    Nodes2 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeData}} =
            vtree_test:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {[NodeId, <<"bulk">>], NodeData}},
        [Node|Acc]
    end, [], lists:seq(1,12)),

    Seedtree2_1 = seedtree_init(Fd2, RootPos2, 1),
    ?debugVal(Seedtree2_1),
    Seedtree2_2 = seedtree_insert_list(Seedtree2_1, Nodes2),
    ?debugVal(Seedtree2_2),
    ?debugVal(Seedtree2_2#seedtree_root.height),
    {ok, Result2, Height2, HeightDiff2} = seedtree_write(
            Fd2, Seedtree2_2,
            TargetTreeHeight2 - Seedtree2_2#seedtree_root.height),
    ?debugVal(Height2),
    ?debugVal(HeightDiff2),
    ?debugVal(Result2),
    ?assertEqual(5, Height2),
    {ok, ResultPos2} = write_parent(Fd2, Result2),
    ?debugVal(ResultPos2),
    {ok, Lookup2_1} = vtree:lookup(Fd2, ResultPos2, {0,0,1001,1001}),
    ?assertEqual(92, length(Lookup2_1)),
    LeafDepths2 = vtreestats:leaf_depths(Fd2, ResultPos2),
    ?assertEqual([5], LeafDepths2),

    % Test 3.3: input tree is too small (1 level) and procudes split
    % (seedtree height=1)
    TargetTreeNodeNum3 = 64,
    TargetTreeHeight3 = log_n_ceil(?MAX_FILLED, TargetTreeNodeNum3),
    {Nodes3, Fd3, RootPos3} = create_random_nodes_and_packed_tree(
        4, TargetTreeNodeNum3, ?MAX_FILLED),

    Seedtree3_1 = seedtree_init(Fd3, RootPos3, 1),
    ?debugVal(Seedtree3_1),
    Seedtree3_2 = seedtree_insert_list(Seedtree3_1, Nodes3),
    {ok, Result3, Height3, HeightDiff3} = seedtree_write(
            Fd3, Seedtree3_2,
            TargetTreeHeight3 - Seedtree3_2#seedtree_root.height),
    ?debugVal(Result3),
    ?assertEqual(3, Height3),
    % Several parents => create new root node
    {ok, ResultPos3} = write_parent(Fd3, Result3),
    ?debugVal(RootPos3),
    ?debugVal(ResultPos3),
    {ok, Lookup3} = vtree:lookup(Fd3, ResultPos3, {0,0,1001,1001}),
    ?assertEqual(68, length(Lookup3)),
    LeafDepths3 = vtreestats:leaf_depths(Fd3, ResultPos3),
    ?assertEqual([3], LeafDepths3),


    % Test 3.4: input tree is too small (1 level) and produces multiple
    % splits (recusively) (seedtree height=2)
    TargetTreeNodeNum4 = 196,
    TargetTreeHeight4 = log_n_ceil(4, TargetTreeNodeNum4),
    ?debugVal(TargetTreeHeight4),
    {Nodes4, Fd4, RootPos4} = create_random_nodes_and_packed_tree(
        4, TargetTreeNodeNum4, 4),
    Seedtree4_1 = seedtree_init(Fd4, RootPos4, 2),
    ?debugVal(Seedtree4_1),
    Seedtree4_2 = seedtree_insert_list(Seedtree4_1, Nodes4),
    {ok, Result4, Height4, HeightDiff4} = seedtree_write(
            Fd4, Seedtree4_2,
            TargetTreeHeight4 - Seedtree4_2#seedtree_root.height),
    ?debugVal(RootPos4),
    ?debugVal(Result4),
    ?assertEqual(4, Height4),
    {ok, ResultPos4} = write_parent(Fd4, Result4),
    ?debugVal(ResultPos4),
    {ok, Lookup4} = vtree:lookup(Fd4, ResultPos4, {0,0,1001,1001}),
    ?assertEqual(200, length(Lookup4)),
    LeafDepths4 = vtreestats:leaf_depths(Fd4, ResultPos4),
    ?assertEqual([4], LeafDepths4),


    % Test 3.5: input tree is too small (2 levels) and produces multiple
    % splits (recusively) (seedtree height=2)
    TargetTreeNodeNum5 = 768,
    TargetTreeHeight5 = log_n_ceil(4, TargetTreeNodeNum5),
    ?debugVal(TargetTreeHeight5),
    {Nodes5, Fd5, RootPos5} = create_random_nodes_and_packed_tree(
        4, TargetTreeNodeNum5, 4),
    Seedtree5_1 = seedtree_init(Fd5, RootPos5, 2),
    ?debugVal(Seedtree5_1),
    Seedtree5_2 = seedtree_insert_list(Seedtree5_1, Nodes5),
    {ok, Result5, Height5, HeightDiff5} = seedtree_write(
            Fd5, Seedtree5_2,
            TargetTreeHeight5 - Seedtree5_2#seedtree_root.height),
    ?debugVal(Result5),
    ?assertEqual(5, Height5),
    {ok, ResultPos5} = couch_file:append_term(Fd5, hd(Result5)),
    ?debugVal(ResultPos5),
    {ok, Lookup5} = vtree:lookup(Fd5, ResultPos5, {0,0,1001,1001}),
    ?assertEqual(772, length(Lookup5)),
    LeafDepths5 = vtreestats:leaf_depths(Fd5, ResultPos5),
    ?assertEqual([4], LeafDepths5).


insert_subtree_test() ->
    {ok, {Fd, {RootPos, TreeHeight}}} = vtree_test:build_random_tree(
            "/tmp/randtree.bin", 20),
    ?debugVal(RootPos),
    ?debugVal(TreeHeight),

    % Test 1: no split of the root node
    Nodes1 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeData}} =
            vtree_test:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {[NodeId, <<"subtree">>], NodeData}},
        [Node|Acc]
    end, [], lists:seq(1,5)),
    {Omt1, SubtreeHeight1} = omt_load(Nodes1, ?MAX_FILLED),
    ?debugVal(SubtreeHeight1),
    {ok, MbrAndPosList1} = omt_write_tree(Fd, Omt1),

    {ok, NewMbr1, NewPos1, _Inc1} = insert_subtree(
            Fd, RootPos, MbrAndPosList1, 2),
    ?debugVal(NewPos1),
    {ok, Lookup1} = vtree:lookup(Fd, NewPos1, {0,0,1001,1001}),
    LeafDepths1 = vtreestats:leaf_depths(Fd, NewPos1),
    ?assertEqual(25, length(Lookup1)),
    ?assertEqual([3] , LeafDepths1),

    % Test 2: split of the root node => tree increases in height
    Nodes2 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeData}} =
            vtree_test:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {[NodeId, <<"subtree">>], NodeData}},
        [Node|Acc]
    end, [], lists:seq(1,250)),
    {Omt2, SubtreeHeight2} = omt_load(Nodes2, ?MAX_FILLED),
    ?debugVal(SubtreeHeight2),
    {ok, MbrAndPosList2} = omt_write_tree(Fd, Omt2),

    {ok, NewMbr2, NewPos2, Inc2} = insert_subtree(
            Fd, RootPos, MbrAndPosList2, 0),
    {ok, Lookup2} = vtree:lookup(Fd, NewPos2, {0,0,1001,1001}),
    LeafDepths2 = vtreestats:leaf_depths(Fd, NewPos2),
    ?assertEqual(1, Inc2),
    ?assertEqual(270, length(Lookup2)),
    ?assertEqual([4] , LeafDepths2).


insert_outliers_test() ->
    TargetTreeNodeNum = 25,
    % NOTE vmx: wonder why this tree is not really dense
    {ok, {Fd, {TargetPos, TargetHeight}}} = vtree_test:build_random_tree(
            "/tmp/seedtree_write.bin", TargetTreeNodeNum),
    {ok, {TargetMbr, _, _}} = couch_file:pread_term(Fd, TargetPos),
    ?debugVal(TargetHeight),
    ?debugVal(TargetPos),
    ?debugVal(TargetMbr),

    % Test 1: insert nodes that result in a tree with same height as the
    % target tree. Outliers build a tree with several nodes at the root
    % level. Rootnodes from newly created tree and rootnodes from target
    % tree *fit* into one node, there's no need for a split of the root node.
    Nodes1 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeData}} =
            vtree_test:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {[NodeId, <<"Out">>], NodeData}},
        [Node|Acc]
    end, [], lists:seq(1,300)),

    {ResultPos1, ResultHeight1} = insert_outliers(
            Fd, TargetPos, TargetMbr, TargetHeight, Nodes1),
    ?debugVal(ResultPos1),
    ?debugVal(ResultHeight1),
    ?assertEqual(5, ResultHeight1),
    {ok, Lookup1} = vtree:lookup(Fd, ResultPos1, {0,0,1001,1001}),
    ?assertEqual(325, length(Lookup1)),
    LeafDepths1 = vtreestats:leaf_depths(Fd, ResultPos1),
    % NOTE vmx: if the tree height is x, then the leaf depth is x-1
    ?assertEqual([4], LeafDepths1),

    % Test 2: insert nodes that result in a tree with same height as the
    % target tree. Outliers build a tree with several nodes at the root
    % level. Rootnodes from newly created tree and rootnodes from target
    % tree  *don't fit* into one node, root node needs to be split
    Nodes2 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeData}} =
            vtree_test:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {[NodeId, <<"Out">>], NodeData}},
        [Node|Acc]
    end, [], lists:seq(1,1000)),

    {ResultPos2, ResultHeight2} = insert_outliers(
            Fd, TargetPos, TargetMbr, TargetHeight, Nodes2),
    ?debugVal(ResultPos2),
    ?debugVal(ResultHeight2),
    ?assertEqual(6, ResultHeight2),
    {ok, Lookup2} = vtree:lookup(Fd, ResultPos2, {0,0,1001,1001}),
    ?assertEqual(1025, length(Lookup2)),
    LeafDepths2 = vtreestats:leaf_depths(Fd, ResultPos2),
    ?assertEqual([5], LeafDepths2),

    % Test 3: insert nodes that result in a tree which is higher than the
    % target tree. Outliers build a tree with several nodes at the root
    % level. Rootnodes from newly created tree and nodes from target
    % tree *fit* into one node, there's no need for a split of a node.
    Nodes3 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeData}} =
            vtree_test:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {[NodeId, <<"Out">>], NodeData}},
        [Node|Acc]
    end, [], lists:seq(1,1500)),

    {ResultPos3, ResultHeight3} = insert_outliers(
            Fd, TargetPos, TargetMbr, TargetHeight, Nodes3),
    ?debugVal(ResultPos3),
    ?debugVal(ResultHeight3),
    ?assertEqual(6, ResultHeight3),
    {ok, Lookup3} = vtree:lookup(Fd, ResultPos3, {0,0,1001,1001}),
    ?assertEqual(1525, length(Lookup3)),
    LeafDepths3 = vtreestats:leaf_depths(Fd, ResultPos3),
    ?assertEqual([5], LeafDepths3),

    % Test 4: insert nodes that result in a tree which is higher than the
    % target tree. Outliers build a tree with several nodes at the root
    % level. Rootnodes from newly created tree and nodes from target
    % tree *don't fit* into one node, it needs to be split.
    Nodes4 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeData}} =
            vtree_test:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {[NodeId, <<"Out">>], NodeData}},
        [Node|Acc]
    end, [], lists:seq(1,4000)),

    {ResultPos4, ResultHeight4} = insert_outliers(
            Fd, TargetPos, TargetMbr, TargetHeight, Nodes4),
    ?debugVal(ResultPos4),
    ?debugVal(ResultHeight4),
    ?assertEqual(7, ResultHeight4),
    {ok, Lookup4} = vtree:lookup(Fd, ResultPos4, {0,0,1001,1001}),
    ?assertEqual(4025, length(Lookup4)),
    LeafDepths4 = vtreestats:leaf_depths(Fd, ResultPos4),
    ?assertEqual([6], LeafDepths4),

    % Test 5: insert nodes that result in a tree which is smaller than the
    % target tree. Outliers build a tree with several nodes at the root
    % level. Rootnodes from newly created tree and nodes from target
    % tree *fit* into one node, there's no need for a split of a node.
    Nodes5 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeData}} =
            vtree_test:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {[NodeId, <<"Out">>], NodeData}},
        [Node|Acc]
    end, [], lists:seq(1,7)),

    {ResultPos5, ResultHeight5} = insert_outliers(
            Fd, TargetPos, TargetMbr, TargetHeight, Nodes5),
    ?debugVal(ResultPos5),
    ?debugVal(ResultHeight5),
    ?assertEqual(4, ResultHeight5),
    {ok, Lookup5} = vtree:lookup(Fd, ResultPos5, {0,0,1001,1001}),
    ?assertEqual(32, length(Lookup5)),
    LeafDepths5 = vtreestats:leaf_depths(Fd, ResultPos5),
    ?assertEqual([3], LeafDepths5),

    % Test 6: insert nodes that result in a tree which is smaller than the
    % target tree. Outliers build a tree with several nodes at the root
    % level. Rootnodes from newly created tree and nodes from target
    % tree *don't fit* into one node, it needs to be split.
    Nodes6 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeData}} =
            vtree_test:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {[NodeId, <<"Out">>], NodeData}},
        [Node|Acc]
    end, [], lists:seq(1,16)),

    {ResultPos6, ResultHeight6} = insert_outliers(
            Fd, TargetPos, TargetMbr, TargetHeight, Nodes6),
    ?debugVal(ResultPos6),
    ?debugVal(ResultHeight6),
    ?assertEqual(4, ResultHeight6),
    {ok, Lookup6} = vtree:lookup(Fd, ResultPos6, {0,0,1001,1001}),
    ?assertEqual(41, length(Lookup6)),
    LeafDepths6 = vtreestats:leaf_depths(Fd, ResultPos6),
    ?assertEqual([3], LeafDepths6).


seedtree_write_insert_test() ->
    Filename = "/tmp/swi.bin",
    Fd = case couch_file:open(Filename, [create, overwrite]) of
    {ok, Fd2} ->
        Fd2;
    {error, _Reason} ->
        io:format("ERROR: Couldn't open file (~s) for tree storage~n",
                  [Filename])
    end,

    Nodes1 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeData}} =
            vtree_test:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {NodeId, NodeData}},
        [Node|Acc]
    end, [], lists:seq(1,4)),
    Nodes2 = lists:foldl(fun(I, Acc) ->
        {NodeId, {NodeMbr, NodeMeta, NodeData}} =
            vtree_test:random_node({I,27+I*329,45}),
        Node = {NodeMbr, NodeMeta, {[NodeId, <<"OMT">>], NodeData}},
        [Node|Acc]
    end, [], lists:seq(1,7)),

    Mbr1 = vtree:calc_nodes_mbr(Nodes1),

    % This test will lead to an OMT tree with several nodes. There was a bug
    % that couldn't handle it.
    {OmtTree1, OmtHeight1} = omt_load(Nodes2, ?MAX_FILLED),
    {ok, Pos1} = couch_file:append_term(Fd, {Mbr1, #node{type=leaf}, Nodes1}),
    Result1 = seedtree_write_insert(Fd, [Pos1], OmtTree1, OmtHeight1),
    ?debugVal(Result1),
    ?assertEqual(3, length(Result1)),

    % This test will lead to an OMT tree with several nodes. Insert into a
    % single level target tree. There was a bug that couldn't handle it.
    Leafnode = {Mbr1, #node{type=leaf}, Nodes1},
    Result2 = seedtree_write_insert(Fd, [Leafnode], OmtTree1, OmtHeight1),
    ?debugVal(Result2),
    ?assertEqual(8, length(Result2)).

%-endif.
