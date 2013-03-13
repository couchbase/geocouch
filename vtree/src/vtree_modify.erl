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

% This module implements the modification of the vtree for insertion and
% deletion. It follows the normal R-tree rules, but takes the "original MBB"
% into account, which is RR*-tree specific. It calls out to modules for the
% choosing the correct subtree and splitting the nodes.
%
% This insertion/deletion supports bulks, i.e. adding/removing multiple items
% at the same time. This is faster than subsequent single inserts/deletes.
% The reason for being faster is, that multiple nodes can be written
% at once, before their parent node is written.
%
% This module is the central piece for the algorithm. The modules vtree_insert
% and vtree_delete implement the specific actions that are different between
% insertion and deletion. But as you can see they are very similar, only the
% function that handle the KP-nodes during traversal and the KV-nodes during
% modification are different.
%
% Here's a quick outline of the way the bulk operations work. First you have a
% list of nodes that all want to be added/deleted to the tree. You start at
% the root node:
% 1. Get all child nodes
% 2. Loop through all nodes that should be inserted/delete and group/partition
%    them into chunks. All nodes that would be added to/removed from a certain
%    node according to the partition function end up in one partition.
% 3. Now keep traversing the tree in depth-first manner and start with 1.,
%    until you hit a leaf node. There you do the actual insertion/deletion.

-module(vtree_modify).

-include("vtree.hrl").
-include("couch_db.hrl").

-export([write_new_root/2, write_nodes/3, modify_multiple/5,
         get_overflowing_subset/2]).

-ifdef(makecheck).
-compile(export_all).
-endif.


% Write a new root node for the given nodes. In case there are more than
% than it can hold, write a new root recursively. Stop when the root is a
% single node.
-spec write_new_root(Vt :: #vtree{}, Nodes :: [#kp_node{} | #kv_node{}]) ->
                            #kp_node{}.
write_new_root(_Vt, [Root]) ->
    Root;
% The `write_nodes/3` call will handle the splitting if needed. It could
% happen that the byte size of nodes returned by `write_nodes/3` is bigger
% than the chunk threshold, hence the recursive call.
write_new_root(Vt, Nodes) ->
    MbbO = vtree_util:nodes_mbb(Nodes, Vt#vtree.less),
    WrittenNodes = write_nodes(Vt, Nodes, MbbO),
    write_new_root(Vt, WrittenNodes).


% Add a list of nodes one by one into a list of nodes (think of the latter
% list as list containing child nodes). The node the new nodes get inserted
% to, will automatically be split.
% The result will again be a list of multiple nodes, with a maximum number of
% nodes that still satisfy the chunk threshold. The total number of elements
% in the resulting list can be bigger than the node can actually hold, hence
% you might need to call it recursively.
-spec insert_into_nodes(Vt :: #vtree{},
                        NodePartitions :: [[#kv_node{} | #kp_node{}]],
                        MbbO :: mbb(), ToInsert :: [#kv_node{} | #kp_node{}])
                       -> [#kv_node{} | #kp_node{}].
insert_into_nodes(_Vt, NodePartitions, _MbbO, []) ->
    NodePartitions;
insert_into_nodes(Vt, NodePartitions, MbbO, [ToInsert|Rest]) ->
    Less = Vt#vtree.less,
    Mbb = get_key(ToInsert),
    FillMax = get_chunk_threshold(Vt, ToInsert),

    % Every node partition contains a list of nodes, the maximum number is
    % given by the chunk threshold
    % Start with calculating the MBBs of the partitions
    PartitionMbbs = [vtree_util:nodes_mbb(Nodes, Less) ||
                        Nodes <- NodePartitions],

    % Choose the partition the new node should be inserted to.
    % vtree_choode:choose_subtree/3 expects a list of 2-tuples with the MBB
    % and any value you like. We use the index in the list as second element
    % in the tuple, so we can insert the new nodes there easily.
    NodesNumbered = lists:zip(PartitionMbbs,
                              lists:seq(0, length(PartitionMbbs)-1)),
    {_, NodeIndex} = vtree_choose:choose_subtree(NodesNumbered, Mbb, Less),
    {A, [Nth|B]} = lists:split(NodeIndex, NodePartitions),
    NewNodes = case erlang:external_size(Nth) > FillMax of
                   % Maximum number of nodes reached, hence split it
                   true ->
                       {C, D} = split_node(Vt, [ToInsert|Nth], MbbO),
                       A ++ [C, D] ++ B;
                   % No need to split the node, just insert the new one
                   false ->
                       C = [ToInsert|Nth],
                       A ++ [C] ++ B
               end,
    insert_into_nodes(Vt, NewNodes, MbbO, Rest).


-spec get_key(Node :: #kv_node{} | #kp_node{}) -> mbb().
get_key(#kv_node{}=Node) ->
    Node#kv_node.key;
get_key(#kp_node{}=Node) ->
    Node#kp_node.key.


-spec get_chunk_threshold(Vt :: #vtree{}, Node :: #kv_node{} | #kp_node{}) ->
                                 number().
get_chunk_threshold(Vt, #kv_node{}) ->
    Vt#vtree.kv_chunk_threshold;
get_chunk_threshold(Vt, #kp_node{}) ->
    Vt#vtree.kp_chunk_threshold.


% Return a minimal subset of nodes that are just about bigger than `FillMax`
-spec get_overflowing_subset(FillMax :: number(),
                             Nodes :: [#kv_node{} | #kp_node{}]) ->
                                    {[#kv_node{}], [#kv_node{}]} |
                                    {[#kp_node{}], [#kp_node{}]}.
get_overflowing_subset(FillMax, Nodes) ->
    get_overflowing_subset(FillMax, Nodes, []).
-spec get_overflowing_subset(FillMax :: number(),
                             Nodes :: [#kv_node{} | #kp_node{}],
                             Acc :: [#kv_node{} | #kp_node{}]) ->
                                    {[#kv_node{}], [#kv_node{}]} |
                                    {[#kp_node{}], [#kp_node{}]}.
get_overflowing_subset(_FillMax, [], Acc) ->
    {lists:reverse(Acc), []};
get_overflowing_subset(FillMax, [H|T]=Nodes, Acc) ->
    case erlang:external_size(Acc) < FillMax of
        true ->
            get_overflowing_subset(FillMax, T, [H|Acc]);
        false ->
            {lists:reverse(Acc), Nodes}
    end.


% `MbbO` is the original MBB when it was create the first time
% It will return a list of KP-nodes. It might return more than the chunk
% threshold (but the size of the nodes per node will be limited by the chunk
% threshold).
-spec write_nodes(Vt :: #vtree{}, Nodes :: [#kv_node{} | #kp_node{}],
                  MbbO :: mbb()) -> [#kp_node{}].
% All nodes were deleted, the current node is empty now
write_nodes(_Vt, [], _MbbO) ->
    [];
write_nodes(Vt, [#kv_node{}|_]=Nodes, MbbO) ->
    FillMax = Vt#vtree.kv_chunk_threshold,
    Size = erlang:external_size(Nodes),
    write_nodes(Vt, Nodes, MbbO, FillMax, Size);
write_nodes(Vt, [#kp_node{}|_]=Nodes, MbbO) ->
    FillMax = Vt#vtree.kp_chunk_threshold,
    Size = erlang:external_size(Nodes),
    write_nodes(Vt, Nodes, MbbO, FillMax, Size).
% Too many nodes for a single split, hence do something smart to get
% all nodes stored in a good way. First take the first FillMax nodes
% and split then into two nodes. Then insert all other nodes one by
% one into one of the two newly created nodes. The decision which node
% to choose is done by vtree_choose:choose_subtree/3.
-spec write_nodes(Vt :: #vtree{}, Nodes :: [#kv_node{} | #kp_node{}],
                  MbbO :: mbb(), FillMax :: number(), Size :: pos_integer()) ->
                         [#kp_node{}].
write_nodes(Vt, Nodes, MbbO, FillMax, Size) when Size > 2*FillMax ->
    {FirstNodes, Rest} = get_overflowing_subset(FillMax, Nodes),
    NewNodes = insert_into_nodes(Vt, [FirstNodes], MbbO, Rest),
    write_multiple_nodes(Vt, NewNodes);
% A simple split into two nodes is possible
write_nodes(Vt, Nodes, MbbO, FillMax, Size) when Size > FillMax ->
    {NodesA, NodesB} = split_node(Vt, Nodes, MbbO),
    write_multiple_nodes(Vt, [NodesA, NodesB]);
% No split needed
write_nodes(Vt, Nodes, MbbO, _FillMax, _Size) ->
    % `write_multiple_nodes/2` isn't used here, as it's the only case,
    % where we use the supplied MBBO and don't calculate a new one.
    %write_multiple_nodes(Vt, [Nodes], [MbbO]).
    #vtree{
            fd = Fd,
            less = Less
          } = Vt,
    {ok, WrittenNode} = vtree_io:write_node(Fd, Nodes, Less),
    [WrittenNode#kp_node{mbb_orig = MbbO}].


% Write multiple nodes at once. It is only called when a split happened, hence
% the original MBB (`MbbO`) is reset.
-spec write_multiple_nodes(Vt :: #vtree{},
                           NodeList :: [[#kp_node{} | #kv_node{}]]) ->
                                  [#kp_node{}].
write_multiple_nodes(Vt, NodeList) ->
    #vtree{
            fd = Fd,
            less = Less
          } = Vt,
    lists:map(
      fun(Nodes) ->
              {ok, WrittenNode} = vtree_io:write_node(Fd, Nodes, Less),
              WrittenNode#kp_node{mbb_orig = WrittenNode#kp_node.key}
      end, NodeList).


% Splits a KV- or KP-Node. Needs to be called with a total size of the nodes
% that can be accommodated by two nodes. It % operates with #kv_node{} and
% #kp_node{} records and also returns those.
-spec split_node(Vt :: #vtree{}, Nodes :: [#kv_node{} | #kp_node{}],
                 MbbO :: mbb()) -> {[#kv_node{}], [#kv_node{}]} |
                                   {[#kp_node{}], [#kp_node{}]}.
split_node(Vt, [#kv_node{}|_]=Nodes, MbbO) ->
    #vtree{
       kv_chunk_threshold = FillMax,
       min_fill_rate = MinRate,
       less = Less
      } = Vt,
    SplitNodes = [{Node#kv_node.key, Node} || Node <- Nodes],
    {SplitNodesA, SplitNodesB} = vtree_split:split_leaf(
                                   SplitNodes, MbbO, FillMax*MinRate, FillMax,
                                   Less),
    {_, NodesA} = lists:unzip(SplitNodesA),
    {_, NodesB} = lists:unzip(SplitNodesB),
    {NodesA, NodesB};
split_node(Vt, [#kp_node{}|_]=Nodes, MbbO) ->
    #vtree{
       kp_chunk_threshold = FillMax,
       min_fill_rate = MinRate,
       less = Less
      } = Vt,
    SplitNodes = [{Node#kp_node.key, Node} || Node <- Nodes],
    {SplitNodesA, SplitNodesB} = vtree_split:split_inner(
                                   SplitNodes, MbbO, FillMax*MinRate, FillMax,
                                   Less),
    {_, NodesA} = lists:unzip(SplitNodesA),
    {_, NodesB} = lists:unzip(SplitNodesB),
    {NodesA, NodesB}.


% The following comment explains the insertion only to make it easier to
% understand, though it works the same for deletions.
%
% This function inserts partitioned nodes into the corresponding position in
% the tree. This means that the number of partitions equals the number of
% nodes of the current level.
% The whole algorithm inserts multiple nodes into the tree and tries to
% minimize the rebuilding of the tree (caused by his append-only nature).
% The goal is to write parent nodes only when all the inserts in their
% children already happened. It's done by depth first traversal, where the
% nodes that get inserted are partitioned according to the currently level.
% Those partitions are then further divided while moving deeper.
%
% Here's an example:
%
%     PartitionedNodes = [[kv_node1, kv_node2], [kv_node3], [], [kv_node4]]
%
% The `PartitionedNodes` has 4 elements, hence the level it get inserted to
% needs to have a exactly 4 nodes
%
%     CurrentLevel = [kp_node1, kp_node2, kp_node2, kp_node4]
%
% The Partitioned nodes get inserted into the `CurrentLevel`, hence
%
%     kp_node1 gets nodes kv_node1 and kv_node2 added
%     kp_node2 gets node kv_node3 added
%     kp_node3 keeps it's original children
%     kp_node4 gets node kv_node4 added
%
% The KP-nodes are traversed recursively and the KV-Nodes that get added are
% also partitioned recursively, so that in the end the KP-nodes get added
% as leaf nodes to the existing KP-nodes.
-spec modify_multiple(Vt :: #vtree{}, ModifyFuns :: {fun(), fun()},
                      Modify :: [#kv_node{}], Existing :: [#kp_node{}],
                      Acc :: [#kp_node{}]) ->
                             [#kp_node{}].
modify_multiple(_Vt, _ModifyFuns, [], [], Acc) ->
    Acc;
% This partition doesn't contain any nodes to delete, hence use the existing
% ones and move on
modify_multiple(Vt, ModifyFuns, [[]|SiblingPartitions],
                [Existing|ExistingSiblings], Acc) ->
    modify_multiple(Vt, ModifyFuns, SiblingPartitions, ExistingSiblings,
                    [Existing|Acc]);
modify_multiple(Vt, ModifyFuns, [ModifyNodes|SiblingPartitions],
                [Existing|ExistingSiblings], Acc) ->
    #vtree{
            fd = Fd,
            less = Less
          } = Vt,
    #kp_node{
              childpointer = ChildPointer,
              mbb_orig = MbbO
            } = Existing,
    {KvModifyFun, KpModifyFun} = ModifyFuns,
    ExistingNodes = vtree_io:read_node(Fd, ChildPointer),
    NewChildren =
        case ExistingNodes of
            [#kv_node{}|_] ->
                KvModifyFun(ModifyNodes, ExistingNodes);
            [#kp_node{}|_] ->
                Partitions = KpModifyFun(ModifyNodes, ExistingNodes, Less),
                % Moving deeper
                modify_multiple(Vt, ModifyFuns, Partitions, ExistingNodes, [])
    end,
    WrittenNodes = write_nodes(Vt, NewChildren, MbbO),
    % Moving sideways
    modify_multiple(Vt, ModifyFuns, SiblingPartitions, ExistingSiblings,
                    WrittenNodes ++ Acc).
