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

% This module implements the insertion into the vtree. It follows the normal
% R-tree rules, but takes the "original MBB" into account, which is RR*-tree
% specific. It calls out to modules for the choosing the correct subtree and
% splitting the nodes.
%
% This insertion supports bulk insertion, i.e. adding multiple items at the
% same time. This is faster than subsequent single inserts.
%
% The reason for being faster is, that multiple nodes can be written
% at once, before their parent node is written.
%
% Here's a quick outline of the way it works. First you have a list
% of nodes that all want to be added to the tree. You start at the
% root node:
% 1. Get all child nodes
% 2. Loop through all nodes that should be inserted and group/partition
%    them into chunks. All nodes that would be added to a certain node
%    according to the choose_subtree function end up in one partition.
% 3. Not keep trafersing the tree in depth-first manner and start with 1.

-module(vtree_insert).

-include("vtree.hrl").
-include("couch_db.hrl").

-export([insert/2]).

-ifdef(makecheck).
-compile(export_all).
-endif.


-spec insert(Vt :: #vtree{}, Nodes :: [#kv_node{}]) -> #vtree{}.
insert(#vtree{root=nil}=Vt, Nodes) when length(Nodes) > Vt#vtree.fill_max ->
    T1 = now(),
    % If we would do single inserts, the first node that was inserted would
    % have set the original Mbb `MbbO`
    MbbO = get_key(hd(Nodes)),

    {Nodes2, Rest} = lists:split(Vt#vtree.fill_max, Nodes),
    KpNodes = write_nodes(Vt, Nodes2, MbbO),
    Root = write_new_root(Vt, KpNodes),
    Vt2 = Vt#vtree{root=Root},

    % NOTE vmx 2012-09-20: The value `math:log(Vt#vtree.fill_max)*50` is
    %     arbitrary, might be worth spending more benchmarking
    % NOTE vmx 2012-09-20: You can call it premature optimization, but it's
    %     really worth it. In the future the initial index building should be
    %     replaces with something better
    Vt3 = insert_in_bulks(Vt2, Rest, round(math:log(Vt#vtree.fill_max)*50)),
    ?LOG_DEBUG("Insertion into empty tree took: ~ps~n",
              [timer:now_diff(now(), T1)/1000000]),
    ?LOG_DEBUG("Root pos: ~p~n", [(Vt3#vtree.root)#kp_node.childpointer]),
    Vt3;
insert(#vtree{root=nil}=Vt, Nodes) ->
    % If we would do single inserts, the first node that was inserted would
    % have set the original Mbb `MbbO`
    MbbO = get_key(hd(Nodes)),
    [Root] = write_nodes(Vt, Nodes, MbbO),
    Vt#vtree{root=Root};
insert(Vt, Nodes) ->
    T1 = now(),
    #vtree{
        root = Root,
        less = Less
       } = Vt,

    PartitionedNodes = partition_nodes([Root], Nodes, Less),
    KpNodes = insert_multiple(Vt, PartitionedNodes, [Root], []),
    NewRoot = write_new_root(Vt, KpNodes),
    ?LOG_DEBUG("Insertion into existing tree took: ~ps~n",
               [timer:now_diff(now(), T1)/1000000]),
    ?LOG_DEBUG("Root pos: ~p~n", [NewRoot#kp_node.childpointer]),
    Vt#vtree{root=NewRoot}.


% Insert the data in a certain chunks
-spec insert_in_bulks(Vt :: #vtree{}, Nodes :: [#kv_node{}],
                      BulkSize :: non_neg_integer()) -> #vtree{}.
insert_in_bulks(Vt, [], _BulkSize) ->
    Vt;
insert_in_bulks(Vt, Nodes, BulkSize) when length(Nodes) > BulkSize ->
    {Insert, Rest} = lists:split(BulkSize, Nodes),
    Vt2 = insert(Vt, Insert),
    insert_in_bulks(Vt2, Rest, BulkSize);
insert_in_bulks(Vt, Nodes, _BulkSize) ->
    insert(Vt, Nodes).


% Inserts partitioned nodes into the corresponding position in the tree.
% This means that the number of partitions equals the number of nodes of the
% current level.
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
-spec insert_multiple(Vt :: #vtree{}, ToInsert :: [#kv_node{}],
                      TreeNodes :: [#kp_node{}], Acc :: [#kp_node{}]) ->
                             [#kp_node{}].
insert_multiple(_Vt, [], [], Acc) ->
    Acc;
% This partition doesn't contain any new nodes, hence use the existing ones
% and move on
insert_multiple(Vt, [[]|SiblingPartitions], [Child|Siblings], Acc) ->
    insert_multiple(Vt, SiblingPartitions, Siblings, [Child|Acc]);
% XXX FIXME vmx 2012-09-12: The KV-Nodes are always fully read, even the
%     geometry and the body. This is a waste of disk i/o. Change this, so
%     that it only contains the pointers. Do the same for nodes that get
%     inserted. First step should be writing the body a geometry and then
%     do the actual insertion into the tree.
insert_multiple(Vt, [Nodes|SiblingPartitions], [Child|Siblings], Acc) ->
    #vtree{
            fd = Fd,
            less = Less
          } = Vt,
    #kp_node{
              childpointer = ChildPointer,
              mbb_orig = MbbO
            } = Child,
    Children = vtree_io:read_node(Fd, ChildPointer),
    NewChildren = case Children of
                      [#kv_node{}|_] ->
                          Nodes ++ Children;
                      [#kp_node{}|_] ->
                          Partitions = partition_nodes(Children, Nodes, Less),
                          % Moving deeper
                          insert_multiple(Vt, Partitions, Children, [])
    end,

    WrittenNodes = write_nodes(Vt, NewChildren, MbbO),
    % Moving sideways
    insert_multiple(Vt, SiblingPartitions, Siblings, WrittenNodes ++ Acc).


% Partitions a list of nodes according to a list of MBBs which are given by
% KP-nodes.
-spec partition_nodes(KpNodes :: [#kp_node{}], ToPartition :: [#kv_node{}],
                      Less :: lessfun()) -> [[#kv_node{}]].
partition_nodes(KpNodes, ToPartition, Less) ->
    Partitions0 = lists:foldl(fun(_Num, Acc) ->
                                      [[]|Acc]
                              end, [], lists:seq(1, length(KpNodes))),
    PartitionMbbs = [Node#kp_node.key || Node <- KpNodes],
    % Choose the partition the new node should be inserted to.
    % vtree_choode:choose_subtree/3 expects a list of 2-tuples with the MBB
    % and any value you like. We use the index in the list as second element
    % in the tuple, so we can insert the new nodes there easily.
    NodesNumbered = lists:zip(PartitionMbbs,
                              lists:seq(1, length(PartitionMbbs))),
    lists:foldl(fun(Node, Partitions) ->
                       {_, NodeIndex} = vtree_choose:choose_subtree(
                                          NodesNumbered, Node#kv_node.key,
                                          Less),
                       add_to_nth(NodeIndex, Node, Partitions)
               end, Partitions0, ToPartition).


% Write a new root node for the given nodes. In case there are more than
% `fill_max` nodes, write a new root recursively. Stop when the root is a
% single node.
-spec write_new_root(Vt :: #vtree{}, Nodes :: [#kp_node{} | #kv_node{}]) ->
                            #kp_node{}.
write_new_root(_Vt, [Root]) ->
    Root;
% The `write_nodes/3` call will handle the splitting if needed. It could
% happen that the number of nodes returned by `write_nodes/3` is bigger
% than `fill_max`, hence the recursive call.
write_new_root(Vt, Nodes) ->
    MbbO = vtree_util:nodes_mbb(Nodes, Vt#vtree.less),
    WrittenNodes = write_nodes(Vt, Nodes, MbbO),
    write_new_root(Vt, WrittenNodes).


% Add a list of nodes one by one into a list of nodes (think of the latter
% list as list containing child nodes). The node the new nodes get inserted
% to, will automatically be split.
% The result will again be a list of multiple nodes, with a maximum of
% `fill_max` nodes (given by #vtree{}). The total number of elements in the
% resulting list can be bigger than `fill_max`, hence you might need to call
% it recursively.
-spec insert_into_nodes(Vt :: #vtree{},
                        NodePartitions :: [[#kv_node{} | #kp_node{}]],
                        MbbO :: mbb(), ToInsert :: [#kv_node{} | #kp_node{}])
                       -> [#kv_node{} | #kp_node{}].
insert_into_nodes(_Vt, NodePartitions, _MbbO, []) ->
    NodePartitions;
insert_into_nodes(Vt, NodePartitions, MbbO, [ToInsert|Rest]) ->
    #vtree{
            fill_max = FillMax,
            less = Less
          } = Vt,
    Mbb = get_key(ToInsert),

    % Every node partition contains a list of nodes, the maximum number is
    % `fill_max`
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
    NewNodes = case length(Nth) =:= FillMax of
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


%The commit to be http://review.couchbase.org/#patch,sidebyside,13557
% http://review.couchbase.org/#/c/17918/
% https://github.com/couchbase/couchdb/commit/9cb569466b7442d6285feaa2fd03c134b4978b92
% `MbbO` is the original MBB when it was create the first time
% It will return a list of KP-nodes. It might return more than `fill_max`
% nodes (but the number of nodes per node will be limited to `fill_max`).
-spec write_nodes(Vt :: #vtree{}, Nodes :: [#kv_node{} | #kp_node{}],
                  MbbO :: mbb()) -> [#kp_node{}].
write_nodes(#vtree{fill_max = FillMax} = Vt, Nodes, MbbO) when
      length(Nodes) =:= FillMax+1 ->
    {NodesA, NodesB} = split_node(Vt, Nodes, MbbO),
    write_multiple_nodes(Vt, [NodesA, NodesB]);
% Too many nodes for a single split, hence do something smart to get all
% nodes stored in a good way. First take the first FillMax nodes and
% split then into two nodes. Then insert all other nodes one by one into
% one of the two newly created nodes. The decision which node to choose is
% done by vtree_choose:choose_subtree/3.
write_nodes(#vtree{fill_max = FillMax} = Vt, Nodes, MbbO) when
      length(Nodes) > FillMax+1 ->
    {FirstNodes, Rest} = lists:split(FillMax, Nodes),
    NewNodes = insert_into_nodes(Vt, [FirstNodes], MbbO, Rest),
    write_multiple_nodes(Vt, NewNodes);
write_nodes(Vt, Nodes, MbbO) ->
    % `write_multiple_nodes/2` isn't used here, as it's the only case, where
    % we use the supplied MBBO and don't calculate a new one.
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


% Splits a KV- or KP-Node. Needs to be called with `fill_max+1` Nodes. It
% operates with #kv_node and #kp_node records and also returns those.
-spec split_node(Vt :: #vtree{}, Nodes :: [#kv_node{} | #kp_node{}],
                 MbbO :: mbb()) -> {[#kv_node{}], [#kv_node{}]} |
                                   {[#kp_node{}], [#kp_node{}]}.
split_node(Vt, [#kv_node{}|_]=Nodes, MbbO) ->
    #vtree{
            fill_min = FillMin,
            fill_max = FillMax,
            less = Less
          } = Vt,
    SplitNodes = [{Node#kv_node.key, Node} || Node <- Nodes],
    {SplitNodesA, SplitNodesB} = vtree_split:split_leaf(
                                   SplitNodes, MbbO, FillMin, FillMax, Less),
    {_, NodesA} = lists:unzip(SplitNodesA),
    {_, NodesB} = lists:unzip(SplitNodesB),
    {NodesA, NodesB};
split_node(Vt, [#kp_node{}|_]=Nodes, MbbO) ->
    #vtree{
            fill_min = FillMin,
            fill_max = FillMax,
            less = Less
          } = Vt,
    SplitNodes = [{Node#kp_node.key, Node} || Node <- Nodes],
    {SplitNodesA, SplitNodesB} = vtree_split:split_inner(
                                   SplitNodes, MbbO, FillMin, FillMax, Less),
    {_, NodesA} = lists:unzip(SplitNodesA),
    {_, NodesB} = lists:unzip(SplitNodesB),
    {NodesA, NodesB}.


% Add some value to a certain position in a list of lists
% `N` is the index starting at 1 for the first element
-spec add_to_nth(N :: pos_integer(), Element :: any(),
                 ListOfLists :: [list()]) -> [list()].
add_to_nth(N, Element, ListOfLists) ->
    {A, [Nth|B]} = lists:split(N-1, ListOfLists),
    A ++ [[Element|Nth]] ++ B.


-spec get_key(Node :: #kv_node{} | #kp_node{}) -> mbb().
get_key(#kv_node{}=Node) ->
    Node#kv_node.key;
get_key(#kp_node{}=Node) ->
    Node#kp_node.key.
