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

-module(vtree_insert).

-include("vtree.hrl").
-include("couch_db.hrl").

-export([insert/2]).

-ifdef(makecheck).
-compile(export_all).
-endif.


-spec insert(Vt :: #vtree{}, Nodes :: [#kv_node{}]) -> #vtree{}.
insert(Vt, []) ->
    Vt;
insert(#vtree{root=nil}=Vt, Nodes) ->
    T1 = now(),
    % If we would do single inserts, the first node that was inserted would
    % have set the original Mbb `MbbO`
    MbbO = (hd(Nodes))#kv_node.key,
    Threshold = Vt#vtree.kv_chunk_threshold,

    case ?ext_size(Nodes) > Threshold of
        true ->
            {Nodes2, Rest} = vtree_modify:get_overflowing_subset(
                               Threshold, Nodes),
            KpNodes = vtree_modify:write_nodes(Vt, Nodes2, MbbO),
            Root = vtree_modify:write_new_root(Vt, KpNodes),
            Vt2 = Vt#vtree{root=Root},

            % NOTE vmx 2012-09-20: The value of `ArbitraryBulkSize` is
            %     arbitrary, might be worth spending more benchmarking
            % NOTE vmx 2012-09-20: You can call it premature optimization, but it's
            %     really worth it. In the future the initial index building should be
            %     replaces with something better
            ArbitraryBulkSize = round(math:log(Threshold)+50),
            Vt3 = insert_in_bulks(Vt2, Rest, ArbitraryBulkSize),
            ?LOG_DEBUG("Insertion into empty tree took: ~ps~n",
                      [timer:now_diff(now(), T1)/1000000]),
            ?LOG_DEBUG("Root pos: ~p~n", [(Vt3#vtree.root)#kp_node.childpointer]),
            Vt3;
        false ->
            [Root] = vtree_modify:write_nodes(Vt, Nodes, MbbO),
            Vt#vtree{root=Root}
    end;
insert(Vt, Nodes) ->
    T1 = now(),
    Root = Vt#vtree.root,
    PartitionedNodes = [Nodes],
    KpNodes = insert_multiple(Vt, PartitionedNodes, [Root]),
    NewRoot = vtree_modify:write_new_root(Vt, KpNodes),
    ?LOG_DEBUG("Insertion into existing tree took: ~ps~n",
               [timer:now_diff(now(), T1)/1000000]),
    Vt#vtree{root=NewRoot}.


% NOTE vmx 2013-03-12: It might make sense to change the bulk size from
%      using the number of nodes, to a byte size based way.
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


-spec insert_multiple(Vt :: #vtree{}, ToInsert :: [#kv_node{}],
                      Existing :: [#kp_node{}]) -> [#kp_node{}].
insert_multiple(Vt, ToInsert, Existing) ->
    ModifyFuns = {fun insert_nodes/2, fun partition_nodes/3},
    vtree_modify:modify_multiple(Vt, ModifyFuns, ToInsert, Existing, []).


-spec insert_nodes(ToInsert :: [#kv_node{}], Existing :: [#kv_node{}]) ->
                          [#kv_node{}].
insert_nodes(ToInsert, Existing) ->
    ToInsert ++ Existing.


% Partitions a list of nodes according to a list of MBBs which are given by
% KP-nodes.
-spec partition_nodes(ToPartition :: [#kv_node{}], KpNodes :: [#kp_node{}],
                      Less :: lessfun()) -> [[#kv_node{}]].
partition_nodes(ToPartition, KpNodes, Less) ->
    Partitions0 = [[] || _ <- lists:seq(1, length(KpNodes))],
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


% Add some value to a certain position in a list of lists
% `N` is the index starting at 1 for the first element
-spec add_to_nth(N :: pos_integer(), Element :: any(),
                 ListOfLists :: [list()]) -> [list()].
add_to_nth(N, Element, ListOfLists) ->
    {A, [Nth|B]} = lists:split(N-1, ListOfLists),
    A ++ [[Element|Nth]] ++ B.
