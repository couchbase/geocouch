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

-module(vtree_delete).

-include("vtree.hrl").
-include("couch_db.hrl").

-export([delete/2]).

-ifdef(makecheck).
-compile(export_all).
-endif.


% The nodes that should get deleted don't need to be KV-nodes with every
% record field set. It's enough to have the `key` and the `docid` set.
-spec delete(Vt :: #vtree{}, Nodes :: [#kv_node{}]) -> #vtree{}.
delete(Vt, []) ->
    Vt;
delete(#vtree{root=nil}=Vt, _Nodes) ->
    Vt;
delete(Vt, Nodes) ->
    T1 = now(),
    Root = Vt#vtree.root,
    PartitionedNodes = [Nodes],
    KpNodes = delete_multiple(Vt, PartitionedNodes, [Root]),
    NewRoot = case KpNodes of
                  [] -> nil;
                  KpNodes ->
                      vtree_modify:write_new_root(Vt, KpNodes)
              end,
    ?LOG_DEBUG("Deletion took: ~ps~n",
               [timer:now_diff(now(), T1)/1000000]),
    Vt#vtree{root=NewRoot}.


-spec delete_multiple(Vt :: #vtree{}, ToDelete :: [#kv_node{}],
                      Existing :: [#kp_node{}]) -> [#kp_node{}].
delete_multiple(Vt, ToDelete, Existing) ->
    ModifyFuns = {fun delete_nodes/2, fun partition_nodes/3},
    vtree_modify:modify_multiple(Vt, ModifyFuns, ToDelete, Existing, []).


-spec delete_nodes(ToDelete :: [#kv_node{}], Existing :: [#kv_node{}]) ->
                          [#kv_node{}].
delete_nodes(ToDelete, Existing) ->
    % Filter out all children that should be deleted
    [E || E <- Existing, not(member_of_nodes(E, ToDelete))].


% Returns true if a given KV-node is member of a list of KV-nodes.
% `key` and `docid` are the fields that are used to determine whether it is
% a member or not.
-spec member_of_nodes(Node :: #kv_node{}, Nodes :: [#kv_node{}]) -> boolean().
member_of_nodes(_A, []) ->
    false;
member_of_nodes(A, [B|_]) when A#kv_node.key == B#kv_node.key andalso
                                  A#kv_node.docid == B#kv_node.docid ->
    true;
member_of_nodes(A, [_B|Rest]) ->
    member_of_nodes(A, Rest).


% Partitions a list of nodes according to a list of MBBs which are given by
% KP-nodes. The nodes are added to those partitions whose MBB fully encloses
% the given node. This means that one node may end up in multiple partitions.
% The reason is that for deletions there could be several search path where
% the node that should be deleted could be.
-spec partition_nodes(ToPartition :: [#kv_node{}], KpNodes :: [#kp_node{}],
                      Less :: lessfun()) -> [[#kv_node{}]].
partition_nodes(ToPartition, KpNodes, Less) ->
    lists:map(fun(#kp_node{key=PartitionMbb}) ->
                      % Filter out all nodes that are not within the MBB of
                      % the current node
                      [P || #kv_node{key=Mbb}=P <- ToPartition,
                            vtree_util:within_mbb(Mbb, PartitionMbb, Less)]
              end, KpNodes).
