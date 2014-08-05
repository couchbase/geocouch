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

-module(vtree_cleanup).

-include("vtree.hrl").
-include("couch_db.hrl").

-export([cleanup/2]).

-ifdef(makecheck).
-compile(export_all).
-endif.


% Nodes get cleaned up by partition ID, nothing else matters.
-spec cleanup(Vt :: #vtree{}, Nodes :: [#kv_node{}]) -> #vtree{}.
cleanup(Vt, []) ->
    Vt;
cleanup(#vtree{root=nil}=Vt, _Nodes) ->
    Vt;
cleanup(Vt, Nodes) ->
    T1 = now(),
    Root = Vt#vtree.root,
    PartitionedNodes = [Nodes],
    KpNodes = cleanup_multiple(Vt, PartitionedNodes, [Root]),
    NewRoot = case KpNodes of
                  [] -> nil;
                  KpNodes ->
                      vtree_modify:write_new_root(Vt, KpNodes)
              end,
    ?LOG_DEBUG("Cleanup took: ~ps~n",
               [timer:now_diff(now(), T1)/1000000]),
    Vt#vtree{root=NewRoot}.

-spec cleanup_multiple(Vt :: #vtree{}, ToCleanup :: [#kv_node{}],
                       Existing :: [#kp_node{}]) -> [#kp_node{}].
cleanup_multiple(Vt, ToCleanup, Existing) ->
    ModifyFuns = {fun cleanup_nodes/2, fun partition_nodes/3},
    vtree_modify:modify_multiple(Vt, ModifyFuns, ToCleanup, Existing, []).


-spec cleanup_nodes(ToCleanup :: [#kv_node{}], Existing :: [#kv_node{}]) ->
                           [#kv_node{}].
cleanup_nodes(ToCleanup, Existing) ->
    % Filter out all children that should be deleted
    [E || E <- Existing, not(member_of_nodes(E, ToCleanup))].


% Returns true if a given KV-node is member of a list of KV-nodes.
% The `partition` is used to determine whether it is a member or not.
-spec member_of_nodes(Node :: #kv_node{}, Nodes :: [#kv_node{}]) -> boolean().
member_of_nodes(_A, []) ->
    false;
member_of_nodes(A, [B|_]) when A#kv_node.partition == B#kv_node.partition ->
    true;
member_of_nodes(A, [_B|Rest]) ->
    member_of_nodes(A, Rest).


% NOTE vmx 2014-08-05: This isn't efficient for the cleanup. But for now it's
% the easist possible way with maximum code re-use. The cleanup will be moved
% to C in one point anyway.
-spec partition_nodes(ToPartition :: [#kv_node{}], KpNodes :: [#kp_node{}],
                      Less :: lessfun()) -> [[#kv_node{}]].
partition_nodes(ToPartition, KpNodes, _Less) ->
    % Put the node into every partition as we want to search the full tree
    lists:map(fun(_) -> ToPartition end, KpNodes).
