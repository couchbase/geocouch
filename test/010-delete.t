#!/usr/bin/env escript
%% -*- erlang -*-
%%! -na me insert@127.0.0.1

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

-include_lib("../include/vtree.hrl").

-define(MOD, vtree_delete).
-define(FILENAME, "/tmp/vtree_delete_vtree.bin").

main(_) ->
    % Set the random seed once. It might be reset a certain tests
    random:seed(1, 11, 91),

    code:add_pathz(filename:dirname(escript:script_name())),
    etap:plan(38),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            % Somehow etap:diag/1 and etap:bail/1 don't work properly
            %etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            %etap:bail(Other),
            io:format(standard_error, "Test died abnormally:~n~p~n", [Other])
     end.


test() ->
    couch_file_write_guard:sup_start_link(),
    test_delete(),
    test_delete_multiple(),
    test_delete_nodes(),
    test_member_of_nodes(),
    test_partition_nodes(),
    ok.


test_delete() ->
    % There are not many tests in here, the mjority of the functionality
    % is already tested with test_delete_multiple/1.
    Fd = vtree_test_util:create_file(?FILENAME),
    Nodes1 = vtree_test_util:generate_kvnodes(6),

    Vtree1 = #vtree{
      fd = Fd,
      fill_min = 4,
      fill_max = 8,
      less = fun(A, B) -> A < B end
     },

    Vtree2 = vtree_insert:insert(Vtree1, Nodes1),
    ToDelete3 = lists:sublist(Nodes1, 2, 3),
    Vtree3 = #vtree{root=Root3} = ?MOD:delete(Vtree2, ToDelete3),
    {Depths3, KvNodes3} = vtree_test_util:get_kvnodes(
                            Vtree3#vtree.fd, Root3#kp_node.childpointer),
    etap:is(sets:size(sets:from_list(Depths3)), 1, "Tree is balanced"),
    etap:is(lists:sort(KvNodes3), lists:sort(Nodes1 -- ToDelete3),
            "6 nodes inserted, 3 deleted"),

    Vtree4 = ?MOD:delete(Vtree2, Nodes1),
    etap:is(Vtree4#vtree.root, nil, "6 nodes inserted, all deleted"),


    Nodes2 = vtree_test_util:generate_kvnodes(174),
    Vtree5 = vtree_insert:insert(Vtree1, Nodes2),
    ToDelete6 = lists:sublist(Nodes2, 23, 107),
    Vtree6 = #vtree{root=Root6} = ?MOD:delete(Vtree5, ToDelete6),
    {Depths6, KvNodes6} = vtree_test_util:get_kvnodes(
                            Vtree6#vtree.fd, Root6#kp_node.childpointer),
    etap:is(sets:size(sets:from_list(Depths6)), 1, "Tree is balanced"),
    etap:is(lists:sort(KvNodes6), lists:sort(Nodes2 -- ToDelete6),
            "174 nodes inserted, 107 deleted"),

    Vtree7 = ?MOD:delete(Vtree5, Nodes2),
    etap:is(Vtree4#vtree.root, nil, "174 nodes inserted, all deleted"),

    couch_file:close(Fd).


test_delete_multiple() ->
    Fd = vtree_test_util:create_file(?FILENAME),

    Vtree1 = #vtree{
      fd = Fd,
      fill_min = 2,
      fill_max = 4,
      less = fun(A, B) -> A < B end
     },

    Tests = [
             {Vtree1, 18, 4, 14},
             {Vtree1, 50, 29, 12},
             {Vtree1, 50, 29, 1},
             {Vtree1, 4, 1, 1},
             {Vtree1, 4, 1, 4},
             {Vtree1, 37, 1, 37},
             {Vtree1, 37, 1, 36}
            ],
    lists:foreach(fun({Vtree, NumNodes, From, To}) ->
                          delete_multiple(Vtree, NumNodes, From, To)
                  end, Tests),

    couch_file:close(Fd).

delete_multiple(Vtree0, NumNodes, From, NumDelete) ->
    Nodes = vtree_test_util:generate_kvnodes(NumNodes),
    Vtree = #vtree{root=Root} = vtree_insert:insert(Vtree0, Nodes),
    ToDelete = lists:sublist(Nodes, From, NumDelete),
    case ?MOD:delete_multiple(Vtree, [ToDelete], [Root]) of
        [KpNode] ->
            {Depths, KvNodes} = vtree_test_util:get_kvnodes(
                                  Vtree#vtree.fd, KpNode#kp_node.childpointer),
            etap:is(sets:size(sets:from_list(Depths)), 1, "Tree is balanced"),
            etap:is(lists:sort(KvNodes), lists:sort(Nodes -- ToDelete),
                    io_lib:format("Deleted ~p node(s) out of ~p node(s)",
                                  [NumDelete, NumNodes]));
        [] ->
            etap:is(NumNodes, NumDelete, "All nodes were deleted")
    end.


test_delete_nodes() ->
    [Node1, Node2, Node3, Node4, Node5] = Nodes =
        vtree_test_util:generate_kvnodes(5),
    etap:is(?MOD:delete_nodes([Node1], Nodes), [Node2, Node3, Node4, Node5],
            "First node got delete"),
    etap:is(?MOD:delete_nodes([Node2], Nodes), [Node1, Node3, Node4, Node5],
            "Second node got delete"),
    etap:is(?MOD:delete_nodes([Node5], Nodes), [Node1, Node2, Node3, Node4],
            "Last node got delete"),
    etap:is(?MOD:delete_nodes([Node5, Node2], Nodes), [Node1, Node3, Node4],
            "Two nodes got delete"),
    etap:is(?MOD:delete_nodes([Node4, Node1, Node3], Nodes), [Node2, Node5],
            "Three nodes got delete"),
    etap:is(lists:sort(?MOD:delete_nodes(Nodes, Nodes)), [],
            "All nodes got delete").


test_member_of_nodes() ->
    Nodes1 = vtree_test_util:generate_kvnodes(10),
    [NoMember1, NoMember2] = vtree_test_util:generate_kvnodes(2),
    etap:ok(?MOD:member_of_nodes(lists:nth(1, Nodes1), Nodes1),
            "Node is a member (a)"),
    etap:ok(?MOD:member_of_nodes(lists:nth(5, Nodes1), Nodes1),
            "Node is a member (b)"),
    etap:ok(?MOD:member_of_nodes(lists:nth(8, Nodes1), Nodes1),
            "Node is a member (c)"),
    etap:not_ok(?MOD:member_of_nodes(NoMember1, Nodes1),
                "Node is not a member (a)"),
    etap:not_ok(?MOD:member_of_nodes(NoMember2, Nodes1),
                "Node is not a member (b)"),

    Nodes2 = vtree_test_util:generate_kvnodes(4),
    [NoMember3] = vtree_test_util:generate_kvnodes(1),
    etap:ok(?MOD:member_of_nodes(lists:nth(1, Nodes2), Nodes2),
            "Node is a member (d)"),
    etap:ok(?MOD:member_of_nodes(lists:nth(2, Nodes2), Nodes2),
            "Node is a member (e)"),
    etap:ok(?MOD:member_of_nodes(lists:nth(3, Nodes2), Nodes2),
            "Node is a member (f)"),
    etap:ok(?MOD:member_of_nodes(lists:nth(4, Nodes2), Nodes2),
            "Node is a member (g)"),
    etap:not_ok(?MOD:member_of_nodes(NoMember3, Nodes2),
                "Node is not a member (c)").


test_partition_nodes() ->
    Less = fun(A, B) -> A < B end,

    Mbb1 = [{-700, -600}, {-30, -25}],
    Mbb2 = [{-800, -500}, {-55, -10}],
    Mbb3 = [{-550, -400}, {-10, 0}],
    Mbb4 = [{-300, -200}, {0, 40}],
    Mbb5 = [{-450, -250}, {-20, 80}],

    Mbb6 = [{-640, -620}, {-30, -25}],
    Mbb7 = [{-700, -600}, {-21, -20}],
    Mbb8 = [{-550, -450}, {-5, 0}],
    Mbb9 = [{-450, -400}, {-10, 0}],
    Mbb10 = [{-450, -400}, {-10, 50}],

    KpNodes = [#kp_node{key=Mbb} || Mbb <- [Mbb3, Mbb1, Mbb2]],
    ToPartition = [#kv_node{key=Mbb} || Mbb <- [Mbb6, Mbb7]],

    Partitioned1 = ?MOD:partition_nodes(ToPartition, KpNodes, Less),
    etap:is(lists:usort(lists:append(Partitioned1)),
            lists:sort(ToPartition),
            "All nodes were partitioned (a)"),
    etap:is(Partitioned1,
            [[], [#kv_node{key=Mbb6}],
             [#kv_node{key=Mbb6}, #kv_node{key=Mbb7}]],
            "All nodes were correctly partitioned (a)"),

    KpNodes2 = [#kp_node{key=Mbb} || Mbb <- [Mbb2, Mbb3, Mbb4, Mbb5]],
    ToPartition2 = [#kv_node{key=Mbb} || Mbb <- [Mbb8, Mbb6, Mbb9, Mbb10]],

    Partitioned2 = ?MOD:partition_nodes(ToPartition2, KpNodes2, Less),
    etap:is(lists:usort(lists:append(Partitioned2)),
            lists:sort(ToPartition2),
            "All nodes were partitioned (b)"),
    etap:is(Partitioned2,
            [[#kv_node{key=Mbb6}], [#kv_node{key=Mbb8}, #kv_node{key=Mbb9}],
             [], [#kv_node{key=Mbb9}, #kv_node{key=Mbb10}]],
            "All nodes were correctly partitioned (b)").
