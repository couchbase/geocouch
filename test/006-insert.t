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

-define(MOD, vtree_insert).
-define(FILENAME, "/tmp/vtree_insert_vtree.bin").

main(_) ->
    % Set the random seed once. It might be reset a certain tests
    random:seed(1, 11, 91),

    code:add_pathz(filename:dirname(escript:script_name())),
    etap:plan(35),
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
    test_insert(),
    test_insert_in_bulks(),
    test_insert_multiple(),
    test_insert_nodes(),
    test_partition_nodes(),
    test_add_to_nth(),
    ok.


test_insert() ->
    Fd = vtree_test_util:create_file(?FILENAME),
    Nodes1 = vtree_test_util:generate_kvnodes(6),

    Vtree1 = #vtree{
      fd = Fd,
      fill_min = 4,
      fill_max = 8,
      less = fun(A, B) -> A < B end
     },

    NewVtree1 = ?MOD:insert(Vtree1, [hd(Nodes1)]),
    Root1 = NewVtree1#vtree.root,
    [InsertedNodes1] = vtree_io:read_node(Fd, Root1#kp_node.childpointer),
    etap:is(InsertedNodes1, hd(Nodes1),
            "Inserted single node into emtpy tree: node got inserted"),

    NewVtree2 = ?MOD:insert(Vtree1, Nodes1),
    Root2 = NewVtree2#vtree.root,
    InsertedNodes2 = vtree_io:read_node(Fd, Root2#kp_node.childpointer),
    etap:is(InsertedNodes2, Nodes1,
            "Inserted into 6 nodes into emtpy tree: all nodes got inserted"),

    Nodes3 = vtree_test_util:generate_kvnodes(500),
    NewVtree3 = ?MOD:insert(Vtree1, Nodes3),
    Root3 = NewVtree3#vtree.root,
    {Depths3, KvNodes3} = vtree_test_util:get_kvnodes(
                            Fd, Root3#kp_node.childpointer),
    etap:is(sets:size(sets:from_list(Depths3)), 1,
            "Inserted 500 nodes into emtpy tree: tree is balanced"),
    etap:is(lists:sort(KvNodes3), lists:sort(Nodes3),
            "Inserted 500 nodes into emtpy tree: all nodes got inserted"),

    Nodes4 = vtree_test_util:generate_kvnodes(223),
    NewVtree4 = ?MOD:insert(NewVtree3, Nodes4),
    Root4 = NewVtree4#vtree.root,
    {Depths4, KvNodes4} = vtree_test_util:get_kvnodes(
                            Fd, Root4#kp_node.childpointer),
    etap:is(sets:size(sets:from_list(Depths4)), 1,
            "Inserted 223 nodes into existing tree: tree is balanced"),
    etap:is(lists:sort(KvNodes4), lists:sort(Nodes3 ++ Nodes4),
            "Inserted 223 nodes into existing tree: all nodes got inserted"),

    Nodes5 = vtree_test_util:generate_kvnodes(1),
    NewVtree5 = ?MOD:insert(NewVtree1, Nodes5),
    Root5 = NewVtree5#vtree.root,
    {Depths5, KvNodes5} = vtree_test_util:get_kvnodes(
                            Fd, Root5#kp_node.childpointer),
    etap:is(sets:size(sets:from_list(Depths5)), 1,
            "Inserted single node into existing single node tree: tree is "
            "balanced. Root has < fill_min nodes"),
    etap:is(lists:sort(KvNodes5), lists:sort([hd(Nodes1)|Nodes5]),
            "Inserted single node into existing tree: node got inserted. "
            "Root has < fill_min nodes"),

    etap:is(?MOD:insert(NewVtree2, []), NewVtree2,
            "Not adding any nodes returns the original tree"),

    couch_file:close(Fd).


test_insert_in_bulks()->
    Less = fun(A, B) -> A < B end,
    Fd = vtree_test_util:create_file(?FILENAME),

    Vtree1 = #vtree{
      fd = Fd,
      fill_min = 2,
      fill_max = 4,
      less = Less
     },

    Nodes1 = vtree_test_util:generate_kvnodes(20),
    Nodes2 = vtree_test_util:generate_kvnodes(100),
    Nodes3 = vtree_test_util:generate_kvnodes(50),

    #vtree{root=Root1} = ?MOD:insert_in_bulks(Vtree1, Nodes1, 7),
    {Depths1, KvNodes1} = vtree_test_util:get_kvnodes(
                            Fd, Root1#kp_node.childpointer),
    etap:is(sets:size(sets:from_list(Depths1)), 1,
            "Tree is balanced (originally empty) (a)"),
    etap:is(lists:sort(KvNodes1), lists:sort(Nodes1),
            "All nodes were inserted (originally empty) (a)"),

    #vtree{root=Root2} = ?MOD:insert_in_bulks(Vtree1, Nodes2, 9),
    {Depths2, KvNodes2} = vtree_test_util:get_kvnodes(
                            Fd, Root2#kp_node.childpointer),
    etap:is(sets:size(sets:from_list(Depths2)), 1,
            "Tree is balanced (originally empty) (b)"),
    etap:is(lists:sort(KvNodes2), lists:sort(Nodes2),
            "All nodes were inserted (originally empty) (b)"),

    #vtree{root=Root3} = ?MOD:insert_in_bulks(Vtree1#vtree{root=Root1},
                                              Nodes3, 10),
    {Depths3, KvNodes3} = vtree_test_util:get_kvnodes(
                            Fd, Root3#kp_node.childpointer),
    etap:is(sets:size(sets:from_list(Depths3)), 1,
            "Tree is balanced (originally not empty)"),
    etap:is(lists:sort(KvNodes3), lists:sort(Nodes1 ++ Nodes3),
            "All nodes were inserted (originally not empty)"),

    couch_file:close(Fd).


test_insert_multiple() ->
    Less = fun(A, B) -> A < B end,
    Fd = vtree_test_util:create_file(?FILENAME),

    Vtree1 = #vtree{
      fd = Fd,
      fill_min = 2,
      fill_max = 4,
      less = Less
     },

    Nodes1 = vtree_test_util:generate_kvnodes(20),
    Nodes2 = vtree_test_util:generate_kvnodes(10),
    Nodes3 = vtree_test_util:generate_kvnodes(100),
    Vtree2 = #vtree{root=Root} = ?MOD:insert(Vtree1, Nodes1),

    PartitionedNodes1 = ?MOD:partition_nodes(Nodes2, [Root], Less),
    [KpNode1] = ?MOD:insert_multiple(Vtree2, PartitionedNodes1, [Root]),
    {Depths1, KvNodes1} = vtree_test_util:get_kvnodes(
                            Fd, KpNode1#kp_node.childpointer),
    etap:is(sets:size(sets:from_list(Depths1)), 1, "Tree is balanced (a)"),
    etap:is(lists:sort(KvNodes1), lists:sort(Nodes1 ++ Nodes2),
            "All nodes were inserted (a)"),

    PartitionedNodes2 = ?MOD:partition_nodes(Nodes3, [Root], Less),
    KpNodes2 = ?MOD:insert_multiple(Vtree2, PartitionedNodes2, [Root]),
    KpNode2 = vtree_modify:write_new_root(Vtree2, KpNodes2),
    {Depths2, KvNodes2} = vtree_test_util:get_kvnodes(
                            Fd, KpNode2#kp_node.childpointer),
    etap:is(sets:size(sets:from_list(Depths2)), 1, "Tree is balanced (b)"),
    etap:is(lists:sort(KvNodes2), lists:sort(Nodes1 ++ Nodes3),
            "All nodes were inserted (b)"),

    couch_file:close(Fd).


test_insert_nodes() ->
    etap:is(lists:sort(?MOD:insert_nodes([d], [a, b, c])), [a, b, c ,d],
            "Single item got inserted"),
    etap:is(lists:sort(?MOD:insert_nodes([d, e], [a, b, c])), [a, b, c, d, e],
            "Two items got inserted"),
    etap:is(lists:sort(?MOD:insert_nodes([d, e], [])), [d, e],
            "Two items got inserted into empty list").


test_partition_nodes() ->
    Less = fun(A, B) -> A < B end,

    Mbb1 = [{-700, -600}, {-30, -20}],
    Mbb2 = [{-600, -500}, {-25, -10}],
    Mbb3 = [{-550, -400}, {-10, 0}],
    Mbb4 = [{-300, -200}, {0, 40}],
    Mbb5 = [{-200, -150}, {30, 80}],

    Mbb6 = [{-740, -720}, {-30, -25}],
    Mbb7 = [{-600, -500}, {-20, -15}],
    Mbb8 = [{-550, -450}, {-25, 10}],
    Mbb9 = [{-150, -120}, {50, 60}],

    Mbb10 = [{300, 500}, {500, 600}],
    Mbb11 = [{-1000, 0}, {-50, 50}],

    Mbb12 = [{5000, 9000}, {7000, 9000}],
    Mbb13 = [{0, 1}, {100, 101}],
    Mbb14 = [{-5000, -9000}, {-7000, -9000}],


    KpNodes = [#kp_node{key=Mbb} || Mbb <- [Mbb1, Mbb2, Mbb3, Mbb4, Mbb5]],
    ToPartition = [#kv_node{key=Mbb} || Mbb <- [Mbb7, Mbb9, Mbb8, Mbb6]],

    Partitioned1 = ?MOD:partition_nodes(ToPartition, KpNodes, Less),
    etap:is(lists:sort(lists:append(Partitioned1)),
            lists:sort(ToPartition),
            "All nodes were partitioned"),
    etap:is(Partitioned1,
            [[#kv_node{key=Mbb6}], [#kv_node{key=Mbb8}, #kv_node{key=Mbb7}],
             [], [], [#kv_node{key=Mbb9}]],
            "All nodes were correctly partitioned (a)"),
    etap:is(?MOD:partition_nodes([#kv_node{key=Mbb8}], KpNodes, Less),
            [[], [#kv_node{key=Mbb8}], [], [], []],
            "One node was correctly partitioned"),

    KpNodes2 = [#kp_node{key=Mbb} || Mbb <- [Mbb10, Mbb11]],
    [H, Partitioned2] = ?MOD:partition_nodes(ToPartition, KpNodes2, Less),
    etap:is(H, [], "All nodes were correctly partitioned (b1)"),
    etap:is(lists:sort(Partitioned2), lists:sort(ToPartition),
            "All nodes were correctly partitioned (b2)"),

    KpNodes3 = [#kp_node{key=Mbb} || Mbb <- [Mbb12, Mbb13, Mbb14]],
    Partitioned3 = ?MOD:partition_nodes(ToPartition, KpNodes3, Less),
    etap:is(lists:nth(1, Partitioned3), [],
            "All nodes were correctly partitioned (c1)"),
    etap:is(lists:sort(lists:nth(2, Partitioned3)), lists:sort(ToPartition),
            "All nodes were correctly partitioned (c2)"),
    etap:is(lists:nth(1, Partitioned3), [],
            "All nodes were correctly partitioned (c3)").


test_add_to_nth() ->
    ListOfLists = [[a, b], [c, d, e], [f, g], [h]],
    etap:is(?MOD:add_to_nth(3, z, ListOfLists),
            [[a, b], [c, d, e], [z, f, g], [h]],
            "Element was added succesfully (a)"),
    etap:is(?MOD:add_to_nth(1, y, ListOfLists),
            [[y, a, b], [c, d, e], [f, g], [h]],
            "Element was added succesfully (b)"),
    etap:is(?MOD:add_to_nth(2, x, ListOfLists),
            [[a, b], [x, c, d, e], [f, g], [h]],
            "Element was added succesfully (c)"),
    etap:is(?MOD:add_to_nth(4, w, ListOfLists),
            [[a, b], [c, d, e], [f, g], [w, h]],
            "Element was added succesfully (d)"),
    try
        ?MOD:add_to_nth(10, w, ListOfLists)
    catch
        error:badarg ->
            etap:ok(true, "Error was thrown")
    end.
