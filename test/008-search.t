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

-define(MOD, vtree_search).
-define(FILENAME, "/tmp/vtree_search_vtree.bin").

main(_) ->
    code:add_pathz(filename:dirname(escript:script_name())),
    etap:plan(48),
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
    test_search(),
    test_all(),
    test_count_search(),
    test_count_all(),
    test_traverse(),
    test_traverse_all(),
    test_any_box_intersects_mbb(),
    test_boxes_intersect_mbb(),
    ok.


test_search() ->
    random:seed(1, 11, 91),

    Fd = vtree_test_util:create_file(?FILENAME),
    Less = fun(A, B) -> A < B end,
    Vtree = #vtree{
      fd = Fd,
      fill_min = 4,
      fill_max = 8,
      less = Less
     },
    Boxes = [N#kv_node.key || N <- vtree_test_util:generate_kvnodes(4)],

    FoldFun = fun(Node, Acc) -> {ok, [Node|Acc]} end,

    etap:is(?MOD:search(Vtree, Boxes, FoldFun, []), [],
            "Searching empty tree works"),

    Nodes1 = vtree_test_util:generate_kvnodes(97),
    Vtree1 = vtree_insert:insert(Vtree, Nodes1),
    Result1 = ?MOD:search(Vtree1, Boxes, FoldFun, []),
    etap:ok(length(Nodes1 -- Result1) > 1,
            "Searching tree (97 items) returned a subset of the items"),

    etap:is(?MOD:search(Vtree1, [], FoldFun, []), [],
            "Searching with an empty list of boxes retrns empty result"),

    couch_file:close(Fd).


test_all() ->
    random:seed(1, 11, 91),

    Fd = vtree_test_util:create_file(?FILENAME),
    Less = fun(A, B) -> A < B end,
    Vtree = #vtree{
      fd = Fd,
      fill_min = 4,
      fill_max = 8,
      less = Less
     },
    Boxes = [N#kv_node.key || N <- vtree_test_util:generate_kvnodes(4)],

    FoldFun = fun(Node, Acc) -> {ok, [Node|Acc]} end,

    etap:is(?MOD:all(Vtree, FoldFun, []), [],
            "Return all items from empty tree works"),

    Nodes1 = vtree_test_util:generate_kvnodes(97),
    Vtree1 = vtree_insert:insert(Vtree, Nodes1),
    Result1 = ?MOD:all(Vtree1, FoldFun, []),
    etap:is(lists:sort(Result1), lists:sort(Nodes1),
            "Returning all items (97) worked"),

    Nodes2 = vtree_test_util:generate_kvnodes(3),
    Vtree2 = vtree_insert:insert(Vtree, Nodes2),
    Result2 = ?MOD:all(Vtree2, FoldFun, []),
    etap:is(lists:sort(Result2), lists:sort(Nodes2),
            "Returning all items (3) worked"),

    couch_file:close(Fd).


test_count_search() ->
    random:seed(1, 11, 91),

    Fd = vtree_test_util:create_file(?FILENAME),
    Less = fun(A, B) -> A < B end,
    Vtree = #vtree{
      fd = Fd,
      fill_min = 4,
      fill_max = 8,
      less = Less
     },

    Boxes = [N#kv_node.key || N <- vtree_test_util:generate_kvnodes(4)],
    ExpectedFun = fun(Nodes, Boxes) ->
                          length(lists:filter(
                                   fun(Node) ->
                                           ?MOD:any_box_intersects_mbb(
                                              Boxes, Node#kv_node.key, Less)
                                   end, Nodes))
                  end,

    Nodes1 = vtree_test_util:generate_kvnodes(20),
    Vtree1 = vtree_insert:insert(Vtree, Nodes1),
    Boxes1 = [hd(Boxes)],
    etap:is(?MOD:count_search(Vtree1, Boxes1), ExpectedFun(Nodes1, Boxes1),
            "Count (tree with 20 items) with one box was correct"),
    {Boxes2, _} = lists:split(2, Boxes),
    etap:is(?MOD:count_search(Vtree1, Boxes2), ExpectedFun(Nodes1, Boxes2),
            "Count (tree with 20 items) with two boxes was correct"),

    Nodes2 = vtree_test_util:generate_kvnodes(137),
    Vtree2 = vtree_insert:insert(Vtree, Nodes2),
    etap:is(?MOD:count_search(Vtree2, Boxes1), ExpectedFun(Nodes2, Boxes1),
            "Count (tree with 137 items) with one box was correct"),
    etap:is(?MOD:count_search(Vtree2, Boxes), ExpectedFun(Nodes2, Boxes),
            "Count (tree with 137 items) with four boxes was correct"),

    Nodes3 = vtree_test_util:generate_kvnodes(4),
    Vtree3 = vtree_insert:insert(Vtree, Nodes3),
    etap:is(?MOD:count_search(Vtree3, Boxes1), ExpectedFun(Nodes3, Boxes1),
            "Count (tree with 4 items) with one box was correct"),
    etap:is(?MOD:count_search(Vtree3, Boxes), ExpectedFun(Nodes3, Boxes),
            "Count (tree with 4 items) with four boxes was correct"),

    etap:is(?MOD:count_search(Vtree, Boxes1), 0,
            "Count (empty tree) with one box was correct"),
    etap:is(?MOD:count_search(Vtree, Boxes), 0,
            "Count (empty tree) with four boxes was correct"),

    couch_file:close(Fd).


test_count_all() ->
    random:seed(1, 11, 91),

    Fd = vtree_test_util:create_file(?FILENAME),
    Less = fun(A, B) -> A < B end,
    Vtree = #vtree{
      fd = Fd,
      fill_min = 4,
      fill_max = 8,
      less = Less
     },

    etap:is(?MOD:count_all(Vtree), 0,
            "Return all items from empty tree works"),

    Nodes1 = vtree_test_util:generate_kvnodes(97),
    Vtree1 = vtree_insert:insert(Vtree, Nodes1),
    etap:is(?MOD:count_all(Vtree1), 97, "Returning all items (97) worked"),

    Nodes2 = vtree_test_util:generate_kvnodes(3),
    Vtree2 = vtree_insert:insert(Vtree, Nodes2),
    etap:is(?MOD:count_all(Vtree2), 3, "Returning all items (3) worked"),

    couch_file:close(Fd).


test_traverse() ->
    random:seed(1, 11, 91),

    Fd = vtree_test_util:create_file(?FILENAME),
    Less = fun(A, B) -> A < B end,
    Vtree = #vtree{
      fd = Fd,
      fill_min = 4,
      fill_max = 8,
      less = Less
     },

    FoldFun = fun(Node, Acc) -> {ok, [Node|Acc]} end,

    Boxes = [N#kv_node.key || N <- vtree_test_util:generate_kvnodes(4)],
    ExpectedFun = fun(Nodes, Boxes) ->
                           lists:filter(
                             fun(Node) ->
                                     ?MOD:any_box_intersects_mbb(
                                        Boxes, Node#kv_node.key, Less)
                             end, Nodes)
                   end,

    Nodes1 = vtree_test_util:generate_kvnodes(20),
    Vtree1 = vtree_insert:insert(Vtree, Nodes1),
    Boxes1 = [hd(Boxes)],
    Expected1 = ExpectedFun(Nodes1, Boxes1),
    {ok, Result1} = ?MOD:traverse(
                       Vtree1, [Vtree1#vtree.root], Boxes1, FoldFun, {ok, []}),
    etap:is(lists:sort(Result1), lists:sort(Expected1),
            "Traversing tree (20 items) with one box was correct"),

    {Boxes2, _} = lists:split(2, Boxes),
    Expected2 = ExpectedFun(Nodes1, Boxes2),
    {ok, Result2} = ?MOD:traverse(
                       Vtree1, [Vtree1#vtree.root], Boxes2, FoldFun, {ok, []}),
    etap:is(lists:sort(Result2), lists:sort(Expected2),
            "Traversing tree (20 items) with two boxes was correct"),


    Nodes2 = vtree_test_util:generate_kvnodes(137),
    Vtree2 = vtree_insert:insert(Vtree, Nodes2),
    Expected3 = ExpectedFun(Nodes2, Boxes1),
    {ok, Result3} = ?MOD:traverse(
                       Vtree2, [Vtree2#vtree.root], Boxes1, FoldFun, {ok, []}),
    etap:is(lists:sort(Result3), lists:sort(Expected3),
            "Traversing tree (137 items) with one box was correct"),

    Expected4 = ExpectedFun(Nodes2, Boxes),
    {ok, Result4} = ?MOD:traverse(
                       Vtree2, [Vtree2#vtree.root], Boxes, FoldFun, {ok, []}),
    etap:is(lists:sort(Result4), lists:sort(Expected4),
            "Traversing tree (137 items) with four boxes was correct"),

    Nodes3 = vtree_test_util:generate_kvnodes(4),
    Vtree3 = vtree_insert:insert(Vtree, Nodes3),
    Expected5 = ExpectedFun(Nodes3, Boxes1),
    {ok, Result5} = ?MOD:traverse(
                       Vtree3, [Vtree3#vtree.root], Boxes1, FoldFun, {ok, []}),
    etap:is(lists:sort(Result5), lists:sort(Expected5),
            "Traversing tree (4 items) with one box was correct"),

    Expected6 = ExpectedFun(Nodes3, Boxes),
    {ok, Result6} = ?MOD:traverse(
                       Vtree3, [Vtree3#vtree.root], Boxes, FoldFun, {ok, []}),
    etap:is(lists:sort(Result6), lists:sort(Expected6),
            "Traversing tree (4 items) with four boxes was correct"),

    couch_file:close(Fd).


test_traverse_all() ->
    random:seed(1, 11, 91),

    Fd = vtree_test_util:create_file(?FILENAME),
    Less = fun(A, B) -> A < B end,
    Vtree = #vtree{
      fd = Fd,
      fill_min = 4,
      fill_max = 8,
      less = Less
     },

    FoldFun = fun(Node, Acc) -> {ok, [Node|Acc]} end,

    Nodes1 = vtree_test_util:generate_kvnodes(20),
    Vtree1 = vtree_insert:insert(Vtree, Nodes1),
    {ok, Result1} = ?MOD:traverse_all(
                       Vtree1, [Vtree1#vtree.root], FoldFun, {ok, []}),
    etap:is(lists:sort(Result1), lists:sort(Nodes1),
            "Traversing tree (20 items) was correct"),

    Nodes2 = vtree_test_util:generate_kvnodes(137),
    Vtree2 = vtree_insert:insert(Vtree, Nodes2),
    {ok, Result3} = ?MOD:traverse_all(
                       Vtree2, [Vtree2#vtree.root], FoldFun, {ok, []}),
    etap:is(lists:sort(Result3), lists:sort(Nodes2),
            "Traversing tree (137 items) was correct"),

    Nodes3 = vtree_test_util:generate_kvnodes(4),
    Vtree3 = vtree_insert:insert(Vtree, Nodes3),
    {ok, Result5} = ?MOD:traverse_all(
                       Vtree3, [Vtree3#vtree.root], FoldFun, {ok, []}),
    etap:is(lists:sort(Result5), lists:sort(Nodes3),
            "Traversing tree (4 items) with one box was correct"),

    couch_file:close(Fd).


test_any_box_intersects_mbb() ->
    Less = fun(A, B) -> A < B end,

    Mbb1 = [{-38, 74.2}, {38, 948}],
    Mbb2 = [{-480, 5}, {-7, 428.74}],
    Mbb3 = [{84.3, 923.8}, {39, 938}],
    Mbb4 = [{-937, 8424}, {-1000, -82}],
    Mbb5 = [{-100.72, -5}, {-2, 39.3}],
    Mbb6 = [{222, 222}, {-432.39, -294.20}],

    etap:is(?MOD:any_box_intersects_mbb([Mbb1], Mbb2, Less), true,
            "One box intersects MBB"),
    etap:is(?MOD:any_box_intersects_mbb([Mbb3], Mbb2, Less), false,
            "One box doesn't intersect MBB"),
    etap:is(?MOD:any_box_intersects_mbb([Mbb1, Mbb3], Mbb2, Less), true,
            "First of two boxes intersects MBB"),
    etap:is(?MOD:any_box_intersects_mbb([Mbb3, Mbb1], Mbb2, Less), true,
            "Second of two boxes intersects MBB"),
    etap:is(?MOD:any_box_intersects_mbb([Mbb4, Mbb3], Mbb2, Less), false,
            "None of two boxes intersects MBB"),
    etap:is(?MOD:any_box_intersects_mbb([Mbb1, Mbb5], Mbb2, Less), true,
            "Two of two boxes intersects MBB"),

    etap:is(?MOD:any_box_intersects_mbb([Mbb2, Mbb3, Mbb4], Mbb1, Less), true,
            "First of three boxes intersects MBB"),
    etap:is(?MOD:any_box_intersects_mbb([Mbb3, Mbb2, Mbb4], Mbb1, Less), true,
            "Second of three boxes intersects MBB"),
    etap:is(?MOD:any_box_intersects_mbb([Mbb3, Mbb4, Mbb2], Mbb1, Less), true,
            "Third of three boxes intersects MBB"),
    etap:is(?MOD:any_box_intersects_mbb([Mbb2, Mbb4, Mbb5], Mbb1, Less), true,
            "First and last of three boxes intersects MBB"),
    etap:is(?MOD:any_box_intersects_mbb([Mbb3, Mbb4, Mbb6], Mbb1, Less), false,
            "None of three boxes intersects MBB").


test_boxes_intersect_mbb() ->
    Less = fun(A, B) -> A < B end,

    Mbb1 = [{-38, 74.2}, {38, 948}],
    Mbb2 = [{-480, 5}, {-7, 428.74}],
    Mbb3 = [{84.3, 923.8}, {39, 938}],
    Mbb4 = [{-937, 8424}, {-1000, -82}],
    Mbb5 = [{-100.72, -5}, {-2, 39.3}],
    Mbb6 = [{222, 222}, {-432.39, -294.20}],

    etap:is(?MOD:boxes_intersect_mbb([Mbb1], Mbb2, Less), [Mbb1],
            "One box intersects MBB"),
    etap:is(?MOD:boxes_intersect_mbb([Mbb3], Mbb2, Less), [],
            "One box doesn't intersect MBB"),
    etap:is(?MOD:boxes_intersect_mbb([Mbb1, Mbb3], Mbb2, Less), [Mbb1],
            "First of two boxes intersects MBB"),
    etap:is(?MOD:boxes_intersect_mbb([Mbb3, Mbb1], Mbb2, Less), [Mbb1],
            "Second of two boxes intersects MBB"),
    etap:is(?MOD:boxes_intersect_mbb([Mbb4, Mbb3], Mbb2, Less), [],
            "None of two boxes intersects MBB"),
    etap:is(?MOD:boxes_intersect_mbb([Mbb1, Mbb5], Mbb2, Less), [Mbb1, Mbb5],
            "Two of two boxes intersects MBB"),

    etap:is(?MOD:boxes_intersect_mbb([Mbb2, Mbb3, Mbb4], Mbb1, Less), [Mbb2],
            "First of three boxes intersects MBB"),
    etap:is(?MOD:boxes_intersect_mbb([Mbb3, Mbb2, Mbb4], Mbb1, Less), [Mbb2],
            "Second of three boxes intersects MBB"),
    etap:is(?MOD:boxes_intersect_mbb([Mbb3, Mbb4, Mbb2], Mbb1, Less), [Mbb2],
            "Third of three boxes intersects MBB"),
    etap:is(?MOD:boxes_intersect_mbb([Mbb2, Mbb4, Mbb5], Mbb1, Less),
            [Mbb2, Mbb5],
            "First and last of three boxes intersects MBB"),
    etap:is(?MOD:boxes_intersect_mbb([Mbb3, Mbb4, Mbb6], Mbb1, Less), [],
            "None of three boxes intersects MBB").
