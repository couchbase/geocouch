#!/usr/bin/env escript
%% -*- erlang -*-

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

-define(MOD, vtree_split).

main(_) ->
    code:add_pathz(filename:dirname(escript:script_name())),
    etap:plan(95),
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
    test_split_inner(),
    test_split_leaf(),
    test_split_axis(),
    test_choose_candidate(),
    test_goal_fun(),
    test_wg(),
    test_wg_overlapfree(),
    test_make_weighting_fun(),
    test_sort_dim(),
    test_perim_max(),
    test_create_split_candidates(),
    test_nodes_perimeter(),
    test_candidates_perimeter(),
    test_min_perim(),
    test_asym(),
    test_mbb_dim_length(),
    test_mbb_dim_center(),
    ok.

test_split_inner() ->
    Less = fun(A, B) -> A < B end,

    Mbb1 = [{-380, -74.2}, {-380, 948}],
    Mbb2 = [{-480, 5}, {-7, 28.74}],
    Mbb3 = [{84.3, 923.8}, {39, 938}],
    Mbb4 = [{-937, 8424}, {-1000, -82}],
    Mbb5 = [{4.72, 593}, {372, 490.3}],
    Mbb6 = [{48, 472}, {-9.38, 26.1}],
    Node1 = {Mbb1, a},
    Node2 = {Mbb2, b},
    Node3 = {Mbb3, c},
    Node4 = {Mbb4, d},
    Node5 = {Mbb5, e},
    Node6 = {Mbb6, f},
    Nodes = [Node1, Node2, Node3, Node4, Node5, Node6],

    Max = ?ext_size([a,b,c,d]),
    {A, B} = ?MOD:split_inner(Nodes, Mbb1, 0.2*Max, Max, Less),
    etap:is(lists:sort(A ++ B), lists:sort(Nodes),
            "One candidate was choosen (inner node)"),
    etap:isnt(?MOD:split_inner(Nodes, Mbb1, 0, 10000, Less), {A, B},
              "The candidate wasn't found with relaxed fill rate conditions").


test_split_leaf() ->
    Less = fun(A, B) -> A < B end,

    Mbb1 = [{-380, -74.2}, {-380, 948}],
    Mbb2 = [{-480, 5}, {-7, 28.74}],
    Mbb3 = [{84.3, 923.8}, {39, 938}],
    Mbb4 = [{-937, 8424}, {-1000, -82}],
    Mbb5 = [{4.72, 593}, {372, 490.3}],
    Mbb6 = [{48, 472}, {-9.38, 26.1}],
    Node1 = {Mbb1, a},
    Node2 = {Mbb2, b},
    Node3 = {Mbb3, c},
    Node4 = {Mbb4, d},
    Node5 = {Mbb5, e},
    Node6 = {Mbb6, f},
    Nodes = [Node1, Node2, Node3, Node4, Node5, Node6],

    Max = ?ext_size([a,b,c,d]),
    {A, B} = ?MOD:split_leaf(Nodes, Mbb1, 0.2*Max, Max, Less),
    etap:is(lists:sort(A ++ B), lists:sort(Nodes),
            "One candidate was choosen (leaf node)"),
    etap:isnt(?MOD:split_leaf(Nodes, Mbb1, 0, 10000, Less), {A, B},
              "The candidate wasn't found with relaxed fill rate conditions").


test_split_axis() ->
    Less = fun(A, B) -> A < B end,

    Mbb1 = [{-38, 74.2}, {38, 948}, {-480, -27}, {-7, -4.28}, {84.3, 923.8}],
    Mbb2 = [{39, 938}, {-937, 8424}, {-1000, -82}, {4.72, 593}, {372, 490.3}],
    Mbb3 = [{48, 472}, {-9.38, 26.1}, {-382, -29}, {-1.4, 30}, {39.9, 100}],
    Node1 = {Mbb1, 3487},
    Node2 = {Mbb2, 823},
    Node3 = {Mbb3, 96242},

    Max = ?ext_size([3487, 823]),

    etap:is(?MOD:split_axis([Node1, Node2, Node3], 0.5*Max, Max, Less),
            {5, [{[
                   {[{-38,74.2},{38,948},{-480,-27},{-7,-4.28},{84.3,923.8}],
                    3487}],
                  [
                   {[{39,938},{-937,8424},{-1000,-82},{4.72,593},{372,490.3}],
                    823},
                   {[{48,472},{-9.38,26.1},{-382,-29},{-1.4,30},{39.9,100}],
                    96242}
                  ]},
                 {[
                   {[{-38,74.2},{38,948},{-480,-27},{-7,-4.28},{84.3,923.8}],
                    3487},
                   {[{39,938},{-937,8424},{-1000,-82},{4.72,593},{372,490.3}],
                    823}],
                  [
                   {[{48,472},{-9.38,26.1},{-382,-29},{-1.4,30},{39.9,100}],
                    96242}]}]},
            "Calculate the split axis").


test_choose_candidate() ->
    Less = fun(A, B) -> A < B end,

    Mbb1 = [{-380, -74.2}, {-380, 948}],
    Mbb2 = [{-480, 5}, {-7, 28.74}],
    Mbb3 = [{84.3, 923.8}, {39, 938}],
    Mbb4 = [{-937, 8424}, {-1000, -82}],
    Mbb5 = [{4.72, 593}, {372, 490.3}],
    Mbb6 = [{48, 472}, {-9.38, 26.1}],
    Mbb7 = [{222, 222}, {462, 781}],
    Mbb8 = [{222, 222}, {583, 953}],
    Mbb9 = [{222, 222}, {184, 483}],
    Node1 = {Mbb1, a},
    Node2 = {Mbb2, b},
    Node3 = {Mbb3, c},
    Node4 = {Mbb4, d},
    Node5 = {Mbb5, e},
    Node6 = {Mbb6, f},
    Node7 = {Mbb7, g},
    Node8 = {Mbb8, h},
    Node9 = {Mbb9, i},

    Nodes1 = [Node1, Node2, Node3, Node4, Node5, Node6],
    Max1 = ?ext_size([a,b,c,d,e]),
    Candidates1 = ?MOD:create_split_candidates(Nodes1, 0.2*Max1, Max1),
    MbbN1 = vtree_util:calc_mbb([Mbb1, Mbb2, Mbb3, Mbb4, Mbb5, Mbb6], Less),
    {_, Best1} = ?MOD:choose_candidate(Candidates1, 1, Mbb1, MbbN1, 0.2*Max1,
                                       Less),
    etap:is(Best1, lists:nth(5, Candidates1), "Best candidate (6 nodes)"),

    Nodes2 = [Node3, Node4, Node5, Node6, Node2],
    Max2 = ?ext_size([c,d,e,f]),
    Candidates2 = ?MOD:create_split_candidates(Nodes2, 0.5*Max2, Max2),
    MbbN2 = vtree_util:calc_mbb([Mbb3, Mbb4, Mbb5, Mbb6, Mbb2], Less),
    {_, Best2} = ?MOD:choose_candidate(Candidates2, 2, Mbb3, MbbN2, 0.5*Max2,
                                       Less),
    etap:is(Best2, lists:nth(2, Candidates2), "Best candidate (5 nodes)"),

    Nodes3 = [Node3, Node5],
    Max3 = ?ext_size([c]),
    Candidates3 = ?MOD:create_split_candidates(Nodes3, Max3, Max3),
    MbbN3 = vtree_util:calc_mbb([Mbb3, Mbb5], Less),
    {_, Best3} = ?MOD:choose_candidate(Candidates3, 1, Mbb3, MbbN3, Max3,
                                       Less),
    etap:is(Best3, lists:nth(1, Candidates3),
            "Best candidate (single partition)"),

    Nodes4 = [Node4, Node3],
    Max4 = ?ext_size([d]),
    Candidates4 = ?MOD:create_split_candidates(Nodes4, Max4, Max4),
    MbbN4 = vtree_util:calc_mbb([Mbb3, Mbb4], Less),
    {_, Best4} = ?MOD:choose_candidate(Candidates4, 1, Mbb3, MbbN4, Max4,
                                       Less),
    etap:is(Best4, lists:nth(1, Candidates4),
            "Best candidate (single partition, overlap-free)"),

    Nodes5 = [Node8, Node9, Node7],
    Max5 = ?ext_size([h,i]),
    Candidates5 = ?MOD:create_split_candidates(Nodes5, 0.5*Max5, Max5),
    MbbN5 = vtree_util:calc_mbb([Mbb7, Mbb8, Mbb9], Less),
    {_, Best5} = ?MOD:choose_candidate(Candidates5, 2, Mbb7, MbbN5, 0.5*Max5,
                                       Less),
    etap:is(Best5, lists:nth(1, Candidates5), "Best candidate (0 volume)").


test_goal_fun() ->
    Less = fun(A, B) -> A < B end,

    PerimMax = 0.436,
    Mbb1 = [{-380, -74.2}, {-380, 948}],
    Mbb2 = [{-480, 5}, {-7, 28.74}],
    Mbb3 = [{84.3, 923.8}, {39, 938}],
    Mbb4 = [{-937, 8424}, {-1000, -82}],
    Mbb5 = [{48, 472}, {-9.38, 26.1}],

    Mbb6 = [{-200, -180}, {40, 50}],
    Mbb7 = [{-150, -120}, {40, 50}],
    Mbb8 = [{-100, -80}, {40, 50}],
    Mbb9 = [{-50, -20}, {40, 50}],
    Mbb10 = [{0, 20}, {40, 50}],

    Node1 = {Mbb1, a},
    Node2 = {Mbb2, b},
    Node3 = {Mbb3, c},
    Node4 = {Mbb4, d},
    Node5 = {Mbb5, e},
    Node6 = {Mbb6, f},
    Node7 = {Mbb7, g},
    Node8 = {Mbb8, h},
    Node9 = {Mbb9, i},
    Node10 = {Mbb10, j},

    Nodes1 = [Node1, Node2, Node3, Node4, Node5],
    Max1 = ?ext_size([N || {_, N} <- tl(Nodes1)]),
    Nodes1Size = Max1 + ?ext_size(e),
    Wf1 = ?MOD:make_weighting_fun(
             0.7, 0.25*Max1, Nodes1Size),
    Candidates1 = ?MOD:create_split_candidates(Nodes1, 0.25*Max1, Max1),
    etap:is(?MOD:goal_fun(lists:nth(1, Candidates1), PerimMax, Wf1, Less),
            906221.1750409077,
            "Goal function for non overlap-free case (a)"),
    etap:is(?MOD:goal_fun(lists:nth(2, Candidates1), PerimMax, Wf1, Less),
            811108.0550765122,
            "Goal function for non overlap-free case (b)"),
    etap:is(?MOD:goal_fun(lists:nth(3, Candidates1), PerimMax, Wf1, Less),
            573662.342991384,
            "Goal function for non overlap-free case (c)"),
    etap:is(?MOD:goal_fun(lists:nth(4, Candidates1), PerimMax, Wf1, Less),
            16767.296138228474,
            "Goal function for non overlap-free case (d)"),

    Nodes2 = [Node6, Node7, Node8, Node9, Node10],
    Max2 = ?ext_size([N || {_, N} <- tl(Nodes2)]),
    Nodes2Size = Max2 + ?ext_size(j),
    Wf2 = ?MOD:make_weighting_fun(
             0.7, 0.25*Max2, Nodes2Size),
    Candidates2 = ?MOD:create_split_candidates(Nodes2, 0.25*Max2, Max2),
    etap:is(?MOD:goal_fun(lists:nth(1, Candidates2), PerimMax, Wf2, Less),
            93.20417461861585,
            "Goal function for overlap-free case (a)"),
    etap:is(?MOD:goal_fun(lists:nth(2, Candidates2), PerimMax, Wf2, Less),
            173.03723571917993,
            "Goal function for overlap-free case (b)"),
    etap:is(?MOD:goal_fun(lists:nth(3, Candidates2), PerimMax, Wf2, Less),
            208.25649965194654,
            "Goal function for overlap-free case (c)").


test_wg() ->
    Less = fun(A, B) -> A < B end,

    Mbb1 = [{-380, -74.2}, {-380, 948}],
    Mbb2 = [{-480, 5}, {-7, 28.74}],
    Mbb3 = [{84.3, 923.8}, {39, 938}],
    Mbb4 = [{-937, 8424}, {-1000, -82}],
    Mbb5 = [{48, 472}, {-9.38, 26.1}],
    % This MBB has no volume
    Mbb6 = [{48, 472}, {-5, -5}],
    Node1 = {Mbb1, a},
    Node2 = {Mbb2, b},
    Node3 = {Mbb3, c},
    Node4 = {Mbb4, d},
    Node5 = {Mbb5, e},
    Node6 = {Mbb6, f},


    Nodes1 = [Node1, Node2, Node3, Node4, Node5],
    Max1 = ?ext_size([a,b,c,d]),
    Candidates1 = ?MOD:create_split_candidates(Nodes1, 0.25*Max1, Max1),
    etap:is(?MOD:wg(lists:nth(1, Candidates1), Less),
            403044.4,
            "Original weighting function for non overlap-free case (a)"),
    etap:is(?MOD:wg(lists:nth(2, Candidates1), Less),
            639230,
            "Original weighting function for non overlap-free case (b)"),
    etap:is(?MOD:wg(lists:nth(3, Candidates1), Less),
            570083.18,
            "Original weighting function for non overlap-free case (c)"),
    etap:is(?MOD:wg(lists:nth(4, Candidates1), Less),
            15043.520000000002,
            "Original weighting function for non overlap-free case (d)"),

    Nodes2 = [Node1, Node2, Node3, Node4, Node6],
    Max2 = ?ext_size([a,b,c,d]),
    Candidates2 = ?MOD:create_split_candidates(Nodes2, 0.1*Max2, Max2),
    etap:is(?MOD:wg(lists:nth(4, Candidates2), Less),
            424,
            "Original weighting function for non overlap-free case "
            "with zero volume case").


test_wg_overlapfree() ->
    Less = fun(A, B) -> A < B end,

    PerimMax = 0.436,
    Mbb1 = [{-380, -74.2}, {-380, 948}],
    Mbb2 = [{-480, 5}, {-7, 28.74}],
    Mbb3 = [{84.3, 923.8}, {39, 938}],
    Mbb4 = [{-937, 8424}, {-1000, -82}],
    Mbb5 = [{4.72, 593}, {372, 490.3}],
    Mbb6 = [{48, 472}, {-9.38, 26.1}],
    Node1 = {Mbb1, a},
    Node2 = {Mbb2, b},
    Node3 = {Mbb3, c},
    Node4 = {Mbb4, d},
    Node5 = {Mbb5, e},
    Node6 = {Mbb6, f},

    Nodes = [Node1, Node2, Node3, Node4, Node5, Node6],
    Max = ?ext_size([a,b,c,d,e]),
    Candidates = ?MOD:create_split_candidates(Nodes, 0.1*Max, Max),
    etap:is(?MOD:wg_overlapfree(lists:nth(1, Candidates), PerimMax, Less),
            12932.364,
            "Original weighting function for overlap-free case (a)"),
    etap:is(?MOD:wg_overlapfree(lists:nth(2, Candidates), PerimMax, Less),
            13111.564,
            "Original weighting function for overlap-free case (b)"),
    etap:is(?MOD:wg_overlapfree(lists:nth(3, Candidates), PerimMax, Less),
            13582.663999999999,
            "Original weighting function for overlap-free case (c)"),
    etap:is(?MOD:wg_overlapfree(lists:nth(4, Candidates), PerimMax, Less),
            12396.524,
            "Original weighting function for overlap-free case (d)"),
    etap:is(?MOD:wg_overlapfree(lists:nth(5, Candidates), PerimMax, Less),
            11768.044,
            "Original weighting function for overlap-free case (e)").


test_make_weighting_fun() ->
    Wfun1 = ?MOD:make_weighting_fun(0.5, 2, 5),
    Wf1 = Wfun1(2),
    Wf2 = Wfun1(3),

    etap:ok((Wf1 >= 0) and (Wf1 =< 1),
            "Weighting function: Asym: 0.5, FillMin: 2, FillMax: 5, Index: 2:"
            " is in range"),
    etap:ok((Wf2 >= 0) and (Wf2 =< 1),
            "Weighting function: Asym: 0.5, FillMin: 2, FillMax: 5, Index: 3:"
            " is in range"),

    Wfun2 = ?MOD:make_weighting_fun(0.2, 2, 5),
    Wf5 = Wfun2(2),
    Wf6 = Wfun2(3),
    Wf7 = Wfun2(4),
    Wf8 = Wfun2(5),

    etap:ok((Wf5 >= 0) and (Wf5 =< 1),
            "Weighting function: Asym: 0.2, FillMin: 2, FillMax: 5, Index: 2:"
            " is in range"),
    etap:ok((Wf6 >= 0) and (Wf6 =< 1),
            "Weighting function: Asym: 0.2, FillMin: 2, FillMax: 5, Index: 3:"
            " is in range"),
    etap:ok((Wf7 >= 0) and (Wf7 =< 1),
            "Weighting function: Asym: 0.2, FillMin: 2, FillMax: 5, Index: 4:"
            " is in range"),
    etap:ok((Wf7 >= 0) and (Wf8 =< 1),
            "Weighting function: Asym: 0.2, FillMin: 2, FillMax: 5, Index: 5:"
            " is in range"),
    etap:ok(Wf7 < Wf6,
            "Weighting function: Asym: 0.2, FillMin: 2, FillMax: 5, Index: 4:"
            " is smaller than Index 3"),
    etap:ok(Wf8 < Wf5,
            "Weighting function: Asym: 0.2, FillMin: 2, FillMax: 5, Index: 5:"
            " is smaller than Index 2"),


    Wfun3 = ?MOD:make_weighting_fun(0.9, 2, 5),
    Wf9 = Wfun3(2),
    Wf10 = Wfun3(3),
    Wf11 = Wfun3(4),
    Wf12 = Wfun3(5),

    etap:ok((Wf9 >= 0) and (Wf9 =< 1),
            "Weighting function: Asym: 0.9, FillMin: 2, FillMax: 5, Index: 2:"
            " is in range"),
    etap:ok((Wf10 >= 0) and (Wf10 =< 1),
            "Weighting function: Asym: 0.9, FillMin: 2, FillMax: 5, Index: 3:"
            " is in range"),
    etap:ok((Wf11 >= 0) and (Wf11 =< 1),
            "Weighting function: Asym: 0.9, FillMin: 2, FillMax: 5, Index: 4:"
            " is in range"),
    etap:ok((Wf12 >= 0) and (Wf12 =< 1),
            "Weighting function: Asym: 0.9, FillMin: 2, FillMax: 5, Index: 5:"
            " is in range"),
    etap:ok(Wf11 < Wf10,
            "Weighting function: Asym: 0.9, FillMin: 2, FillMax: 5, Index: 4:"
            " is smaller than Index 3"),
    etap:ok(Wf12 < Wf9,
            "Weighting function: Asym: 0.9, FillMin: 2, FillMax: 5, Index: 5:"
            " is smaller than Index 2"),


    Wfun4 = ?MOD:make_weighting_fun(0.5, 3, 9),
    Wf13 = Wfun4(3),
    Wf14 = Wfun4(4),
    Wf15 = Wfun4(5),
    Wf16 = Wfun4(6),

    etap:ok((Wf13 >= 0) and (Wf13 =< 1),
            "Weighting function: Asym: 0.5, FillMin: 3, FillMax: 9, Index: 3:"
            " is in range"),
    etap:ok((Wf14 >= 0) and (Wf14 =< 1),
            "Weighting function: Asym: 0.5, FillMin: 3, FillMax: 9, Index: 4:"
            " is in range"),
    etap:ok((Wf15 >= 0) and (Wf15 =< 1),
            "Weighting function: Asym: 0.5, FillMin: 3, FillMax: 9, Index: 5:"
            " is in range"),
    etap:ok((Wf16 >= 0) and (Wf16 =< 1),
            "Weighting function: Asym: 0.5, FillMin: 3, FillMax: 9, Index: 6:"
            " is in range").


test_sort_dim() ->
    Less = fun(A, B) -> A < B end,

    Mbb1 = [{-38, 74.2}, {38, 948}, {-480, -27}, {-7, -4.28}, {84.3, 923.8}],
    Mbb2 = [{39, 938}, {-937, 8424}, {-1000, -82}, {4.72, 593}, {372, 490.3}],
    Node1 = {Mbb1, 3487},
    Node2 = {Mbb2, 823},
    Nodes = [Node1, Node2],
    SortedMin1 = ?MOD:sort_dim_min(Nodes, 1, Less),
    SortedMax1 = ?MOD:sort_dim_max(Nodes, 1, Less),
    SortedMin2 = ?MOD:sort_dim_min(Nodes, 2, Less),
    SortedMax2 = ?MOD:sort_dim_max(Nodes, 2, Less),
    SortedMin3 = ?MOD:sort_dim_min(Nodes, 3, Less),
    SortedMax3 = ?MOD:sort_dim_max(Nodes, 3, Less),
    SortedMin4 = ?MOD:sort_dim_min(Nodes, 4, Less),
    SortedMax4 = ?MOD:sort_dim_max(Nodes, 4, Less),
    SortedMin5 = ?MOD:sort_dim_min(Nodes, 5, Less),
    SortedMax5 = ?MOD:sort_dim_max(Nodes, 5, Less),
    etap:is(SortedMin1, [Node1, Node2], "Sorted by min 1st dimension"),
    etap:is(SortedMax1, [Node1, Node2], "Sorted by max 1st dimension"),
    etap:is(SortedMin2, [Node2, Node1], "Sorted by min 2nd dimension"),
    etap:is(SortedMax2, [Node1, Node2], "Sorted by max 2nd dimension"),
    etap:is(SortedMin3, [Node2, Node1], "Sorted by min 3rd dimension"),
    etap:is(SortedMax3, [Node2, Node1], "Sorted by max 3rd dimension"),
    etap:is(SortedMin4, [Node1, Node2], "Sorted by min 4th dimension"),
    etap:is(SortedMax4, [Node1, Node2], "Sorted by max 4th dimension"),
    etap:is(SortedMin5, [Node1, Node2], "Sorted by min 5th dimension"),
    etap:is(SortedMax5, [Node2, Node1], "Sorted by max 5th dimension").


test_perim_max() ->
    Mbb1 = [{-38, 74.2}, {38, 948}, {-480, -27}, {-7, -4.28}, {84.3, 923.8}],
    Mbb2 = [{-38, 74.2}, {948, 948}, {-480, -27}, {-7, -4.28}, {84.3, 923.8}],
    Perimeter1 = vtree_util:calc_perimeter(Mbb1),
    Perimeter2 = vtree_util:calc_perimeter(Mbb2),

    etap:is(?MOD:perim_max(Mbb1), Perimeter1*2-(-4.28 + 7), "Maximum perimeter"),
    etap:is(?MOD:perim_max(Mbb2), Perimeter2*2,
            "Maximum perimeter with one collapsed to a point dimension").


test_create_split_candidates() ->
    Node1 = {a, a},
    Node2 = {b, b},
    Node3 = {c, c},
    Node4 = {d, d},
    Node5 = {e, e},
    Node6 = {f, f},
    Node7 = {g, g},
    Node8 = {h, h},
    Node9 = {i, i},
    Node10 = {j, j},

    Nodes1 = [Node1, Node2, Node3, Node4, Node5],
    Max1 = ?ext_size([N || {_, N} <- tl(Nodes1)]),
    etap:is(?MOD:create_split_candidates(Nodes1, 0.2*Max1, Max1),
            [{[Node1], [Node2, Node3, Node4, Node5]},
             {[Node1, Node2], [Node3, Node4, Node5]},
             {[Node1, Node2, Node3], [Node4, Node5]},
             {[Node1, Node2, Node3, Node4], [Node5]}],
            "5 split candidates with max=size of 4 elements, "
            "min_fill_rate=0.2"),
    etap:is(?MOD:create_split_candidates(Nodes1, 0.5*Max1, Max1),
            [{[Node1, Node2], [Node3, Node4, Node5]},
             {[Node1, Node2, Node3], [Node4, Node5]}],
            "5 split candidates with max=size of 4 elements, "
            "min_fill_rate=0.5"),
    etap:is(?MOD:create_split_candidates(Nodes1, 0.7*Max1, Max1),
            [{[Node1], [Node2, Node3, Node4, Node5]},
             {[Node1, Node2], [Node3, Node4, Node5]},
             {[Node1, Node2, Node3], [Node4, Node5]},
             {[Node1, Node2, Node3, Node4], [Node5]}],
            "5 split candidates with max=size of 4 elements, "
            "min_fill_rate=0.7. This min_fill_rate is too high for a proper"
            "split, hence it will return all possible candidates"),

    Max2 = ?ext_size([N || {_, N} <- lists:nthtail(2, Nodes1)]),
    etap:is(?MOD:create_split_candidates(Nodes1, 0.5*Max2, Max2),
            [{[Node1, Node2], [Node3, Node4, Node5]},
             {[Node1, Node2, Node3], [Node4, Node5]}],
            "5 split candidates with max=size of 3 elements, "
            "min_fill_rate=0.5"),

    Nodes2 = Nodes1 ++ [Node6, Node7, Node8, Node9, Node10],
    Max3 = ?ext_size([N || {_, N} <- lists:nthtail(2, Nodes2)]),
    etap:is(?MOD:create_split_candidates(Nodes2, 0.5*Max3, Max3),
            [{[Node1, Node2, Node3, Node4],
              [Node5, Node6, Node7, Node8, Node9, Node10]},
             {[Node1, Node2, Node3, Node4, Node5],
              [Node6, Node7, Node8, Node9, Node10]},
             {[Node1, Node2, Node3, Node4, Node5, Node6],
              [Node7, Node8, Node9, Node10]}],
            "10 split candidates with max=size of 8 elements, "
            "min_fill_rate=0.5"),

    Nodes3 = [Node1, Node2, Node3, Node4, Node5, Node6, Node7],
    Max4 = ?ext_size([N || {_, N} <- tl(Nodes3)]),
    etap:is(?MOD:create_split_candidates(Nodes3, 0.8*Max4, Max4),
            [{[Node1], [Node2, Node3, Node4, Node5, Node6, Node7]},
             {[Node1, Node2], [Node3, Node4, Node5, Node6, Node7]},
             {[Node1, Node2, Node3], [Node4, Node5, Node6, Node7]},
             {[Node1, Node2, Node3, Node4], [Node5, Node6, Node7]},
             {[Node1, Node2, Node3, Node4, Node5], [Node6, Node7]},
             {[Node1, Node2, Node3, Node4, Node5, Node6], [Node7]}],
            "7 split candidates with max=size of 6 elements, "
            "min_fill_rate=0.8. This min_fill_rate is too high for a proper "
            "split, hence it will return all possible candidates"),

    Nodes4 = [{a, a}, {b, "this is a much bigger node (in bytes)"}],
    Max5 = ?ext_size([N || {_, N} <- Nodes4]),
    etap:is(?MOD:create_split_candidates(Nodes4, 0.5*Max5, Max5),
            [{[{a, a}], [{b, "this is a much bigger node (in bytes)"}]}],
            "2 split candidates with max=size of 2 elements, "
            "min_fill_rate=0.5, where one node has a much bigger byte size"),

    % Tests whether there is always a split candidate, even if it violates
    % the maximum chunk threshold

    % This call is needed, else you'll get a badmatch error for every call
    % to ?LOG_* (and create_split_candidates/3 will log an error.
    disk_log:open([{name, couch_disk_logger}, {format, external}]),

    Nodes5 = [Node1, Node2, Node3],
    Max6 = ?ext_size(Nodes5)/4,
    [Candidate5] = ?MOD:create_split_candidates(Nodes5, 0.5*Max6, Max6),
    {Overflow5, _} = Candidate5,
    etap:is(Candidate5, {[Node1], [Node2, Node3]},
            "The split candidate for the case when the maximum chunk threshold"
            "guarantee is violated (a)"),
    etap:ok(?ext_size(Overflow5) > Max6,
            "The first partition of the split candidate is *as expected* "
            "bigger than the maximum chunk threshold"),

    Nodes6 = [Node1, {b, "this is again a huge node"}, Node3],
    Max7 = ?ext_size(Nodes6)/3,
    [Candidate6] = ?MOD:create_split_candidates(Nodes6, 0.5*Max7, Max7),
    {Overflow6, _} = Candidate6,
    etap:is(Candidate6, {[Node1, {b, "this is again a huge node"}], [Node3]},
            "The split candidate for the case when the maximum chunk threshold"
            "guarantee is violated (b)"),
    etap:ok(?ext_size(Overflow6) > Max7,
            "The first partition of the split candidate is *as expected* "
            "bigger than the maximum chunk threshold (b)"),

    % MB-15975
    Nodes7 = [Node1, {b, "this is again a huge node"}],
    Min8 = ?ext_size(Node1),
    Max8 = ?ext_size(Nodes7)/4,
    [Candidate7] = ?MOD:create_split_candidates(Nodes7, Min8 , Max8),
    {Overflow7, _} = Candidate7,
    etap:is(Candidate7, {[Node1], [{b, "this is again a huge node"}]},
            "The split candidate for the case when the maximum chunk "
            "threshold guarantee is violated and the overflow happens after "
            "the last node"),
    etap:ok(?ext_size(Overflow7) < Max8,
            "The first partition of the split candidate is *not* bigger than "
            "the maximum chunk threshold as the node that made it overflow "
            "was moved to the other partition").


test_nodes_perimeter() ->
    Less = fun(A, B) -> A < B end,

    Mbb1 = [{-38, 74.2}, {38, 948}, {-480, -27}, {-7, -4.28}, {84.3, 923.8}],
    Mbb2 = [{39, 938}, {-937, 8424}, {-1000, -82}, {4.72, 593}, {372, 490.3}],
    Mbb3 = [{48, 472}, {-9.38, 26.1}, {-382, -29}, {-1.4, 30}, {39.9, 100}],
    Node1 = {Mbb1, 3487},
    Node2 = {Mbb2, 823},
    Node3 = {Mbb3, 96242},
    etap:is(?MOD:nodes_perimeter([Node1], Less),
            vtree_util:calc_perimeter(Mbb1),
            "Calculate the perimeter of a single node (a)"),
    etap:is(?MOD:nodes_perimeter([Node2], Less),
            vtree_util:calc_perimeter(Mbb2),
            "Calculate the perimeter of a single node (b)"),
    etap:is(?MOD:nodes_perimeter([Node1, Node2], Less),
            vtree_util:calc_perimeter(vtree_util:calc_mbb([Mbb1, Mbb2], Less)),
            "Calculate the perimeter of two nodes (a)"),
    etap:is(?MOD:nodes_perimeter([Node2, Node3], Less),
            vtree_util:calc_perimeter(vtree_util:calc_mbb([Mbb2, Mbb3], Less)),
            "Calculate the perimeter of two nodes (b)"),
    etap:is(?MOD:nodes_perimeter([Node1, Node2, Node3], Less),
            vtree_util:calc_perimeter(
              vtree_util:calc_mbb([Mbb1, Mbb2, Mbb3], Less)),
            "Calculate the perimeter of three nodes").


test_candidates_perimeter() ->
    Less = fun(A, B) -> A < B end,

    Mbb1 = [{-38, 74.2}, {38, 948}, {-480, -27}, {-7, -4.28}, {84.3, 923.8}],
    Mbb2 = [{39, 938}, {-937, 8424}, {-1000, -82}, {4.72, 593}, {372, 490.3}],
    Mbb3 = [{48, 472}, {-9.38, 26.1}, {-382, -29}, {-1.4, 30}, {39.9, 100}],
    Node1 = {Mbb1, 3487},
    Node2 = {Mbb2, 823},
    Node3 = {Mbb3, 96242},

    Max1 = ?ext_size([N || {_, N} <- [Node1, Node2]]),
    etap:is(?MOD:candidates_perimeter([Node1, Node2], 0.5*Max1, Max1, Less),
            {14202.0,
             ?MOD:create_split_candidates([Node1, Node2], 0.5*Max1, Max1)},
            "Calculate the total perimeter of all split candidates"),
    Max2 = ?ext_size([N || {_, N} <- [Node2, Node3]]),
    etap:is(?MOD:candidates_perimeter([Node2, Node3], 0.5*Max2, Max2, Less),
            {12788.56,
             ?MOD:create_split_candidates([Node2, Node3], 0.5*Max2, Max2)},
            "Calculate the total perimeter of all split candidates"),
    etap:is(?MOD:candidates_perimeter(
               [Node1, Node2, Node3], 0.5*Max1, Max1, Less),
            {28246.699999999997,
             ?MOD:create_split_candidates(
                [Node1, Node2, Node3], 0.5*Max1, Max1)},
            "Calculate the total perimeter of all split candidates").


test_min_perim() ->
    etap:is(?MOD:min_perim([{34.38, a}, {84, b}, {728, c}, {348, d},
                            {26, e}, {834.238, f}, {372, g}]),
            {26, e},
            "Calculate the tuple with the minimum perimeter"),
    etap:is(?MOD:min_perim([{34.38, a}]), {34.38, a},
            "Calculate the tuple with the minimum perimeter (one element)").


test_asym() ->
    Less = fun(A, B) -> A < B end,

    MbbO = [{-38, 74.2}, {38, 948}, {-480, -27}, {-7, -4.28}, {84.3, 84.3}],
    MbbAdd = [{48, 472}, {-9.38, 26.1}, {-382, -29}, {-1.4, 30}, {84.3, 84.3}],
    MbbN = vtree_util:calc_mbb([MbbO, MbbAdd], Less),

    etap:is(?MOD:asym(1, MbbO, MbbN), 0.78,
            "asym() value of the 1st dimension"),
    etap:is(?MOD:asym(2, MbbO, MbbN), -0.04948923102634272,
            "asym() value of the 2nd dimension"),
    etap:is(?MOD:asym(3, MbbO, MbbN), 0.0,
            "asym() value of the 3rd dimension"),
    etap:is(?MOD:asym(4, MbbO, MbbN), 0.9264864864864866,
            "asym() value of the 4th dimension"),
    etap:is(?MOD:asym(5, MbbO, MbbN), 0,
            "asym() value of the 5th dimension is zero").


test_mbb_dim_length() ->
    Mbb = [{-38, 74.2}, {38, 948}, {-480, -27}, {-7, -4.28}, {84.3, 923.8}],

    etap:is(?MOD:mbb_dim_length(1, Mbb), 112.2,
            "Length of the 1st dimension"),
    etap:is(?MOD:mbb_dim_length(2, Mbb), 910,
            "Length of the 2nd dimension"),
    etap:is(?MOD:mbb_dim_length(3, Mbb), 453,
            "Length of the 3rd dimension"),
    etap:is(?MOD:mbb_dim_length(4, Mbb), 2.7199999999999998,
            "Length of the 4th dimension"),
    etap:is(?MOD:mbb_dim_length(5, Mbb), 839.5,
            "Length of the 5th dimension").


test_mbb_dim_center() ->
    Mbb = [{-38, 74.2}, {38, 948}, {-480, -27}, {-7, -4.28}, {84.3, 923.8}],

    etap:is(?MOD:mbb_dim_center(1, Mbb), 18.1,
            "Center of the 1st dimension"),
    etap:is(?MOD:mbb_dim_center(2, Mbb), 493.0,
            "Center of the 2nd dimension"),
    etap:is(?MOD:mbb_dim_center(3, Mbb), -253.5,
            "Center of the 3rd dimension"),
    etap:is(?MOD:mbb_dim_center(4, Mbb), -5.640000000000001,
            "Center of the 4th dimension"),
    etap:is(?MOD:mbb_dim_center(5, Mbb), 504.05,
            "Center of the 5th dimension").
