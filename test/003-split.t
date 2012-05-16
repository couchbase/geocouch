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

-define(MOD, vtree_split).


main(_) ->
    code:add_pathz(filename:dirname(escript:script_name())),
    etap:plan(109),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            % Somehow etap:diag/1 and etap:bail/1 don't work properly
            %etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            %etap:bail(Other),
            io:format("Test died abnormally:~n~p~n", [Other])
     end.


test() ->
    test_split_axis(),
    test_choose_candidate(),
    test_goal_fun(),
    test_wg(),
    test_wg_overlapfree(),
    test_make_weighting_fun(),
    test_sort_dim(),
    test_calc_perimeter(),
    test_calc_volume(),
    test_perim_max(),
    test_create_split_candidates(),
    test_nodes_mbb(),
    test_nodes_perimeter(),
    test_candidates_perimeter(),
    test_min_perim(),
    test_intersect_mbb(),
    test_find_min_candidate(),
    test_asym(),
    test_mbb_dim_length(),
    test_mbb_dim_center(),
    ok.


test_split_axis() ->
    Less = fun(A, B) -> A < B end,

    Mbb1 = [{-38, 74.2}, {38, 948}, {-480, -27}, {-7, -4.28}, {84.3, 923.8}],
    Mbb2 = [{39, 938}, {-937, 8424}, {-1000, -82}, {4.72, 593}, {372, 490.3}],
    Mbb3 = [{48, 472}, {-9.38, 26.1}, {-382, -29}, {-1.4, 30}, {39.9, 100}],
    Node1 = {Mbb1, 3487},
    Node2 = {Mbb2, 823},
    Node3 = {Mbb3, 96242},

    etap:is(?MOD:split_axis([Node1, Node2, Node3], 1, 2, Less),
            [{[
               {[{-38,74.2},{38,948},{-480,-27},{-7,-4.28},{84.3,923.8}], 3487}],
              [
               {[{39,938},{-937,8424},{-1000,-82},{4.72,593},{372,490.3}], 823},
               {[{48,472},{-9.38,26.1},{-382,-29},{-1.4,30},{39.9,100}], 96242}
              ]},
             {[
               {[{-38,74.2},{38,948},{-480,-27},{-7,-4.28},{84.3,923.8}], 3487},
               {[{39,938},{-937,8424},{-1000,-82},{4.72,593},{372,490.3}], 823}],
              [
               {[{48,472},{-9.38,26.1},{-382,-29},{-1.4,30},{39.9,100}], 96242}]}],
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

    Candidates1 = ?MOD:create_split_candidates(
                    [Node1, Node2, Node3, Node4, Node5, Node6], 1, 5),
    MbbN1 = vtree_util:calc_mbb([Mbb1, Mbb2, Mbb3, Mbb4, Mbb5, Mbb6], Less),
    etap:is(?MOD:choose_candidate(Candidates1, 1, Mbb1, MbbN1, 1, 5, Less),
            lists:nth(5, Candidates1),
            "Best candidate (6 nodes)"),

    Candidates2 = ?MOD:create_split_candidates(
                    [Node3, Node4, Node5, Node6, Node2], 2, 4),
    MbbN2 = vtree_util:calc_mbb([Mbb3, Mbb4, Mbb5, Mbb6, Mbb2], Less),
    etap:is(?MOD:choose_candidate(Candidates2, 2, Mbb3, MbbN2, 2, 4, Less),
            lists:nth(2, Candidates2),
            "Best candidate (5 nodes)"),

    Candidates3 = ?MOD:create_split_candidates([Node3, Node5], 1, 1),
    MbbN3 = vtree_util:calc_mbb([Mbb3, Mbb5], Less),
    etap:is(?MOD:choose_candidate(Candidates3, 1, Mbb3, MbbN3, 1, 1, Less),
            lists:nth(1, Candidates3),
            "Best candidate (single partition)"),

    Candidates4 = ?MOD:create_split_candidates([Node3, Node4], 1, 1),
    MbbN4 = vtree_util:calc_mbb([Mbb3, Mbb4], Less),
    etap:is(?MOD:choose_candidate(Candidates4, 1, Mbb3, MbbN4, 1, 1, Less),
            lists:nth(1, Candidates4),
            "Best candidate "
            "(single partition, overlap-free)"),

    Candidates5 = ?MOD:create_split_candidates([Node7, Node8, Node9], 1, 2),
    MbbN5 = vtree_util:calc_mbb([Mbb7, Mbb8, Mbb9], Less),
    etap:is(?MOD:choose_candidate(Candidates5, 2, Mbb7, MbbN5, 1, 2, Less),
            lists:nth(2, Candidates5),
            "Best candidate (0 volume)").


test_goal_fun() ->
    Less = fun(A, B) -> A < B end,

    PerimMax = 0.436,
    Wf = ?MOD:make_weighting_fun(0.7, 1, 4),
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


    Candidates1 = ?MOD:create_split_candidates(
                     [Node1, Node2, Node3, Node4, Node5], 1, 4),
    etap:is(?MOD:goal_fun(lists:nth(1, Candidates1), PerimMax, Wf, Less),
            3641782.6880705818,
            "Goal function for non overlap-free case (a)"),
    etap:is(?MOD:goal_fun(lists:nth(2, Candidates1), PerimMax, Wf, Less),
            1400215.4316110883,
            "Goal function for non overlap-free case (b)"),
    etap:is(?MOD:goal_fun(lists:nth(3, Candidates1), PerimMax, Wf, Less),
            628714.2773957844,
            "Goal function for non overlap-free case (c)"),
    etap:is(?MOD:goal_fun(lists:nth(4, Candidates1), PerimMax, Wf, Less),
            16062.054113644115,
            "Goal function for non overlap-free case (d)"),

    Candidates2 = ?MOD:create_split_candidates(
                     [Node6, Node7, Node8, Node9, Node10], 1, 4),
    etap:is(?MOD:goal_fun(lists:nth(1, Candidates2), PerimMax, Wf, Less),
            23.192926068399995,
            "Goal function for overlap-free case (a)"),
    etap:is(?MOD:goal_fun(lists:nth(2, Candidates2), PerimMax, Wf, Less),
            100.23592980868027,
            "Goal function for overlap-free case (b)"),
    etap:is(?MOD:goal_fun(lists:nth(3, Candidates2), PerimMax, Wf, Less),
            190.02099336502368,
            "Goal function for overlap-free case (c)"),
    etap:is(?MOD:goal_fun(lists:nth(4, Candidates2), PerimMax, Wf, Less),
            205.64091005484858,
            "Goal function for overlap-free case (d)").


test_wg() ->
    Less = fun(A, B) -> A < B end,

    PerimMax = 0.436,
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


    Candidates1 = ?MOD:create_split_candidates(
                     [Node1, Node2, Node3, Node4, Node5], 1, 4),
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

    Candidates2 = ?MOD:create_split_candidates(
                    [Node1, Node2, Node3, Node4, Node6], 1, 4),
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

    Candidates = ?MOD:create_split_candidates(
                    [Node1, Node2, Node3, Node4, Node5, Node6], 1, 5),
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
    Wf3 = Wfun1(4),
    Wf4 = Wfun1(5),

    etap:ok((Wf1 >= 0) and (Wf1 =< 1),
            "Weighting function: Asym: 0.5, FillMin: 2, FillMax: 5, Index: 2:"
            " is in range"),
    etap:ok((Wf2 >= 0) and (Wf2 =< 1),
            "Weighting function: Asym: 0.5, FillMin: 2, FillMax: 5, Index: 3:"
            " is in range"),
    etap:is(lose_precision(Wf3, 5), lose_precision(Wf2, 5),
            "Weighting function: Asym: 0.5, FillMin: 2, FillMax: 5, Index: 4:"
            " is symmetric to Index 3"),
    etap:is(lose_precision(Wf4, 5), lose_precision(Wf1, 5),
            "Weighting function: Asym: 0.5, FillMin: 2, FillMax: 5, Index: 5:"
            " is symmetric to Index 2"),


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
    etap:ok(Wf11 > Wf10,
            "Weighting function: Asym: 0.9, FillMin: 2, FillMax: 5, Index: 4:"
            " is greater than Index 3"),
    etap:ok(Wf12 > Wf9,
            "Weighting function: Asym: 0.9, FillMin: 2, FillMax: 5, Index: 5:"
            " is greater than Index 2"),


    Wfun4 = ?MOD:make_weighting_fun(0.5, 3, 9),
    Wf13 = Wfun4(3),
    Wf14 = Wfun4(4),
    Wf15 = Wfun4(5),
    Wf16 = Wfun4(6),
    Wf17 = Wfun4(7),
    Wf18 = Wfun4(8),
    Wf19 = Wfun4(9),

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
            " is in range"),
    etap:is(lose_precision(Wf17, 5), lose_precision(Wf15, 5),
            "Weighting function: Asym: 0.5, FillMin: 3, FillMax: 9, Index: 7:"
            " is symmetric to Index 5"),
    etap:is(lose_precision(Wf18, 5), lose_precision(Wf14, 5),
            "Weighting function: Asym: 0.5, FillMin: 3, FillMax: 9, Index: 8:"
            " is symmetric to Index 4"),
    etap:is(lose_precision(Wf19, 5), lose_precision(Wf13, 5),
            "Weighting function: Asym: 0.5, FillMin: 3, FillMax: 9, Index: 9:"
            " is symmetric to Index 3").


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


test_calc_perimeter() ->
    Mbb1 = [{-38, 74.2}, {38, 948}, {-480, -27}, {-7, -4.28}, {84.3, 923.8}],
    Mbb2 = [{39, 938}, {-937, 8424}, {-1000, -82}, {4.72, 593}, {372, 490.3}],

    etap:is(?MOD:calc_perimeter(Mbb1), 2317.42, "Perimeter of an MBB (a)"),
    etap:is(?MOD:calc_perimeter(Mbb2), 11884.58, "Perimeter of an MBB (b)").


test_calc_volume() ->
    Mbb1 = [{-38, 74.2}, {38, 948}, {-480, -27}, {-7, -4.28}, {84.3, 923.8}],
    Mbb2 = [{39, 938}, {-937, 8424}, {-1000, -82}, {4.72, 593}, {372, 490.3}],
    Mbb3 = [{48, 472}, {-9.38, 26.1}, {-29, -29}, {-1.4, 30}, {39.9, 100}],

    etap:is(?MOD:calc_volume(Mbb1), 105614137268.64, "Volume of an MBB (a)"),
    etap:is(?MOD:calc_volume(Mbb2), 537642320109142.24,
            "Volume of an MBB (b)"),
    etap:is(?MOD:calc_volume(Mbb3), 0, "Zero volume of an MBB").


test_perim_max() ->
    Mbb1 = [{-38, 74.2}, {38, 948}, {-480, -27}, {-7, -4.28}, {84.3, 923.8}],
    Mbb2 = [{-38, 74.2}, {948, 948}, {-480, -27}, {-7, -4.28}, {84.3, 923.8}],
    Perimeter1 = ?MOD:calc_perimeter(Mbb1),
    Perimeter2 = ?MOD:calc_perimeter(Mbb2),

    etap:is(?MOD:perim_max(Mbb1), Perimeter1*2-(-4.28 + 7), "Maximum perimeter"),
    etap:is(?MOD:perim_max(Mbb2), Perimeter2*2,
            "Maximum perimeter with one collapsed to a point dimension").


test_create_split_candidates() ->
    Nodes = [a,b,c,d,e],
    etap:is(?MOD:create_split_candidates(Nodes, 1, 4),
            [{[a], [b,c,d,e]}, {[a,b], [c,d,e]}, {[a,b,c], [d,e]},
             {[a,b,c,d], [e]}],
            "5 split candidates with min=1, max=4"),
    etap:is(?MOD:create_split_candidates(Nodes, 2, 4),
            [{[a,b], [c,d,e]}, {[a,b,c], [d,e]}],
            "5 split candidates with min=2, max=4"),
    etap:is(?MOD:create_split_candidates(Nodes, 2, 3),
            [{[a,b], [c,d,e]}, {[a,b,c], [d,e]}],
            "5 split candidates with min=2, max=3"),
    etap:is(?MOD:create_split_candidates(Nodes, 3, 4),
            [],
            "5 split candidates, can't create split with min=3, max=4"),
    Nodes2 = [a,b,c,d,e,f,g,h,i,j],
    etap:is(?MOD:create_split_candidates(Nodes2, 4, 8),
            [{[a,b,c,d], [e,f,g,h,i,j]},
             {[a,b,c,d,e], [f,g,h,i,j]},
             {[a,b,c,d,e,f], [g,h,i,j]}],
            "10 split candidates with min=4, max=8").


test_nodes_mbb() ->
    Less = fun(A, B) -> A < B end,

    Mbb1 = [{-38, 74.2}, {38, 948}, {-480, -27}, {-7, -4.28}, {84.3, 923.8}],
    Mbb2 = [{39, 938}, {-937, 8424}, {-1000, -82}, {4.72, 593}, {372, 490.3}],
    Mbb3 = [{48, 472}, {-9.38, 26.1}, {-382, -29}, {-1.4, 30}, {39.9, 100}],
    Node1 = {Mbb1, 3487},
    Node2 = {Mbb2, 823},
    Node3 = {Mbb3, 96242},

    etap:is(?MOD:nodes_mbb([Node1, Node2], Less),
            vtree_util:calc_mbb([Mbb1, Mbb2], Less),
            "Calculate the MBB of two nodes (a)"),
    etap:is(?MOD:nodes_mbb([Node2, Node3], Less),
            vtree_util:calc_mbb([Mbb2, Mbb3], Less),
            "Calculate the MBB of two nodes (b)"),
    etap:is(?MOD:nodes_mbb([Node2], Less), vtree_util:calc_mbb([Mbb2], Less),
            "Calculate the MBB of a single node (a)"),
    etap:is(?MOD:nodes_mbb([Node3], Less), vtree_util:calc_mbb([Mbb3], Less),
            "Calculate the MBB of a single node (b)").


test_nodes_perimeter() ->
    Less = fun(A, B) -> A < B end,

    Mbb1 = [{-38, 74.2}, {38, 948}, {-480, -27}, {-7, -4.28}, {84.3, 923.8}],
    Mbb2 = [{39, 938}, {-937, 8424}, {-1000, -82}, {4.72, 593}, {372, 490.3}],
    Mbb3 = [{48, 472}, {-9.38, 26.1}, {-382, -29}, {-1.4, 30}, {39.9, 100}],
    Node1 = {Mbb1, 3487},
    Node2 = {Mbb2, 823},
    Node3 = {Mbb3, 96242},
    etap:is(?MOD:nodes_perimeter([Node1], Less), ?MOD:calc_perimeter(Mbb1),
            "Calculate the perimeter of a single node (a)"),
    etap:is(?MOD:nodes_perimeter([Node2], Less), ?MOD:calc_perimeter(Mbb2),
            "Calculate the perimeter of a single node (b)"),
    etap:is(?MOD:nodes_perimeter([Node1, Node2], Less),
            ?MOD:calc_perimeter(vtree_util:calc_mbb([Mbb1, Mbb2], Less)),
            "Calculate the perimeter of two nodes (a)"),
    etap:is(?MOD:nodes_perimeter([Node2, Node3], Less),
            ?MOD:calc_perimeter(vtree_util:calc_mbb([Mbb2, Mbb3], Less)),
            "Calculate the perimeter of two nodes (b)"),
    etap:is(?MOD:nodes_perimeter([Node1, Node2, Node3], Less),
            ?MOD:calc_perimeter(vtree_util:calc_mbb([Mbb1, Mbb2, Mbb3], Less)),
            "Calculate the perimeter of three nodes").


test_candidates_perimeter() ->
    Less = fun(A, B) -> A < B end,

    Mbb1 = [{-38, 74.2}, {38, 948}, {-480, -27}, {-7, -4.28}, {84.3, 923.8}],
    Mbb2 = [{39, 938}, {-937, 8424}, {-1000, -82}, {4.72, 593}, {372, 490.3}],
    Mbb3 = [{48, 472}, {-9.38, 26.1}, {-382, -29}, {-1.4, 30}, {39.9, 100}],
    Node1 = {Mbb1, 3487},
    Node2 = {Mbb2, 823},
    Node3 = {Mbb3, 96242},

    etap:is(?MOD:candidates_perimeter([Node1, Node2], 1, 2, Less),
            {14202.0,
             ?MOD:create_split_candidates([Node1, Node2], 1, 2)},
            "Calculate the total perimeter of all split candidates"),
    etap:is(?MOD:candidates_perimeter([Node2, Node3], 1, 2, Less),
            {12788.56,
             ?MOD:create_split_candidates([Node2, Node3], 1, 2)},
            "Calculate the total perimeter of all split candidates"),
    etap:is(?MOD:candidates_perimeter([Node1, Node2, Node3], 1, 2, Less),
            {28246.699999999997,
             ?MOD:create_split_candidates([Node1, Node2, Node3], 1, 2)},
            "Calculate the total perimeter of all split candidates").


test_min_perim() ->
    etap:is(?MOD:min_perim([{34.38, a}, {84, b}, {728, c}, {348, d},
                            {26, e}, {834.238, f}, {372, g}]),
            {26, e},
            "Calculate the tuple with the minimum perimeter"),
    etap:is(?MOD:min_perim([{34.38, a}]), {34.38, a},
            "Calculate the tuple with the minimum perimeter (one element)").


test_intersect_mbb() ->
    Less = fun(A, B) -> A < B end,

    Mbb1 = [{-38, 74.2}, {38, 948}],
    Mbb2 = [{-480, 5}, {-7, 428.74}],
    Mbb3 = [{84.3, 923.8}, {39, 938}],
    Mbb4 = [{-937, 8424}, {-1000, -82}],
    Mbb5 = [{4.72, 593}, {-472, -390.3}],
    Mbb6 = [{4.72, 593}, {-472, 390.3}, {-480, 5}, {-7, 428.74}],
    Mbb7 = [{84.3, 923.8}, {39, 938}, {-937, 8424}, {-1000, 82}],
    Mbb8 = [{222, 222}, {-432.39, -294.20}],
    Mbb9 = [{-222, -222}, {-382.39, 294.20}],
    Mbb10 = [{593, 777}, {-432.39, -294.20}],
    Mbb11 = [{593, 593}, {-432.39, -294.20}],


    etap:is(?MOD:intersect_mbb(Mbb1, Mbb2, Less), [{-38,5},{38,428.74}],
            "MBBs intersect (two dimensions)"),
    etap:is(?MOD:intersect_mbb(Mbb2, Mbb3, Less), overlapfree,
            "MBBs are overlap-free (first dimension)"),
    etap:is(?MOD:intersect_mbb(Mbb3, Mbb4, Less), overlapfree,
            "MBBs are overlap-free (second dimension)"),
    etap:is(?MOD:intersect_mbb(Mbb4, Mbb5, Less), Mbb5,
            "One MBB is in another MBB"),
    etap:is(?MOD:intersect_mbb(Mbb6, Mbb7, Less),
            [{84.3,593},{39,390.3},{-480,5},{-7,82}],
            "MBBs intersect (4 dimensions)"),
    etap:is(?MOD:intersect_mbb(Mbb5, Mbb8, Less),
            [{222,222},{-432.39,-390.3}],
            "One MBBs has zero volume and intersects"),
    etap:is(?MOD:intersect_mbb(Mbb3, Mbb9, Less),
            overlapfree,
            "One MBBs has zero volume and doesn't overlap (is overlap-free)"),
    etap:is(?MOD:intersect_mbb(Mbb5, Mbb10, Less),
            overlapfree,
            "One MBB touches another MBB"),
    etap:is(?MOD:intersect_mbb(Mbb5, Mbb11, Less),
            overlapfree,
            "A zero volume MBB touches another MBB").


test_find_min_candidate() ->
    Candidates1 = ?MOD:create_split_candidates([a,b,c,d,e,f], 1, 5),
    MinFun = fun({F, S}) -> abs(length(F) - length(S)) end,
    etap:is(?MOD:find_min_candidate(MinFun, Candidates1),
            {[a,b,c],[d,e,f]},
            "Candidate with min value"),

    Candidates2 = [{[a],[b,c]}],
    etap:is(?MOD:find_min_candidate(MinFun, Candidates2),
            hd(Candidates2),
            "Single candidate").


test_asym() ->
    Less = fun(A, B) -> A < B end,

    MbbO = [{-38, 74.2}, {38, 948}, {-480, -27}, {-7, -4.28}, {84.3, 923.8}],
    MbbAdd = [{48, 472}, {-9.38, 26.1}, {-382, -29}, {-1.4, 30}, {39.9, 100}],
    MbbN = vtree_util:calc_mbb([MbbO, MbbAdd], Less),

    etap:is(?MOD:asym(1, MbbO, MbbN), 0.78,
            "asym() value of the 1st dimension"),
    etap:is(?MOD:asym(2, MbbO, MbbN), -0.04948923102634272,
            "asym() value of the 2nd dimension"),
    etap:is(?MOD:asym(3, MbbO, MbbN), 0.0,
            "asym() value of the 3rd dimension"),
    etap:is(?MOD:asym(4, MbbO, MbbN), 0.9264864864864866,
            "asym() value of the 4th dimension"),
    etap:is(?MOD:asym(5, MbbO, MbbN), -0.050231926688539534,
            "asym() value of the 5th dimension").


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


% From http://www.codecodex.com/wiki/Round_a_number_to_a_specific_decimal_place (2012-05-16)
lose_precision(Number, Precision) ->
    P = math:pow(10, Precision),
    round(Number * P) / P.
