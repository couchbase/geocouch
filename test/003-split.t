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
    etap:plan(63),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail(Other)
    end.


test() ->
    test_split_axis(),
    test_choose_candidate(),
    test_sort_dim(),
    test_calc_perimeter(),
    test_calc_volume(),
    test_create_split_candidates(),
    test_nodes_mbb(),
    test_nodes_perimeter(),
    test_candidates_perimeter(),
    test_min_perim(),
    test_intersect_mbb(),
    test_overlapfree_candidates(),
    test_find_min_candidate(),
    test_min_perimeter_candidate(),
    test_min_volume_overlap_candidate(),
    test_min_perimeter_overlap_candidate(),
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
    etap:is(?MOD:choose_candidate(Candidates1, Less),
            lists:last(Candidates1),
            "Best candidate (6 nodes)"),

    Candidates2 = ?MOD:create_split_candidates(
                    [Node3, Node4, Node5, Node6, Node2], 2, 4),
    etap:is(?MOD:choose_candidate(Candidates2, Less),
            lists:nth(2, Candidates2),
            "Best candidate (5 nodes)"),

    Candidates3 = ?MOD:create_split_candidates([Node3, Node5], 1, 1),
    etap:is(?MOD:choose_candidate(Candidates3, Less),
            lists:nth(1, Candidates3),
            "Best candidate (single partition)"),
    Candidates4 = ?MOD:create_split_candidates([Node3, Node4], 1, 1),

    etap:is(?MOD:choose_candidate(Candidates4, Less),
            lists:nth(1, Candidates4),
            "Best candidate "
            "(single partition, overlap-free)"),

    Candidates5 = ?MOD:create_split_candidates(
                    [Node7, Node8, Node9], 1, 2),
    etap:is(?MOD:choose_candidate(Candidates5, Less),
            lists:nth(2, Candidates5),
            "Best candidate (0 volume)").


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


test_overlapfree_candidates() ->
    Less = fun(A, B) -> A < B end,

    Node1 = {[{-38000, -7400.2}, {-38000, 9480}], a},
    Node2 = {[{-480, 5}, {-7, 28.74}], b},
    Node3 = {[{84.3, 923.8}, {39, 938}], c},
    Node4 = {[{-937, 8424}, {-1000, -82}], d},
    Node5 = {[{4.72, 593}, {372, 490.3}], e},
    Node6 = {[{48, 472}, {-9.38, 26.1}], f},

    Candidates1 = ?MOD:create_split_candidates(
                    [Node1, Node2, Node3, Node4, Node5, Node6], 1, 5),
    NotOverlapFree = ?MOD:overlapfree_candidates(Candidates1, Less),
    etap:is(length(NotOverlapFree), 1,
            "One overlap-free candidate"),

    Candidates2 = ?MOD:create_split_candidates(
                    [Node2, Node3, Node4, Node5, Node6], 1, 5),
    OverlapFree = ?MOD:overlapfree_candidates(Candidates2, Less),
    etap:is(length(OverlapFree), 0,
            "No overlap-free candidates").


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


test_min_perimeter_candidate() ->
    Less = fun(A, B) -> A < B end,

    Node1 = {[{-38000, -7400.2}, {-38000, 9480}], a},
    Node2 = {[{-480, 5}, {-7, 28.74}], b},
    Node3 = {[{84.3, 923.8}, {39, 938}], c},
    Node4 = {[{-937, 8424}, {-1000, -82}], d},
    Node5 = {[{4.72, 593}, {372, 490.3}], e},
    Node6 = {[{48, 472}, {-9.38, 26.1}], f},

    Candidates1 = ?MOD:create_split_candidates(
                    [Node1, Node2, Node3, Node4, Node5, Node6], 1, 5),
    etap:is(?MOD:min_perimeter_candidate(Candidates1, Less),
            lists:nth(1, Candidates1),
            "Candidate with minimum perimeter (6 nodes)"),

    Candidates2 = ?MOD:create_split_candidates(
                    [Node3, Node4, Node5, Node6, Node2], 2, 4),
    etap:is(?MOD:min_perimeter_candidate(Candidates2, Less),
            lists:nth(2, Candidates2),
            "Candidate with minimum perimeter (5 nodes)"),

    Candidates3 = ?MOD:create_split_candidates([Node3, Node4], 1, 1),
    etap:is(?MOD:min_perimeter_candidate(Candidates3, Less),
            lists:nth(1, Candidates3),
            "Candidate with minimum perimeter (single partition)").


test_min_volume_overlap_candidate() ->
    Less = fun(A, B) -> A < B end,

    Mbb1 = [{-380, -74.2}, {-380, 948}],
    Mbb2 = [{-480, 5}, {-7, 28.74}],
    Mbb3 = [{84.3, 923.8}, {39, 938}],
    Mbb4 = [{-937, 8424}, {-1000, -82}],
    Mbb5 = [{4.72, 593}, {372, 490.3}],
    Mbb6 = [{48, 472}, {-9.38, 26.1}],
    Mbb7 = [{222, 222}, {462, 781}],
    Node1 = {Mbb1, a},
    Node2 = {Mbb2, b},
    Node3 = {Mbb3, c},
    Node4 = {Mbb4, d},
    Node5 = {Mbb5, e},
    Node6 = {Mbb6, f},
    Node7 = {Mbb7, g},

    Candidates1 = ?MOD:create_split_candidates(
                    [Node1, Node2, Node3, Node4, Node5, Node6], 1, 5),
    etap:is(?MOD:min_volume_overlap_candidate(Candidates1, Less),
            lists:last(Candidates1),
            "Candidate with minimum overlap (volume) (6 nodes)"),

    Candidates2 = ?MOD:create_split_candidates(
                    [Node3, Node4, Node5, Node6, Node2], 2, 4),
    etap:is(?MOD:min_volume_overlap_candidate(Candidates2, Less),
            lists:nth(2, Candidates2),
            "Candidate with minimum overlap (volume) (5 nodes)"),

    Candidates3 = ?MOD:create_split_candidates([Node3, Node5], 1, 1),
    etap:is(?MOD:min_volume_overlap_candidate(Candidates3, Less),
            lists:nth(1, Candidates3),
            "Candidate with minimum overlap (volume) (single partition)"),

    Candidates4 = ?MOD:create_split_candidates([Node3, Node7], 1, 1),
    etap:is(?MOD:min_volume_overlap_candidate(Candidates4, Less),
            lists:nth(1, Candidates4),
            "Candidate with minimum overlap (volume) (zero volume)").


test_min_perimeter_overlap_candidate() ->
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

    Candidates1 = ?MOD:create_split_candidates(
                    [Node1, Node2, Node3, Node4, Node5, Node6], 1, 5),
    etap:is(?MOD:min_perimeter_overlap_candidate(Candidates1, Less),
            lists:last(Candidates1),
            "Candidate with minimum overlap (perimeter) (6 nodes)"),
    Candidates2 = ?MOD:create_split_candidates(
                    [Node3, Node4, Node5, Node6, Node2], 2, 4),
    etap:is(?MOD:min_perimeter_overlap_candidate(Candidates2, Less),
            lists:nth(2, Candidates2),
            "Candidate with minimum overlap (perimeter) (5 nodes)"),

    Candidates3 = ?MOD:create_split_candidates([Node3, Node5], 1, 1),
    etap:is(?MOD:min_perimeter_overlap_candidate(Candidates3, Less),
            lists:nth(1, Candidates3),
            "Candidate with minimum overlap (perimeter) (single partition)").
