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

-define(MOD, vtree_choose).


main(_) ->
    code:add_pathz(filename:dirname(escript:script_name())),
    etap:plan(51),
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
    test_choose_subtree(),
    test_limit_nodes(),
    test_process_limited(),
    test_check_comp_perimeter(),
    test_check_comp_volume(),
    test_any_zero_volume(),
    test_calc_delta_perimeter(),
    test_calc_delta_common_overlap_perimeter(),
    test_calc_delta_common_overlap_volume(),
    test_min_size(),
    test_within_mbb(),
    ok.


test_choose_subtree() ->
    Less = fun(A, B) -> A < B end,

    Mbb1 = [{-380, -74.2}, {-380, 948}],
    Mbb2 = [{-480, 5}, {-7, 28.74}],
    Mbb3 = [{84.3, 923.8}, {39, 938}],
    Mbb4 = [{-937, 8424}, {-1000, -82}],
    Mbb5 = [{4.72, 593}, {372, 490.3}],
    Mbb6 = [{148, 472}, {85.38, 260.1}],

    Node1 = {Mbb1, a},
    Node2 = {Mbb2, b},
    Node3 = {Mbb3, c},
    Node4 = {Mbb4, d},
    Node5 = {Mbb5, e},

    etap:is(?MOD:choose_subtree([Node1, Node2, Node3, Node4], Mbb6, Less),
            Node3, "One node encloses the NewMbb completely"),
    etap:is(?MOD:choose_subtree([Node1, Node2, Node3, Node4], Mbb5, Less),
            Node3, "No node fully encloses the NewMbb, but the first node "
            "*won't* increase the overlap with the existing nodes if the "
            "new node is added to the first one"),
    etap:is(?MOD:choose_subtree([Node1, Node2, Node4, Node5], Mbb6, Less),
            Node5, "No node fully encloses the NewMbb and the first node "
            "*will* increase the overlap with the existing nodes if the "
            "new node is added to the first one").


test_limit_nodes() ->
    Less = fun(A, B) -> A < B end,

    Mbb1 = [{-380, -74.2}, {-380, 948}],
    Mbb2 = [{-480, 5}, {-7, 28.74}],
    Mbb3 = [{84.3, 923.8}, {39, 938}],
    Mbb4 = [{-937, 8424}, {-1000, -82}],
    Mbb5 = [{4.72, 593}, {372, 490.3}],

    Node1 = {Mbb1, a},
    Node2 = {Mbb2, b},
    Node3 = {Mbb3, c},
    Node4 = {Mbb4, d},
    Node5 = {Mbb5, e},

    etap:is(?MOD:limit_nodes([Node3, Node2, Node4, Node1], Mbb5, Less),
            [],
            "New node is fully within the first node, hence no overlap "
            "with other nodes will be increased"),
    etap:is(?MOD:limit_nodes([Node2, Node3, Node5, Node4], Mbb5, Less),
            [Node3, Node5], "Check that it is limited to the correct nodes").


test_process_limited() ->
    Less = fun(A, B) -> A < B end,

    Mbb1 = [{-38, 74.2}, {38, 948}, {-480, -27}, {-7, -4.28}, {84.3, 923.8}],
    Mbb2 = [{39, 938}, {-937, 8424}, {-1000, -82}, {4.72, 593}, {372, 490.3}],
    Mbb3 = [{48, 472}, {-9.38, 26.1}, {-382, -29}, {-1.4, 30}, {39.9, 100}],
    Mbb4 = [{48, 472}, {-9.38, 26.1}, {-29, -29}, {-1.4, 30}, {39.9, 100}],

    Mbb5 = [{43.5, 95.2}, {-83, 432}, {-8.6, -0.5}, {0, 582.2}, {50, 100}],
    Mbb6 = [{43.5, 95.2}, {-83, 432}, {-29, -29}, {0, 582.2}, {50, 100}],

    Node1 = {Mbb1, 3487},
    Node2 = {Mbb2, 823},
    Node3 = {Mbb3, 96242},
    Node4 = {Mbb4, 6948},

    etap:is(?MOD:process_limited([Node1, Node2, Node3], Mbb5, Less),
            Node3, "No zero volume node => volume based"),
    etap:is(?MOD:process_limited([Node1, Node4, Node2, Node3], Mbb6, Less),
            Node4, "One zero volume node => perimeter based").


test_check_comp_perimeter() ->
    Less = fun(A, B) -> A < B end,

    Mbb1 = [{-380, -74.2}, {-380, 948}],
    Mbb2 = [{-480, 5}, {-7, 28.74}],
    Mbb3 = [{84.3, 923.8}, {39, 938}],
    Mbb4 = [{-937, 8424}, {-1000, -82}],
    Mbb5 = [{4.72, 593}, {372, 490.3}],
    Mbb6 = [{48, 472}, {-9.38, 26.1}],

    Mbb8 = [{-200, -180}, {40, 50}],
    Mbb9 = [{-150, -120}, {40, 50}],
    Mbb10 = [{-100, -80}, {40, 50}],
    Mbb11 = [{-50, -20}, {40, 50}],
    Mbb12 = [{0, 20}, {40, 50}],

    Mbb13 = [{-170, -110}, {40, 50}],
    Mbb14 = [{-140, -130}, {42, 48}],
    Mbb15 = [{-110, -80}, {40, 50}],

    Mbb16 = [{-200, -100}, {40, 50}],
    Mbb17 = [{-50, 0}, {40, 50}],
    Mbb18 = [{-200, -100}, {100, 200}],
    Mbb19 = [{-200, -100}, {300, 350}],

    Mbb20 = [{-150, -120}, {20, 200}],

    Node1 = {Mbb1, a},
    Node2 = {Mbb2, b},
    Node3 = {Mbb3, c},
    Node4 = {Mbb4, d},
    Node5 = {Mbb5, e},

    Node8 = {Mbb8, h},
    Node9 = {Mbb9, i},
    Node10 = {Mbb10, j},
    Node11 = {Mbb11, k},
    Node12 = {Mbb12, l},

    Node16 = {Mbb16, p},
    Node17 = {Mbb17, q},
    Node18 = {Mbb18, r},
    Node19 = {Mbb19, s},

    Nodes1 = [Node1, Node2, Node3, Node4, Node5],
    {overlap, Comp1} = ?MOD:check_comp(perimeter, Nodes1, Mbb6, Less),
    {_, Overlaps1} = lists:unzip(Comp1),
    etap:is(Overlaps1, [Node1, Node4, Node3, Node5, Node2],
            "Non overlap-free case"),

    Nodes2 = [Node8, Node9, Node10, Node11, Node12],
    {success, Comp2} = ?MOD:check_comp(perimeter, Nodes2, Mbb13, Less),
    etap:is(Comp2, Node9, "Don't need to expand any node, when added to 2nd, "
           "bigger than 2nd node"),
    {success, Comp3} = ?MOD:check_comp(perimeter, Nodes2, Mbb14, Less),
    etap:is(Comp3, Node9, "Don't need to expand any node, when added to 2nd, "
           "within 2nd node"),
    {success, Comp4} = ?MOD:check_comp(perimeter, Nodes2, Mbb15, Less),
    etap:is(Comp4, Node10, "Don't need to expand any node, when added to 3rd, "
           "bigger than 3rd node"),

    Nodes3 = [Node16, Node17, Node18, Node19],
    {overlap, Comp5} = ?MOD:check_comp(perimeter, Nodes3, Mbb20, Less),
    {_, Overlaps5} = lists:unzip(Comp5),
    etap:is(Overlaps5, [Node16, Node18],
            "Cand doesn't contain all nodes").


test_check_comp_volume() ->
    Less = fun(A, B) -> A < B end,

    Mbb1 = [{-380, -74.2}, {-380, 948}],
    Mbb2 = [{-480, 5}, {-7, 28.74}],
    Mbb3 = [{84.3, 923.8}, {39, 938}],
    Mbb4 = [{-937, 8424}, {-1000, -82}],
    Mbb5 = [{4.72, 593}, {372, 490.3}],
    Mbb6 = [{48, 472}, {-9.38, 26.1}],

    Mbb8 = [{-200, -180}, {40, 50}],
    Mbb9 = [{-150, -120}, {40, 50}],
    Mbb10 = [{-100, -80}, {40, 50}],
    Mbb11 = [{-50, -20}, {40, 50}],
    Mbb12 = [{0, 20}, {40, 50}],

    Mbb13 = [{-170, -110}, {40, 50}],
    Mbb14 = [{-140, -130}, {42, 48}],
    Mbb15 = [{-110, -80}, {40, 50}],

    Mbb16 = [{-200, -100}, {40, 50}],
    Mbb17 = [{-50, 0}, {40, 50}],
    Mbb18 = [{-200, -100}, {100, 200}],
    Mbb19 = [{-200, -100}, {300, 350}],

    Mbb20 = [{-150, -120}, {20, 200}],

    Node1 = {Mbb1, a},
    Node2 = {Mbb2, b},
    Node3 = {Mbb3, c},
    Node4 = {Mbb4, d},
    Node5 = {Mbb5, e},

    Node8 = {Mbb8, h},
    Node9 = {Mbb9, i},
    Node10 = {Mbb10, j},
    Node11 = {Mbb11, k},
    Node12 = {Mbb12, l},

    Node16 = {Mbb16, p},
    Node17 = {Mbb17, q},
    Node18 = {Mbb18, r},
    Node19 = {Mbb19, s},

    Nodes1 = [Node1, Node2, Node3, Node4, Node5],
    {overlap, Comp1} = ?MOD:check_comp(volume, Nodes1, Mbb6, Less),
    {Numbers1, Overlaps1} = lists:unzip(Comp1),
    etap:is(Overlaps1, [Node1, Node4, Node3, Node5, Node2],
            "Non overlap-free case"),

    Nodes2 = [Node8, Node9, Node10, Node11, Node12],
    {success, Comp2} = ?MOD:check_comp(volume, Nodes2, Mbb13, Less),
    etap:is(Comp2, Node9, "Don't need to expand any node, when added to 2nd, "
           "bigger than 2nd node"),
    {success, Comp3} = ?MOD:check_comp(volume, Nodes2, Mbb14, Less),
    etap:is(Comp3, Node9, "Don't need to expand any node, when added to 2nd, "
           "within 2nd node"),
    {success, Comp4} = ?MOD:check_comp(volume, Nodes2, Mbb15, Less),
    etap:is(Comp4, Node10, "Don't need to expand any node, when added to 3rd, "
           "bigger than 3rd node"),

    Nodes3 = [Node16, Node17, Node18, Node19],
    {overlap, Comp5} = ?MOD:check_comp(volume, Nodes3, Mbb20, Less),
    {_, Overlaps5} = lists:unzip(Comp5),
    etap:is(Overlaps5, [Node16, Node18],
            "Cand doesn't contain all nodes"),

    {overlap, Comp1Perimeter} = ?MOD:check_comp(perimeter, Nodes1, Mbb6, Less),
    {Numbers1Perimeter, _} = lists:unzip(Comp1Perimeter),
    etap:isnt(Numbers1, Numbers1Perimeter,
              "Make sure check_comp triggers different code path for "
              "perimeter and volume calculations").


test_any_zero_volume() ->
    Less = fun(A, B) -> A < B end,

    Mbb1 = [{-38, 74.2}, {38, 948}, {-480, -27}, {-7, -4.28}, {84.3, 923.8}],
    Mbb2 = [{39, 938}, {-937, 8424}, {-1000, -82}, {4.72, 593}, {372, 490.3}],
    Mbb3 = [{48, 472}, {-9.38, 26.1}, {-382, -29}, {-1.4, 30}, {39.9, 100}],
    Mbb4 = [{48, 472}, {-9.38, 26.1}, {-29, -29}, {-1.4, 30}, {39.9, 100}],

    Mbb5 = [{43.5, 95.2}, {-83, 432}, {-8.6, -0.5}, {0, 582.2}, {50, 100}],
    Mbb6 = [{43.5, 95.2}, {-83, 432}, {-29, -29}, {0, 582.2}, {50, 100}],
    Mbb7 = [{43.5, 43.5}, {-83, 432}, {-8.6, -0.5}, {0, 582.2}, {50, 100}],

    Node1 = {Mbb1, 3487},
    Node2 = {Mbb2, 823},
    Node3 = {Mbb3, 96242},
    Node4 = {Mbb4, 6948},

    etap:is(?MOD:any_zero_volume([Node1, Node2, Node3], Mbb5, Less), false,
            "No zero volume node"),
    etap:is(?MOD:any_zero_volume([Node1, Node2, Node3], Mbb7, Less), false,
            "No zero volume node (new node has zero volume)"),
    etap:is(?MOD:any_zero_volume([Node1, Node2, Node4], Mbb5, Less), false,
            "No zero volume node (one existing node has zero volume)"),
    etap:is(?MOD:any_zero_volume([Node1, Node2, Node4], Mbb7, Less), false,
            "No zero volume node (new node and one existing node has "
            "zero volume)"),
    etap:is(?MOD:any_zero_volume([Node4, Node1, Node2, Node3], Mbb6, Less),
            true, "One zero volume node (first one)"),
    etap:is(?MOD:any_zero_volume([Node1, Node4, Node2, Node3], Mbb6, Less),
            true, "One zero volume node (a middle one)"),
    etap:is(?MOD:any_zero_volume([Node1, Node2, Node3, Node4], Mbb6, Less),
            true, "One zero volume node (last one)"),
    etap:is(?MOD:any_zero_volume([Node1, Node3, Node3, Node4], Mbb6, Less),
            true, "Two zero volume nodes").


test_calc_delta_perimeter() ->
    Less = fun(A, B) -> A < B end,

    Mbb1 = [{-380, -74.2}, {-380, 948}],
    Mbb2 = [{-280, -150}, {-7, 28.74}],

    Mbb3 = [{84.3, 923.6}, {39, 938}],

    etap:is(?MOD:calc_delta_perimeter(Mbb1, Mbb1, Less), 0,
            "No delta: equal nodes"),
    etap:is(?MOD:calc_delta_perimeter(Mbb1, Mbb2, Less), 0,
            "No delta: new node is within the original one"),
    etap:is(?MOD:calc_delta_perimeter(Mbb1, Mbb3, Less), 997.8,
            "Some delta"),
    etap:is(?MOD:calc_delta_perimeter(Mbb2, Mbb1, Less), 1468.06,
            "Some delta: new node enclosed original one").


test_calc_delta_common_overlap_perimeter() ->
    Less = fun(A, B) -> A < B end,

    Mbb1 = [{-380, -74.2}, {-380, 948}],
    Mbb2 = [{-580, -74.2}, {-280, 948}],
    Mbb3 = [{-100.6, 200.3}, {39, 538}],
    Mbb4 = [{50.4, 60.79}, {1000, 1001}],
    Mbb5 = [{-580, 100.8}, {-280.93, 1948}],

    etap:is(?MOD:calc_delta_common_overlap_perimeter(Mbb1, Mbb1, Mbb3, Less),
            0, "No delta perimeter: T didn't change"),
    etap:is(?MOD:calc_delta_common_overlap_perimeter(Mbb1, Mbb3, Mbb2, Less),
            0,
            "No delta perimeter: T did change, but not the intersection to J"),
    etap:is(?MOD:calc_delta_common_overlap_perimeter(Mbb1, Mbb4, Mbb2, Less),
            0, "No delta perimeter: T and J are overlap-free"),
    etap:is(?MOD:calc_delta_common_overlap_perimeter(Mbb1, Mbb3, Mbb5, Less),
            175.0,
            "Some delta perimeter: T and J are overlap-free, but NewNode "
            "makes them overlap"),
    etap:is(?MOD:calc_delta_common_overlap_perimeter(Mbb1, Mbb3, Mbb4, Less),
            134.99,
            "Some delta perimeter: T and J overlap, NewNode increases "
            "intersection").


test_calc_delta_common_overlap_volume() ->
    Less = fun(A, B) -> A < B end,

    Mbb1 = [{-380, -74.2}, {-380, 948}],
    Mbb2 = [{-580, -74.2}, {-280, 948}],
    Mbb3 = [{-100.6, 200.3}, {39, 538}],
    Mbb4 = [{50.4, 60.79}, {1000, 1001}],
    Mbb5 = [{-580, 100.8}, {-280.93, 1948}],

    etap:is(?MOD:calc_delta_common_overlap_volume(Mbb1, Mbb1, Mbb3, Less),
            0, "No delta volume: T didn't change"),
    etap:is(?MOD:calc_delta_common_overlap_volume(Mbb1, Mbb3, Mbb2, Less),
            0, "No delta volume: T did change, but not the intersection to J"),
    etap:is(?MOD:calc_delta_common_overlap_volume(Mbb1, Mbb4, Mbb2, Less),
            0, "No delta volume: T and J are overlap-free"),
    etap:is(?MOD:calc_delta_common_overlap_volume(Mbb1, Mbb3, Mbb5, Less),
            87325.0,
            "Some delta volume: T and J are overlap-free, but NewNode makes "
            "them overlap"),
    etap:is(?MOD:calc_delta_common_overlap_volume(Mbb1, Mbb3, Mbb4, Less),
            67360.01,
            "Some delta volume: T and J overlap, NewNode increases "
            "intersection").


test_min_size() ->
    Mbb1 = [{84.3, 923.8}, {39, 938}],

    Mbb2 = [{-937, 8424}, {-1000, -82}],
    Mbb3 = [{-380, -74.2}, {-380, 948}],
    Mbb4 = [{48, 472}, {-9.38, 26.1}],
    Mbb5 = [{4.72, 4.72}, {372, 49000.3}],
    Mbb6 = [{583.23, 584}, {765, 765}],

    Node1 = {Mbb1, a},
    Node2 = {Mbb2, b},
    Node3 = {Mbb3, c},
    Node4 = {Mbb4, d},
    Node5 = {Mbb5, e},
    Node6 = {Mbb6, f},

    etap:is(?MOD:min_size([Node1, Node2, Node3]), Node3,
            "min volume: three nodes"),
    etap:is(?MOD:min_size([Node1, Node2, Node3, Node4]), Node4,
            "min volume: four nodes"),
    etap:is(?MOD:min_size([Node1, Node5, Node4, Node2]), Node4,
            "min perimeter: four nodes (other node has zero volume, but "
            "bigger perimeter)"),
    etap:is(?MOD:min_size([Node1, Node5, Node4, Node6]), Node6,
            "min perimeter: two zero volume nodes, one with smallest "
            "perimeter").


test_within_mbb() ->
    Less = fun(A, B) -> A < B end,

    Mbb1 = [{-38, 74.2}, {38, 948}],
    Mbb2 = [{-37.3, 50}, {43, 428.74}],
    Mbb3 = [{-37.3, 74.2}, {43, 428.74}],
    Mbb4 = [{400.72, 593}, {-472, -390.3}],
    Mbb5 = [{4.72, 593}, {72, 390.3}],
    Mbb6 = [{-48, 84.2}, {28, 958}],
    Mbb7 = [{-48.4, -38}, {28, 958}],

    etap:is(?MOD:within_mbb(Mbb2, Mbb1, Less), true,
            "The MBB is completely within the other"),
    etap:is(?MOD:within_mbb(Mbb3, Mbb1, Less), true,
            "The MBB is completely within the other, but touches the other"),
    etap:is(?MOD:within_mbb(Mbb1, Mbb1, Less), true,
            "The MBB is the same as the other"),
    etap:is(?MOD:within_mbb(Mbb4, Mbb1, Less), false,
            "The MBB is complely outside the other"),
    etap:is(?MOD:within_mbb(Mbb5, Mbb1, Less), false,
            "The MBB intersects the other"),
    etap:is(?MOD:within_mbb(Mbb6, Mbb1, Less), false,
            "The MBB encloses the other"),
    etap:is(?MOD:within_mbb(Mbb7, Mbb1, Less), false,
            "The MBB is outside, but touches the other").
