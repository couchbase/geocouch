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

-define(MOD, vtree_util).

main(_) ->
    % Set the random seed once, for the whole test suite
    random:seed(1, 11, 91),

    code:add_pathz(filename:dirname(escript:script_name())),
    etap:plan(43),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            % Somehow etap:diag/1 and etap:bail/1 don't work properly
            %etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            %etap:bail(Other),
            io:format("Test died abnormally:~n~p~n", [Other])
    end,
    ok.

test() ->
    test_min(),
    test_max(),
    test_calc_perimeter(),
    test_calc_volume(),
    test_calc_mbb(),
    test_nodes_mbb(),
    test_intersect_mbb(),
    test_find_min_value(),
    test_within_mbb(),
    ok.

test_min() ->
    Less = fun(A, B) -> A < B end,

    etap:is(?MOD:min({-5, 2}, Less), -5, "min (a)"),
    etap:is(?MOD:min({2, -5}, Less), -5, "min (b)"),
    etap:is(?MOD:min({39.450, 70}, Less), 39.450, "min (c)"),
    etap:is(?MOD:min({70, 39.450}, Less), 39.450, "min (d)"),
    etap:is(?MOD:min({9572, 9572}, Less), 9572, "min (e)"),

    etap:is(-485, ?MOD:min([842, -84.29, -485, 8372, 294.93], Less),
            "min (f)").

test_max() ->
    Less = fun(A, B) -> A < B end,

    etap:is(?MOD:max({-5, 2}, Less), 2, "max (a)"),
    etap:is(?MOD:max({2, -5}, Less), 2, "max (b)"),
    etap:is(?MOD:max({39.450, 70}, Less), 70, "max (c)"),
    etap:is(?MOD:max({70, 39.450}, Less), 70, "max (d)"),
    etap:is(?MOD:max({9572, 9572}, Less), 9572, "max (e)"),

    etap:is(?MOD:max([842, -84.29, -485, 8372, 294.93], Less), 8372,
            "max (f)").


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


test_calc_mbb() ->
    Less = fun(A, B) -> A < B end,

    Mbb1 = [{-38, 74.2}, {38, 948}, {-27, -3480}, {-4.28, -7}, {84.4, 923.1}],
    Mbb2 = [{39, 938}, {-937, 8424}, {-1, 0}, {372, 490.2}, {593, 45982.72}],
    etap:is(?MOD:calc_mbb([Mbb1, Mbb2], Less),
            [{-38, 938}, {-937, 8424}, {-27, 0}, {-4.28, 490.2},
             {84.4, 45982.72}],
            "Combine two MBBs"),
    etap:is(?MOD:calc_mbb([Mbb1], Less), Mbb1,
            "Single MBB (nothing to combine)").


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


test_find_min_value() ->
    Pair1 = {a, 1},
    Pair2 = {b, 2},
    Pair3 = {c, 3},
    Pair4 = {d, 4},
    Pair5 = {e, 5},

    MinFun = fun({_Key, Value}) -> Value+3 end,
    etap:is(?MOD:find_min_value(MinFun, [Pair3, Pair4, Pair2, Pair1]),
            {4, Pair1}, "Last item has minimum value"),
    etap:is(?MOD:find_min_value(MinFun, [Pair1, Pair4, Pair2, Pair3]),
            {4, Pair1}, "First item has minimum value"),
    etap:is(?MOD:find_min_value(MinFun, [Pair5, Pair4, Pair2, Pair3]),
            {5, Pair2}, "Item in the middle has minimum value"),
    etap_exception:throws_ok(fun() -> ?MOD:find_min_value(MinFun, []) end,
                             function_clause, "Empty list throws error").


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
