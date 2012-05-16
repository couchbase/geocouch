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
    etap:plan(14),
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
    test_calc_mbb(),
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
