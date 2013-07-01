#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable

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

main(_) ->
    test_util:init_code_path(),

    etap:plan(1),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail(Other)
    end,
    ok.


test() ->
    % Purpose of this test is to create all system databases (_users, _replicator)
    % before we start running all other tests in parallel. When the other tests start
    % in parallel, if the system databases don't exist, they will all attempt to create
    % them, and 1 succeeds while others will fail.
    couch_set_view_test_util:start_server(),
    {ok, RepDb} = couch_db:open_int(<<"_replicator">>, []),
    {ok, UsersDb} = couch_db:open_int(<<"_users">>, []),
    {ok, _} = couch_db:ensure_full_commit(RepDb),
    {ok, _} = couch_db:ensure_full_commit(UsersDb),
    ok = couch_db:close(RepDb),
    ok = couch_db:close(UsersDb),
    etap:is(true, true, "Preparation for parallel testing done"),
    couch_set_view_test_util:stop_server(),
    ok.
