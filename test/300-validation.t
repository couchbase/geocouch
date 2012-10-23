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

-record(user_ctx, {
    name = null,
    roles = [],
    handler
}).

-define(etap_match(Got, Expected, Desc),
        etap:fun_is(fun(XXXXXX) ->
            case XXXXXX of Expected -> true; _ -> false end

        end, Got, Desc)).


test_db_name() -> <<"geocouch_test_validation">>.
admin_user_ctx() -> {user_ctx, #user_ctx{roles = [<<"_admin">>]}}.

main(_) ->
    code:add_pathz(filename:dirname(escript:script_name())),
    gc_test_util:init_code_path(),
    etap:plan(17),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail(Other)
    end,
    ok.

test() ->
    ok = ssl:start(),
    ok = lhttpc:start(),
    GeoCouchConfig = filename:join(
        gc_test_util:root_dir() ++ [gc_test_util:gc_config_file()]),
    ConfigFiles = test_util:config_files() ++ [GeoCouchConfig],
    couch_server_sup:start_link(ConfigFiles),
    timer:sleep(1000),
    put(addr, couch_config:get("httpd", "bind_address", "127.0.0.1")),
    put(port, integer_to_list(mochiweb_socket_server:get(couch_httpd, port))),


    etap:diag("Testing spatial function with invalid syntax"),
    test_spatial_syntax_error(),

    etap:diag("Testing spatial function with invalid name"),
    test_spatial_name_error(),

    etap:diag("Testing spatial function with wrong value"),
    test_spatial_value_error(),

    ok.

test_spatial_syntax_error() ->
    delete_db(),
    create_db(),

    DDocId = <<"_design/syntax">>,
    DDoc = {[
        {<<"meta">>, {[{<<"id">>, DDocId}]}},
        {<<"json">>, {[
            {<<"language">>, <<"javascript">>},
            {<<"spatial">>, {[
                {<<"test">>, <<"function(doc, meta) { emit({\"type\": \"Point\", \"cooridinates\": [0, 1]}, 1); ">>}
            ]}}
        ]}}
    ]},
    Result = try
        add_design_doc(DDoc)
    catch throw:Error ->
        Error
    end,
    ?etap_match(Result, {invalid_design_doc, _}, "Design document creation got rejected"),
    {invalid_design_doc, Reason} = Result,
    etap:diag("Design document creation error reason: " ++ binary_to_list(Reason)),

    delete_db().


test_spatial_name_error() ->
    delete_db(),
    create_db(),

    ddoc_name_error(<<"  test  ">>),
    ddoc_name_error(<<"test ">>),
    ddoc_name_error(<<"   test ">>),
    ddoc_name_error(<<"test\t">>),
    ddoc_name_error(<<"\ttest\t">>),
    ddoc_name_error(<<"\t\ttest">>),
    ddoc_name_error(<<"\ntest">>),
    ddoc_name_error(<<"test\n\n\n">>),
    ddoc_name_error(<<"\n\ntest\n\n\n">>),
    ddoc_name_error(<<"test\r\r">>),
    ddoc_name_error(<<"\rtest">>),
    ddoc_name_error(<<"\r\rtest\r">>),
    ddoc_name_error(<<" \r\ntest">>),
    ddoc_name_error(<<"test\t ">>),
    ddoc_name_error(<<"\n  test\t ">>),

    delete_db().


ddoc_name_error(SpatialName) ->
    DDocId = <<"_design/name">>,
    DDoc = {[
        {<<"meta">>, {[{<<"id">>, DDocId}]}},
        {<<"json">>, {[
            {<<"language">>, <<"javascript">>},
            {<<"spatial">>, {[
                {SpatialName, <<"function(doc, meta) { emit({\"type\": \"Point\", \"cooridinates\": [0, 1]}, 1);}">>}
            ]}}
        ]}}
    ]},
    Result = try
        add_design_doc(DDoc)
    catch throw:Error ->
        Error
    end,
    ?etap_match(Result, {invalid_design_doc, _}, "Design document creation got rejected"),
    {invalid_design_doc, Reason} = Result,
    etap:diag("Design document creation error reason: " ++ binary_to_list(Reason)).

test_spatial_value_error() ->
    delete_db(),
    create_db(),

    DDocId = <<"_design/syntax">>,
    DDoc = {[
        {<<"meta">>, {[{<<"id">>, DDocId}]}},
        {<<"json">>, {[
            {<<"language">>, <<"javascript">>},
            {<<"spatial">>, {[
                {<<"test">>, {[]}}
            ]}}
        ]}}
    ]},
    Result = try
        add_design_doc(DDoc)
    catch throw:Error ->
        Error
    end,
    ?etap_match(Result, {invalid_design_doc, _}, "Design document creation got rejected"),
    {invalid_design_doc, Reason} = Result,
    etap:diag("Design document creation error reason: " ++ binary_to_list(Reason)),

    delete_db().


create_db() ->
    {ok, Db} = couch_db:create(test_db_name(), [admin_user_ctx()]),
    couch_db:close(Db),
    ok.

delete_db() ->
    couch_server:delete(test_db_name(), [admin_user_ctx()]).


add_design_doc(DDoc0) ->
    {ok, Db} = couch_db:open_int(test_db_name(), [admin_user_ctx()]),
    DDoc = couch_doc:from_json_obj(DDoc0),
    ok = couch_db:update_doc(Db, DDoc, []),
    {ok, _} = couch_db:ensure_full_commit(Db),
    couch_db:close(Db),
    ok.
