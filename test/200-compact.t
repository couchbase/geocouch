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

test_db1_name() -> <<"geocouch_test_compaction">>.
test_db2_name() -> <<"geocouch_test_compaction_foreign">>.
ddoc_name() -> <<"foo">>.
admin_user_ctx() -> {user_ctx, #user_ctx{roles = [<<"_admin">>]}}.

main(_) ->
    code:add_pathz(filename:dirname(escript:script_name())),
    gc_test_util:init_code_path(),
    etap:plan(6),
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

    delete_dbs(),
    create_dbs(),
    add_design_doc(test_db1_name()),

    test_compaction(),
    test_compaction_foreign(),

    delete_dbs(),
    couch_server_sup:stop(),
    ok.


test_compaction() ->
    DbName = test_db1_name(),
    DdocName = <<"_design/", (ddoc_name())/binary>>,

    insert(DbName),
    query_spatial(DbName),
    insert(DbName),
    query_spatial(DbName),
    insert(DbName),
    query_spatial(DbName),

    SpatialGroup = couch_spatial:get_group_server(DbName, DdocName),
    etap:is(is_pid(SpatialGroup), true, "got spatial group pid"),
    etap:is(is_process_alive(SpatialGroup), true,
        "spatial group pid is alive"),

    PreCompact = get_spatial_size(DbName),
    compact_spatial_group(DbName, ddoc_name()),
    PostCompact = get_spatial_size(DbName),

    etap:is(PostCompact < PreCompact, true, "spatial view got compacted"),
    ok.

% This test tests compaction with a Design Document which is not in the same
% database as the data
test_compaction_foreign() ->
    DbName = test_db2_name(),
    DdocName = <<"_design/", (ddoc_name())/binary>>,
    % Name of the database that holds the Design Document
    DdocDbName = test_db1_name(),

    insert(DbName),
    query_spatial(DbName, DdocDbName),
    insert(DbName),
    query_spatial(DbName, DdocDbName),
    insert(DbName),
    query_spatial(DbName, DdocDbName),


    SpatialGroup = couch_spatial:get_group_server(
        {DbName, DdocDbName}, DdocName),
    etap:is(is_pid(SpatialGroup), true, "got spatial group pid (b)"),
    etap:is(is_process_alive(SpatialGroup), true,
        "spatial group pid is alive (b)"),

    PreCompact = get_spatial_size({DbName, DdocDbName}),
    compact_spatial_group({DbName, DdocDbName}, ddoc_name()),
    PostCompact = get_spatial_size({DbName, DdocDbName}),

    etap:is(PostCompact < PreCompact, true, "spatial view got compacted (b)"),
    ok.


create_dbs() ->
    {ok, Db1} = couch_db:create(test_db1_name(), [admin_user_ctx()]),
    {ok, Db2} = couch_db:create(test_db2_name(), [admin_user_ctx()]),
    couch_db:close(Db1),
    couch_db:close(Db2),
    ok.

delete_dbs() ->
    couch_server:delete(test_db1_name(), [admin_user_ctx()]),
    couch_server:delete(test_db2_name(), [admin_user_ctx()]).


compact_spatial_group(DbName, DdocName) ->
    ok = couch_spatial_compactor:start_compact(DbName, DdocName),
    wait_compaction_finished(DbName).


add_design_doc(DbName) ->
    {ok, Db} = couch_db:open_int(DbName, [admin_user_ctx()]),
    DDoc = couch_doc:from_json_obj({[
        {<<"meta">>, {[
            {<<"id">>, <<"_design/foo">>}]}},
        {<<"json">>, {[
            {<<"language">>, <<"javascript">>},
            {<<"spatial">>, {[
                {<<"foo">>, <<"function(doc) { emit({type: \"Point\", coordinates: [0,0]}, doc); }">>}
            ]}}
        ]}}
    ]}),
    ok = couch_db:update_docs(Db, [DDoc]),
    {ok, _} = couch_db:ensure_full_commit(Db),
    couch_db:close(Db),
    ok.


% Inserts documents and queries the spatial view
insert(DbName) ->
    {ok, Db} = couch_db:open_int(DbName, [admin_user_ctx()]),
    _Docs = lists:map(
        fun(_) ->
            Doc = couch_doc:from_json_obj(
                {[{<<"meta">>, {[{<<"id">>, couch_uuids:new()}]}}]}),
            ok = couch_db:update_docs(Db, [Doc])
        end,
        lists:seq(1, 100)),
    couch_db:close(Db),
    ok.

query_spatial(DbName) ->
    {ok, Db} = couch_db:open_int(DbName, [admin_user_ctx()]),
    DdocName = <<"_design/", (ddoc_name())/binary>>,
    % Don't use the HTTP API, as it doesn't support foreig Design
    % Documents (those are Design Documents that are not in the same
    % database as the data is).
    {ok, _Index, _Group} = couch_spatial:get_spatial_index(
        Db, DdocName, ddoc_name(), nil),
    couch_db:close(Db).

query_spatial(DbName, DdocDbName) ->
    {ok, Db} = couch_db:open_int(DbName, [admin_user_ctx()]),
    {ok, DdocDb} = couch_db:open_int(DdocDbName, [admin_user_ctx()]),
    DdocName = <<"_design/", (ddoc_name())/binary>>,
    % Don't use the HTTP API, as it doesn't support foreig Design
    % Documents (those are Design Documents that are not in the same
    % database as the data is).
    {ok, _Index, _Group} = couch_spatial:get_spatial_index(
        Db, {DdocDb, DdocName}, ddoc_name(), nil),
    couch_db:close(Db),
    couch_db:close(DdocDb).


wait_compaction_finished(DbName) ->
    Parent = self(),
    Loop = spawn_link(fun() -> wait_loop(Parent, DbName) end),
    receive
    {done, Loop} ->
        etap:diag("Spatial compaction has finished")
    after 60000 ->
        etap:bail("Compaction not triggered")
    end.

wait_loop(Parent, DbName) ->
    DdocName = <<"_design/", (ddoc_name())/binary>>,
    {ok, SpatialInfo} = couch_spatial:get_group_info(DbName, DdocName),
    case couch_util:get_value(compact_running, SpatialInfo) =:= true of
    false ->
        Parent ! {done, self()};
    true ->
        ok = timer:sleep(500),
        wait_loop(Parent, DbName)
    end.


get_spatial_size(DbName) ->
    DdocName = <<"_design/", (ddoc_name())/binary>>,
    {ok, SpatialInfo} = couch_spatial:get_group_info(DbName, DdocName),
    couch_util:get_value(disk_size, SpatialInfo).
