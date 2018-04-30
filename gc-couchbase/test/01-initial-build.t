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

-include_lib("../include/couch_spatial.hrl").
-include_lib("couch_set_view/include/couch_set_view.hrl").

% from couch_db.hrl
-define(MIN_STR, <<>>).
-define(MAX_STR, <<255>>).


test_set_name() -> <<"couch_test_spatial_view_initial_build">>.
num_set_partitions() -> 4.
ddoc_id() -> <<"_design/test">>.
num_docs() -> 1024.  % keep it a multiple of num_set_partitions()


main(_) ->
    test_util:init_code_path(),

    etap:plan(2),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail(Other)
    end,
    %init:stop(),
    %receive after infinity -> ok end,
    ok.


test() ->
    spatial_test_util:start_server(test_set_name()),

    etap:diag("Testing inital build of spatial views"),

    test_spatial_query(),

    % A clean shutdown is not implemented yet, it will come in future commits
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    spatial_test_util:stop_server(),
    ok.


test_spatial_query() ->
    setup_test(),
    ok = configure_spatial_group(ddoc_id()),

    {ok, Rows} = (catch query_spatial_view(<<"test">>)),
    etap:is(length(Rows), num_docs(),
        "Got all view rows (" ++ integer_to_list(num_docs()) ++ ")"),
    verify_rows(Rows),

    shutdown_group(),
    ok.


verify_rows(Rows) ->
    DocList = lists:map(fun(Doc) ->
        {[{<<"meta">>, {[{<<"id">>, DocId}]}},
          {<<"json">>, {[{<<"value">>, Value} | _Rest]}}]} = Doc,
        {DocId,
         <<"\"val", (list_to_binary(integer_to_list(Value)))/binary, "\"">>}
    end, create_docs(1, num_docs())),

    RowsWithoutKey = [{DocId, Value} ||
        {Key, DocId, {_PartId, Value, nil}} <- Rows],
    etap:is(lists:sort(RowsWithoutKey), lists:sort(DocList),
            "Returned correct rows").


query_spatial_view(ViewName) ->
    etap:diag("Querying spatial view " ++ binary_to_list(ddoc_id()) ++ "/" ++
        binary_to_list(ViewName)),
    Req = #set_view_group_req{
        stale = false
    },
    {ok, View, Group, _} = spatial_view:get_spatial_view(
        test_set_name(), ddoc_id(), ViewName, Req),

    FoldFun = fun({{Key, DocId}, Value}, Acc) ->
        {ok, [{Key, DocId, Value} | Acc]}
    end,
    ViewArgs = #spatial_query_args{},

    {ok, _, Rows} = couch_set_view:fold(Group, View, FoldFun, [], ViewArgs),
    couch_set_view:release_group(Group),
    {ok, lists:reverse(Rows)}.


setup_test() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),

    DDoc = {[
        {<<"meta">>, {[{<<"id">>, ddoc_id()}]}},
        {<<"json">>, {[
            {<<"spatial">>, {[
                {<<"test">>, <<"function(doc, meta) { emit([[doc.min, doc.max], [doc.min2, doc.max2]], 'val'+doc.value); }">>}
            ]}}
        ]}}
    ]},
    populate_set(DDoc).


create_docs(From, To) ->
    rand:seed(exrop, {91, 1, 11}),
    lists:map(
        fun(I) ->
            RandomMin = rand:uniform(2000),
            RandomMax = RandomMin + rand:uniform(167),
            RandomMin2 = rand:uniform(1769),
            RandomMax2 = RandomMin2 + rand:uniform(132),
            {[
              {<<"meta">>, {[{<<"id">>, iolist_to_binary(["doc", integer_to_list(I)])}]}},
              {<<"json">>, {[
                             {<<"value">>, I},
                             {<<"min">>, RandomMin},
                             {<<"max">>, RandomMax},
                             {<<"min2">>, RandomMin2},
                             {<<"max2">>, RandomMax2}
                            ]}}
            ]}
        end,
        lists:seq(From, To)).


populate_set(DDoc) ->
    etap:diag("Populating the " ++ integer_to_list(num_set_partitions()) ++
        " databases with " ++ integer_to_list(num_docs()) ++ " documents"),
    ok = couch_set_view_test_util:update_ddoc(test_set_name(), DDoc),
    DocList = create_docs(1, num_docs()),
    ok = couch_set_view_test_util:populate_set_sequentially(
        test_set_name(),
        lists:seq(0, num_set_partitions() - 1),
        DocList).


configure_spatial_group(DDocId) ->
    etap:diag("Configuring spatial view group"),
    Params = #set_view_params{
        max_partitions = num_set_partitions(),
        active_partitions = lists:seq(0, num_set_partitions()-1),
        passive_partitions = [],
        use_replica_index = false
    },
    try
        ok = couch_set_view:define_group(
            spatial_view, test_set_name(), DDocId, Params)
    catch _:Error ->
        Error
    end.


% A clean shutdown is not implemented yet, it will come in future commits
shutdown_group() ->
    GroupPid = couch_set_view:get_group_pid(
        spatial_view, test_set_name(), ddoc_id(), prod),
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    MonRef = erlang:monitor(process, GroupPid),
    receive
    {'DOWN', MonRef, _, _, _} ->
        ok
    after 10000 ->
        etap:bail("Timeout waiting for group shutdown")
    end.
