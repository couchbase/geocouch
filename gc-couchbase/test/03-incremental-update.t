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


test_set_name() -> <<"couch_test_spatial_view_update_build">>.
num_set_partitions() -> 4.
ddoc_id() -> <<"_design/test">>.
num_docs() -> 1024.  % keep it a multiple of num_set_partitions()


main(_) ->
    test_util:init_code_path(),

    etap:plan(14),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail(Other)
    end,
    init:stop(),
    receive after infinity -> ok end,
    ok.


test() ->
    spatial_test_util:start_server(test_set_name()),

    etap:diag("Testing incremental update of spatial views"),
    setup_test(),

    etap:diag("Initial build"),
    test_spatial_insert(1, num_docs()),

    etap:diag("Insert documents to the index"),
    test_spatial_insert(num_docs()+1, 2*num_docs()),

    etap:diag("Insert documents to the index again"),
    test_spatial_insert((2*num_docs())+1, 3*num_docs()),

    % The start of the docs must line up with number of partitions,
    % else tests will fail (the reason is the way the partitions are
    % populated for the test)
    etap:diag("Update existing documents"),
    test_spatial_update(num_docs()+1, num_docs() + (num_docs() div 4),
        3*num_docs(), 7),

    etap:diag("Update some other existing documents"),
    test_spatial_update(1, 2*num_docs(), 3*num_docs(), 8),

    etap:diag("Remove some documents"),
    test_spatial_delete(1, num_docs() div 2, 3*num_docs()),

    etap:diag("Remove some other documents"),
    test_spatial_delete(num_docs()+1, 2*num_docs(), 3*num_docs()),

    shutdown_group(),
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    spatial_test_util:stop_server(),
    ok.


test_spatial_insert(From, To) ->
    insert_docs(From, To),

    {ok, Rows} = (catch query_spatial_view(<<"test">>)),
    etap:is(length(Rows), To,
        "Got all view rows (" ++ integer_to_list(To) ++ ")"),
    verify_rows(Rows, To).

test_spatial_update(From, To, Total, AddToValue) ->
    setup_test(),
    insert_docs(1, Total),

    update_docs(From, To, AddToValue),

    {ok, Rows} = (catch query_spatial_view(<<"test">>)),
    etap:is(length(Rows), Total,
        "Got all view rows (" ++ integer_to_list(To) ++ ")"),
    verify_updated_rows(Rows, From, To, Total, AddToValue).

test_spatial_delete(From, To, Total) ->
    setup_test(),
    insert_docs(1, Total),

    delete_docs(From, To),

    {ok, Rows} = (catch query_spatial_view(<<"test">>)),
    ExpectedNumRows = Total-(To-From)-1,
    etap:is(length(Rows), ExpectedNumRows,
        "Got all view rows (" ++ integer_to_list(ExpectedNumRows) ++ ")"),
    verify_deleted_rows(Rows, From, To, Total).


verify_rows(Rows, To) ->
    DocList = create_docs_for_verification(1, To, 0),
    RowsWithoutKey = [{DocId, Value} || {Key, DocId, {_PartId, Value}} <- Rows],
    etap:is(lists:sort(RowsWithoutKey), lists:sort(DocList),
            "Returned correct rows").

verify_updated_rows(Rows, From, To, Total, AddToValue) ->
    DocList1 = create_docs_for_verification(1, From-1, 0),
    DocList2 = create_docs_for_verification(From, To, AddToValue),
    DocList3 = create_docs_for_verification(To+1, Total, 0),
    DocList = DocList1 ++ DocList2 ++ DocList3,
    RowsWithoutKey = [{DocId, Value} || {Key, DocId, {_PartId, Value}} <- Rows],
    etap:is(lists:sort(RowsWithoutKey), lists:sort(DocList),
            "Returned correct rows").

% Just check that the deleted rows are not ther
verify_deleted_rows(Rows, From, To, Total) ->
    DocList1 = create_docs_for_verification_docids(1, From-1),
    DocList2 = create_docs_for_verification_docids(To+1, Total),
    DocList = DocList1 ++ DocList2,
    RowsDocIds = [DocId || {Key, DocId, {_PartId, Value}} <- Rows],
    etap:is(lists:sort(RowsDocIds), lists:sort(DocList),
            "Returned correct rows").

create_docs_for_verification(From, To, AddToValue) ->
    lists:map(fun(Doc) ->
        {[{<<"meta">>, {[{<<"id">>, DocId}]}},
          {<<"json">>, {[{<<"value">>, Value} | _Rest]}}]} = Doc,
        {DocId,
         <<"\"val", (list_to_binary(integer_to_list(Value)))/binary, "\"">>}
    end, create_docs(From, To, AddToValue)).

create_docs_for_verification_docids(From, To) ->
    lists:map(fun(Doc) ->
        {[{<<"meta">>, {[{<<"id">>, DocId}]}}, _]} = Doc,
        DocId
    end, create_docs(From, To)).


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
    ok = couch_set_view_test_util:update_ddoc(test_set_name(), DDoc),
    ok = configure_spatial_group(ddoc_id()).

doc_id(I) ->
    iolist_to_binary(io_lib:format("doc_~8..0b", [I])).

create_docs(From, To) ->
    create_docs(From, To, 0).
create_docs(From, To, AddToValue) ->
    random:seed(91, 1, 11),
    lists:map(
        fun(I) ->
            RandomMin = random:uniform(2000),
            RandomMax = RandomMin + random:uniform(167),
            RandomMin2 = random:uniform(1769),
            RandomMax2 = RandomMin2 + random:uniform(132),
            {[
              {<<"meta">>, {[{<<"id">>, doc_id(I)}]}},
              {<<"json">>, {[
                             {<<"value">>, (I + AddToValue)},
                             {<<"min">>, RandomMin},
                             {<<"max">>, RandomMax},
                             {<<"min2">>, RandomMin2},
                             {<<"max2">>, RandomMax2}
                            ]}}
            ]}
        end,
        lists:seq(From, To)).


create_deleted_docs(From, To) ->
    lists:map(
        fun(I) ->
            {[
              {<<"meta">>, {[{<<"deleted">>, true},  {<<"id">>, doc_id(I)}]}},
              {<<"json">>, {[]}}
            ]}
        end,
        lists:seq(From, To)).


insert_docs(From, To) ->
    etap:diag("Populating the " ++ integer_to_list(num_set_partitions()) ++
        " databases with " ++ integer_to_list(To-From+1) ++ " documents"),
    DocList = create_docs(From, To),
    ok = couch_set_view_test_util:populate_set_alternated(
        test_set_name(),
        lists:seq(0, num_set_partitions() - 1),
        DocList).

update_docs(From, To, AddToValue) ->
    etap:diag("Update " ++ integer_to_list(To-From+1) ++ " documents" ++
        "in the " ++ integer_to_list(num_set_partitions()) ++ " databases"),
    DocList = create_docs(From, To, AddToValue),
    ok = couch_set_view_test_util:populate_set_alternated(
        test_set_name(),
        lists:seq(0, num_set_partitions() - 1),
        DocList).

delete_docs(From, To) ->
    etap:diag("Delete " ++ integer_to_list(To-From+1) ++ " documents" ++
        "in the " ++ integer_to_list(num_set_partitions()) ++ " databases"),
    DocList = create_deleted_docs(From, To),
    ok = couch_set_view_test_util:populate_set_alternated(
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
