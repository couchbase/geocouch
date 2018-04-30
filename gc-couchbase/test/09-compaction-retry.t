#!/usr/bin/env escript
%% -*- Mode: Erlang; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

-define(JSON_ENCODE(V), ejson:encode(V)). % couch_db.hrl
-define(MAX_WAIT_TIME, 900 * 1000).

-include_lib("couch_set_view/include/couch_set_view.hrl").

test_set_name() -> <<"couch_test_spatial_index_compaction_retry">>.
num_set_partitions() -> 64.
ddoc_id() -> <<"_design/test">>.
num_docs_0() -> 5056.

main(_) ->
    test_util:init_code_path(),

    etap:plan(52),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail(Other)
    end,
%    init:stop(),
%    receive after infinity -> ok end,
    ok.


test() ->
    spatial_test_util:start_server(test_set_name()),

    couch_set_view_test_util:delete_set_dbs(
        test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(
        test_set_name(), num_set_partitions()),

    create_set(),
    GroupPid = couch_set_view:get_group_pid(
        spatial_view, test_set_name(), ddoc_id(), prod),

    ValueGenFun1 = fun(I) -> I end,
    update_documents(0, num_docs_0(), ValueGenFun1),

    % build index
    _ = get_group_snapshot(),

    verify_btrees(ValueGenFun1, num_docs_0()),

    compact_add_new_docs(128, ValueGenFun1),

    verify_btrees(ValueGenFun1, num_docs_0() + 128),
    ValueGenFun2 = fun(I) -> I * 3 end,
    compact_update_docs(num_docs_0() + 128, ValueGenFun2),

    verify_btrees(ValueGenFun2, num_docs_0() + 128),

    % delete last N docs
    compact_delete_docs(num_docs_0() + 128, 64),

    verify_btrees(ValueGenFun2, num_docs_0() + 64),

    etap:diag("Testing 2 compaction retries work"),
    ValueGenFun3 = fun(I) -> I * 5 end,
    ValueGenFun4 = fun(I) -> I * 11 end,
    compact_2_retries_update_docs(num_docs_0() + 64, ValueGenFun3,
        ValueGenFun4),

    verify_btrees(ValueGenFun4, num_docs_0() + 64),

    etap:diag("Testing compaction start after updater started"),
    ValueGenFun5 = fun(I) -> I * 22 end,
    test_start_compactor_after_updater(ValueGenFun5, num_docs_0() + 64),

    verify_btrees(ValueGenFun5, num_docs_0() + 64),

    etap:diag("Testing compaction retry phase with multiple "
              "updates during initial phase"),
    ValueGenFun6 = fun(I) -> I * 39 end,
    ValueGenFun7 = fun(I) -> I * 45 end,
    % Simulates MB-8357
    test_retry_multiple_updates_during_initial_phase(
        ValueGenFun6, ValueGenFun7, num_docs_0() + 64),
    verify_btrees(ValueGenFun7, num_docs_0() + 64),

    couch_util:shutdown_sync(GroupPid),
    couch_set_view_test_util:delete_set_dbs(
        test_set_name(), num_set_partitions()),
    ok = timer:sleep(1000),
    couch_set_view_test_util:stop_server(),
    ok.


compact_add_new_docs(NewDocCount, ValueGenFun) ->
    {ok, Pid} = couch_set_view_compactor:start_compact(
        spatial_view, test_set_name(), ddoc_id(), main),
    Ref = erlang:monitor(process, Pid),
    Pid ! pause,
    receive
    {'DOWN', Ref, process, Pid, _Reason} ->
        etap:bail("Compaction finished before we could add new documents and "
            "trigger index update")
    after 0 ->
        ok
    end,
    update_documents(num_docs_0(), NewDocCount, ValueGenFun),
    % trigger index update
    _ = get_group_snapshot(),
    Pid ! unpause,
    receive
    {'DOWN', Ref, process, Pid, normal} ->
        ok;
    {'DOWN', Ref, process, Pid, Reason} ->
        etap:bail("Compactor died with reason: " ++ couch_util:to_list(Reason))
    after ?MAX_WAIT_TIME ->
        etap:bail("Timeout waiting for compaction to finish")
    end,
    ok.


compact_update_docs(DocCount, ValueGenFun) ->
    {ok, Pid} = couch_set_view_compactor:start_compact(
        spatial_view, test_set_name(), ddoc_id(), main),
    Ref = erlang:monitor(process, Pid),
    Pid ! pause,
    receive
    {'DOWN', Ref, process, Pid, _Reason} ->
        etap:bail("Compaction finished before we could update documents and "
            "trigger index update")
    after 0 ->
        ok
    end,
    update_documents(0, DocCount, ValueGenFun),
    % trigger index update
    _ = get_group_snapshot(),
    Pid ! unpause,
    receive
    {'DOWN', Ref, process, Pid, normal} ->
        ok;
    {'DOWN', Ref, process, Pid, Reason} ->
        etap:bail("Compactor died with reason: " ++ couch_util:to_list(Reason))
    after ?MAX_WAIT_TIME ->
        etap:bail("Timeout waiting for compaction to finish")
    end,
    ok.


compact_delete_docs(TotalDocCount, ToDeleteCount) ->
    {ok, Pid} = couch_set_view_compactor:start_compact(
        spatial_view, test_set_name(), ddoc_id(), main),
    Ref = erlang:monitor(process, Pid),
    Pid ! pause,
    receive
    {'DOWN', Ref, process, Pid, _Reason} ->
        etap:bail("Compaction finished before we could delete documents and "
            "trigger index update")
    after 0 ->
        ok
    end,
    delete_docs(TotalDocCount - ToDeleteCount, ToDeleteCount),
    % trigger index update
    _ = get_group_snapshot(),
    Pid ! unpause,
    receive
    {'DOWN', Ref, process, Pid, normal} ->
        ok;
    {'DOWN', Ref, process, Pid, Reason} ->
        etap:bail("Compactor died with reason: " ++ couch_util:to_list(Reason))
    after ?MAX_WAIT_TIME ->
        etap:bail("Timeout waiting for compaction to finish")
    end,
    ok.


compact_2_retries_update_docs(DocCount, ValueGenFun1, ValueGenFun2) ->
    {ok, Pid} = couch_set_view_compactor:start_compact(
        spatial_view, test_set_name(), ddoc_id(), main),
    Ref = erlang:monitor(process, Pid),
    Pid ! pause,
    receive
    {'DOWN', Ref, process, Pid, _Reason} ->
        etap:bail("Compaction finished before we could update documents and "
            "trigger index update")
    after 0 ->
        ok
    end,
    update_documents(0, DocCount, ValueGenFun1),
    % trigger index update
    _ = get_group_snapshot(),
    Pid ! unpause,
    Pid ! pause,
    receive
    {'DOWN', Ref, process, Pid, _Reason2} ->
        etap:bail("Compaction retry finished before we could update documents "
            "and trigger index update")
    after 0 ->
        ok
    end,
    update_documents(0, DocCount, ValueGenFun2),
    % trigger index update
    _ = get_group_snapshot(),
    Pid ! unpause,
    receive
    {'DOWN', Ref, process, Pid, normal} ->
        ok;
    {'DOWN', Ref, process, Pid, Reason} ->
        etap:bail("Compactor died with reason: " ++ couch_util:to_list(Reason))
    after ?MAX_WAIT_TIME ->
        etap:bail("Timeout waiting for compaction to finish")
    end,
    ok.


test_start_compactor_after_updater(ValueGenFun, DocCount) ->
    update_documents(0, DocCount, ValueGenFun),
    GroupPid = couch_set_view:get_group_pid(
        spatial_view, test_set_name(), ddoc_id(), prod),
    {ok, UpPid} = gen_server:call(GroupPid, {start_updater, []}, infinity),
    case is_pid(UpPid) of
    true ->
        ok;
    false ->
        etap:bail("Updater not started")
    end,
    {ok, CompactorPid} = couch_set_view_compactor:start_compact(
        spatial_view, test_set_name(), ddoc_id(), main),
    CompactorPid ! pause,
    UpRef = erlang:monitor(process, UpPid),
    receive
    {'DOWN', UpRef, process, UpPid, noproc} ->
        etap:bail("Updater died before compactor started")
    after 0 ->
        ok
    end,
    receive
    {'DOWN', UpRef, process, UpPid, {updater_finished, _}} ->
        ok;
    {'DOWN', UpRef, process, UpPid, Reason} ->
        etap:bail("Updater died with unexpected reason: " ++
            couch_util:to_list(Reason))
    after ?MAX_WAIT_TIME ->
        etap:bail("Timeout waiting for updater to finish")
    end,
    etap:is(is_process_alive(CompactorPid), true,
        "Compactor is still running"),
    CompactorRef = erlang:monitor(process, CompactorPid),
    CompactorPid ! unpause,
    receive
    {'DOWN', CompactorRef, process, CompactorPid, normal} ->
        ok;
    {'DOWN', CompactorRef, process, CompactorPid, Reason2} ->
        etap:bail("Compactor died with unexpected reason: " ++
            couch_util:to_list(Reason2))
    after ?MAX_WAIT_TIME ->
        etap:bail("Timeout waiting for compactor to finish")
    end,
    ok.


% Simulates MB-8357
test_retry_multiple_updates_during_initial_phase(ValueGenFun1, ValueGenFun2,
        DocCount) ->
    GroupPid = couch_set_view:get_group_pid(
        spatial_view, test_set_name(), ddoc_id(), prod),
    {ok, CompactorPid} = couch_set_view_compactor:start_compact(
        spatial_view, test_set_name(), ddoc_id(), main),
    CompactorPid ! pause,
    CompactorRef = erlang:monitor(process, CompactorPid),
    receive
    {'DOWN', CompactorRef, process, CompactorPid, noproc} ->
        etap:bail("Compactor finished too soon")
    after 0 ->
        ok
    end,

    update_documents(0, DocCount, ValueGenFun1),
    {ok, UpPid1} = gen_server:call(GroupPid, {start_updater, []}, infinity),
    case is_pid(UpPid1) of
    true ->
        ok;
    false ->
        etap:bail("Updater not started")
    end,
    UpRef1 = erlang:monitor(process, UpPid1),
    receive
    {'DOWN', UpRef1, process, UpPid1, {updater_finished, _}} ->
        ok;
    {'DOWN', UpRef1, process, UpPid1, Reason1} ->
        etap:bail("Updater died with unexpected reason: " ++
            couch_util:to_list(Reason1))
    after ?MAX_WAIT_TIME ->
        etap:bail("Timeout waiting for updater to finish")
    end,
    etap:is(is_process_alive(CompactorPid), true,
        "Compactor is still running"),

    update_documents(0, DocCount, ValueGenFun2),
    {ok, UpPid2} = gen_server:call(GroupPid, {start_updater, []}, infinity),
    case is_pid(UpPid2) of
    true ->
        ok;
    false ->
        etap:bail("Updater not started")
    end,
    UpRef2 = erlang:monitor(process, UpPid2),
    receive
    {'DOWN', UpRef2, process, UpPid2, {updater_finished, _}} ->
        ok;
    {'DOWN', UpRef2, process, UpPid2, Reason2} ->
        etap:bail("Updater died with unexpected reason: " ++
            couch_util:to_list(Reason2))
    after ?MAX_WAIT_TIME ->
        etap:bail("Timeout waiting for updater to finish")
    end,
    etap:is(is_process_alive(CompactorPid), true,
        "Compactor is still running"),

    CompactorPid ! unpause,
    receive
    {'DOWN', CompactorRef, process, CompactorPid, normal} ->
        ok;
    {'DOWN', CompactorRef, process, CompactorPid, Reason3} ->
        etap:bail("Compactor died with unexpected reason: " ++
            couch_util:to_list(Reason3))
    after ?MAX_WAIT_TIME ->
        etap:bail("Timeout waiting for compactor to finish")
    end,
    ok.


get_group_snapshot() ->
    GroupPid = couch_set_view:get_group_pid(
        spatial_view, test_set_name(), ddoc_id(), prod),
    {ok, Group, 0} = gen_server:call(
        GroupPid, #set_view_group_req{stale = false, debug = true}, infinity),
    Group.


create_set() ->
    couch_set_view_test_util:delete_set_dbs(
        test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(
        test_set_name(), num_set_partitions()),
    couch_set_view:cleanup_index_files(spatial_view, test_set_name()),
    etap:diag("Creating the set databases (# of partitions: " ++
        integer_to_list(num_set_partitions()) ++ ")"),
    DDoc = {[
        {<<"meta">>, {[{<<"id">>, ddoc_id()}]}},
        {<<"json">>, {[
            {<<"spatial">>, {[
                {<<"test">>, <<"function(doc, meta) { "
                    "emit([[doc.min, doc.max], [doc.min*2, doc.max*3]], "
                    "'val'+doc.value); }">>}
            ]}}
        ]}}
    ]},
    ok = couch_set_view_test_util:update_ddoc(test_set_name(), DDoc),
    etap:diag("Configuring set view with partitions [0 .. 63] as active"),
    Params = #set_view_params{
        max_partitions = num_set_partitions(),
        active_partitions = lists:seq(0, 63),
        passive_partitions = [],
        use_replica_index = false
    },
    ok = couch_set_view:define_group(
        spatial_view, test_set_name(), ddoc_id(), Params).


update_documents(StartId, Count, ValueGenFun) ->
    etap:diag("Updating " ++ integer_to_list(Count) ++ " documents"),
    rand:seed(exrop, {91, 1, 11}),
    DocList0 = lists:map(
        fun(I) ->
            Value = ValueGenFun(I),
            {I rem num_set_partitions(), {[
                {<<"meta">>, {[{<<"id">>, doc_id(I)}]}},
                {<<"json">>, {[
                    {<<"value">>, Value},
                    {<<"min">>, Value},
                    {<<"max">>, Value + 10}
                ]}}
            ]}}
        end,
        lists:seq(StartId, StartId + Count - 1)),
    DocList = [Doc || {_, Doc} <- lists:keysort(1, DocList0)],
    ok = couch_set_view_test_util:populate_set_sequentially(
        test_set_name(),
        lists:seq(0, num_set_partitions() - 1),
        DocList).


delete_docs(StartId, NumDocs) ->
    Dbs = dict:from_list(lists:map(
        fun(I) ->
            {ok, Db} =
                couch_set_view_test_util:open_set_db(test_set_name(), I),
            {I, Db}
        end,
        lists:seq(0, 63))),
    Docs = lists:foldl(
        fun(I, Acc) ->
            Doc = couch_doc:from_json_obj({[
                {<<"meta">>, {[{<<"deleted">>, true},{<<"id">>, doc_id(I)}]}},
                {<<"json">>, {[]}}
            ]}),
            DocList = case orddict:find(I rem 64, Acc) of
            {ok, L} ->
                L;
            error ->
                []
            end,
            orddict:store(I rem 64, [Doc | DocList], Acc)
        end,
        orddict:new(), lists:seq(StartId, StartId + NumDocs - 1)),
    [] = orddict:fold(
        fun(I, DocList, Acc) ->
            Db = dict:fetch(I, Dbs),
            etap:diag("Deleting " ++ integer_to_list(length(DocList)) ++
                " documents from partition " ++ integer_to_list(I)),
            ok = couch_db:update_docs(Db, DocList, [sort_docs]),
            Acc
        end,
        [], Docs),
    ok = lists:foreach(fun({_, Db}) ->
        ok = couch_db:close(Db)
    end, dict:to_list(Dbs)).


doc_id(I) ->
    iolist_to_binary(io_lib:format("doc_~8..0b", [I])).


verify_btrees(ValueGenFun, NumDocs) ->
    Group = get_group_snapshot(),
    #set_view_group{
        id_btree = IdBtree,
        views = [View],
        index_header = #set_view_index_header{
            seqs = HeaderUpdateSeqs,
            abitmask = Abitmask,
            pbitmask = Pbitmask,
            cbitmask = Cbitmask
        }
    } = Group,
    #set_view{
        indexer = #spatial_view{
            vtree = Vt
        }
    } = View,
    ActiveParts = lists:seq(0, num_set_partitions() - 1),
    ExpectedBitmask = couch_set_view_util:build_bitmask(ActiveParts),
    DbSeqs = couch_set_view_test_util:get_db_seqs(
        test_set_name(), ActiveParts),
    ExpectedKVCount = NumDocs,

    etap:is(
        couch_set_view_test_util:full_reduce_id_btree(Group, IdBtree),
        {ok, {ExpectedKVCount, ExpectedBitmask}},
        "Id Btree has the right reduce value"),

    etap:is(HeaderUpdateSeqs, DbSeqs, "Header has right update seqs list"),
    etap:is(Abitmask, ExpectedBitmask, "Header has right active bitmask"),
    etap:is(Pbitmask, 0, "Header has right passive bitmask"),
    etap:is(Cbitmask, 0, "Header has right cleanup bitmask"),

    etap:diag("Verifying the Id Btree"),
    MaxPerPart = NumDocs div num_set_partitions(),
    {ok, _, {_, _, _, IdBtreeFoldResult}} = spatial_test_util:fold_id_btree(
        IdBtree,
        fun(Kv, _, {P0, I0, C0, It}) ->
            case C0 >= MaxPerPart of
            true ->
                P = P0 + 1,
                I = P,
                C = 1;
            false ->
                P = P0,
                I = I0,
                C = C0 + 1
            end,
            true = (P < num_set_partitions()),
            DocId = doc_id(I),
            Value = ValueGenFun(I),
            StoredValue = [
                 {View#set_view.id_num,
                     [{Value, Value+10}, {Value*2, (Value+10)*3}]}
            ],
            ExpectedKv = {<<P:16, DocId/binary>>, {P, StoredValue}},
            case ExpectedKv == Kv of
            true ->
                ok;
            false ->
                etap:bail("Id Btree has an unexpected KV at iteration " ++
                              integer_to_list(It))
            end,
            {ok, {P, I + num_set_partitions(), C, It + 1}}
        end,
        {0, 0, 0, 0}, []),
    etap:is(IdBtreeFoldResult, ExpectedKVCount,
        "Id Btree has " ++ integer_to_list(ExpectedKVCount) ++ " entries"),
    etap:diag("Verifying the Spatial View"),
    VtreeCount = vtree_search:count_all(Vt),
    etap:is(VtreeCount, ExpectedKVCount,
        "Spatial View has " ++ integer_to_list(ExpectedKVCount) ++ " entries"),
    ok.
