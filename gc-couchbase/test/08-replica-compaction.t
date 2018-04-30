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

-define(MAX_WAIT_TIME, 600 * 1000).

-include_lib("couch_set_view/include/couch_set_view.hrl").

test_set_name() -> <<"couch_test_spatial_index_replica_compact">>.
num_set_partitions() -> 64.
ddoc_id() -> <<"_design/test">>.
num_docs() -> 24128.


main(_) ->
    test_util:init_code_path(),

    etap:plan(26),
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

    couch_set_view_test_util:delete_set_dbs(
        test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(
        test_set_name(), num_set_partitions()),

    populate_set(),

    etap:diag("Marking partitions [ 8 .. 63 ] as replicas"),
    ok = couch_set_view:add_replica_partitions(
        spatial_view, test_set_name(), ddoc_id(), lists:seq(8, 63)),

    verify_group_info_before_replica_removal(),
    wait_for_replica_full_update(),
    verify_group_info_before_replica_removal(),

    etap:diag("Removing partitions [ 8 .. 63 ] from replica set"),
    ok = couch_set_view:remove_replica_partitions(
        spatial_view, test_set_name(), ddoc_id(), lists:seq(8, 63)),
    verify_group_info_after_replica_removal(),

    DiskSizeBefore = replica_index_disk_size(),

    {MainGroupBefore, RepGroupBefore} = get_group_snapshots(),

    etap:diag("Trigerring replica group compaction"),
    {ok, CompactPid} = couch_set_view_compactor:start_compact(
        spatial_view, test_set_name(), ddoc_id(), replica),
    Ref = erlang:monitor(process, CompactPid),
    etap:diag("Waiting for replica group compaction to finish"),
    receive
    {'DOWN', Ref, process, CompactPid, normal} ->
        ok;
    {'DOWN', Ref, process, CompactPid, noproc} ->
        ok;
    {'DOWN', Ref, process, CompactPid, Reason} ->
        etap:bail("Failure compacting replica group: " ++
            couch_util:to_list(Reason))
    after ?MAX_WAIT_TIME ->
        etap:bail("Timeout waiting for replica group compaction to finish")
    end,

    {MainGroupAfter, RepGroupAfter} = get_group_snapshots(),

    etap:is(
        MainGroupAfter#set_view_group.ref_counter,
        MainGroupBefore#set_view_group.ref_counter,
        "Same ref counter for main group after replica compaction"),
    etap:is(
        MainGroupAfter#set_view_group.fd,
        MainGroupBefore#set_view_group.fd,
        "Same fd for main group after replica compaction"),

    etap:is(
        is_process_alive(MainGroupBefore#set_view_group.ref_counter),
        true,
        "Main group's ref counter still alive"),
    etap:is(
        is_process_alive(MainGroupBefore#set_view_group.fd),
        true,
        "Main group's fd still alive"),

    etap:is(
        couch_ref_counter:count(MainGroupAfter#set_view_group.ref_counter),
        1,
        "Main group's ref counter count is 1"),

    etap:isnt(
        RepGroupAfter#set_view_group.ref_counter,
        RepGroupBefore#set_view_group.ref_counter,
        "Different ref counter for replica group after replica compaction"),
    etap:isnt(
        RepGroupAfter#set_view_group.fd,
        RepGroupBefore#set_view_group.fd,
        "Different fd for replica group after replica compaction"),

    etap:is(
        is_process_alive(RepGroupBefore#set_view_group.ref_counter),
        false,
        "Old replica group ref counter is dead"),

    etap:is(
        is_process_alive(RepGroupBefore#set_view_group.fd),
        false,
        "Old replica group fd is dead"),

    etap:is(
        couch_ref_counter:count(RepGroupAfter#set_view_group.ref_counter),
        1,
        "Replica group's new ref counter count is 1"),

    RepGroupInfo = get_replica_group_info(),
    {Stats} = couch_util:get_value(stats, RepGroupInfo),
    etap:is(couch_util:get_value(compactions, Stats), 1,
        "Replica had 1 full compaction in stats"),
    etap:is(couch_util:get_value(cleanups, Stats), 1,
        "Replica had 1 full cleanup in stats"),
    verify_group_info_after_replica_compact(),

    DiskSizeAfter = replica_index_disk_size(),
    etap:is(DiskSizeAfter < DiskSizeBefore, true,
        "Index file size is smaller after compaction"),

    couch_set_view_test_util:delete_set_dbs(
        test_set_name(), num_set_partitions()),
    ok = timer:sleep(1000),
    couch_set_view_test_util:stop_server(),
    ok.


get_group_snapshots() ->
    GroupPid = couch_set_view:get_group_pid(
        spatial_view, test_set_name(), ddoc_id(), prod),
    {ok, MainGroup, 0} = gen_server:call(
        GroupPid,
        #set_view_group_req{stale = false, debug = true},
        infinity),
    {ok, RepGroup, 0} = gen_server:call(
        MainGroup#set_view_group.replica_pid,
        #set_view_group_req{stale = false, debug = true},
        infinity),
    couch_ref_counter:drop(MainGroup#set_view_group.ref_counter),
    couch_ref_counter:drop(RepGroup#set_view_group.ref_counter),
    {MainGroup, RepGroup}.


verify_group_info_before_replica_removal() ->
    etap:diag(
        "Verifying replica group info before removing replica partitions"),
    RepGroupInfo = get_replica_group_info(),
    etap:is(
        couch_util:get_value(active_partitions, RepGroupInfo),
        [],
        "Replica group has [ ] as active partitions"),
    etap:is(
        couch_util:get_value(passive_partitions, RepGroupInfo),
        lists:seq(8, 63),
        "Replica group has [ 8 .. 63 ] as passive partitions"),
    etap:is(
        couch_util:get_value(cleanup_partitions, RepGroupInfo),
        [],
        "Replica group has [ ] as cleanup partitions").


verify_group_info_after_replica_removal() ->
    etap:diag(
        "Verifying replica group info after removing replica partitions"),
    RepGroupInfo = get_replica_group_info(),
    etap:is(
        couch_util:get_value(active_partitions, RepGroupInfo),
        [],
        "Replica group has [ ] as active partitions"),
    etap:is(
        couch_util:get_value(passive_partitions, RepGroupInfo),
        [],
        "Replica group has [ ] as passive partitions"),
    CleanupParts = couch_util:get_value(cleanup_partitions, RepGroupInfo),
    {Stats} = couch_util:get_value(stats, RepGroupInfo),
    CleanupHist = couch_util:get_value(cleanup_history, Stats),
    case length(CleanupHist) > 0 of
    true ->
        etap:is(
            length(CleanupParts),
            0,
            "Replica group has a right value for cleanup partitions");
    false ->
        etap:is(
            length(CleanupParts) > 0,
            true,
           "Replica group has a right value for cleanup partitions")
    end,
    etap:is(
        ordsets:intersection(CleanupParts, lists:seq(0, 7)),
        [],
        "Replica group doesn't have any cleanup partition with ID in "
        "[ 0 .. 7 ]").


verify_group_info_after_replica_compact() ->
    etap:diag("Verifying replica group info after compaction"),
    RepGroupInfo = get_replica_group_info(),
    etap:is(
        couch_util:get_value(active_partitions, RepGroupInfo),
        [],
        "Replica group has [ ] as active partitions"),
    etap:is(
        couch_util:get_value(passive_partitions, RepGroupInfo),
        [],
        "Replica group has [ ] as passive partitions"),
    etap:is(
        couch_util:get_value(cleanup_partitions, RepGroupInfo),
        [],
        "Replica group has [ ] as cleanup partitions").


wait_for_replica_full_update() ->
    etap:diag("Waiting for a full replica group update"),
    UpdateCountBefore = get_replica_updates_count(),
    MainGroupPid = couch_set_view:get_group_pid(
        spatial_view, test_set_name(), ddoc_id(), prod),
    {ok, ReplicaGroupPid} = gen_server:call(
        MainGroupPid, replica_pid, infinity),
    {ok, UpPid} = gen_server:call(
        ReplicaGroupPid, {start_updater, []}, infinity),
    case is_pid(UpPid) of
    true ->
        ok;
    false ->
        etap:bail("Updater was not triggered")
    end,
    Ref = erlang:monitor(process, UpPid),
    receive
    {'DOWN', Ref, process, UpPid, {updater_finished, _}} ->
        ok;
    {'DOWN', Ref, process, UpPid, noproc} ->
        ok;
    {'DOWN', Ref, process, UpPid, Reason} ->
        etap:bail(
            "Failure updating replica group: " ++ couch_util:to_list(Reason))
    after ?MAX_WAIT_TIME ->
        etap:bail("Timeout waiting for replica group update")
    end,
    UpdateCountAfter = get_replica_updates_count(),
    case UpdateCountAfter == (UpdateCountBefore + 1) of
    true ->
        ok;
    false ->
        etap:bail("Updater was not triggered")
    end.


get_replica_updates_count() ->
    get_replica_updates_count(get_replica_group_info()).


get_replica_updates_count(RepGroupInfo) ->
    {Stats} = couch_util:get_value(stats, RepGroupInfo),
    Updates = couch_util:get_value(full_updates, Stats),
    true = is_integer(Updates),
    Updates.


get_replica_group_info() ->
    {ok, MainInfo} = couch_set_view:get_group_info(
        spatial_view, test_set_name(), ddoc_id(), prod),
    {RepInfo} = couch_util:get_value(replica_group_info, MainInfo),
    RepInfo.


replica_index_disk_size() ->
    Info = get_replica_group_info(),
    Size = couch_util:get_value(disk_size, Info),
    true = is_integer(Size),
    true = (Size >= 0),
    Size.


create_docs(From, To) ->
    rand:seed(exrop, {91, 1, 11}),
    lists:map(
        fun(I) ->
            RandomMin = rand:uniform(2000),
            RandomMax = RandomMin + rand:uniform(167),
            RandomMin2 = rand:uniform(1769),
            RandomMax2 = RandomMin2 + rand:uniform(132),
            DocId = iolist_to_binary(["doc", integer_to_list(I)]),
            {[
              {<<"meta">>, {[{<<"id">>, DocId}]}},
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


populate_set() ->
    couch_set_view:cleanup_index_files(spatial_view, test_set_name()),
    etap:diag("Populating the " ++ integer_to_list(num_set_partitions()) ++
        " databases with " ++ integer_to_list(num_docs()) ++ " documents"),
    DDoc = {[
        {<<"meta">>, {[{<<"id">>, ddoc_id()}]}},
        {<<"json">>, {[
            {<<"spatial">>, {[
                {<<"test">>, <<"function(doc, meta) { "
                    "emit([[doc.min, doc.max], [doc.min2, doc.max2]], "
                    "'val'+doc.value); }">>}
            ]}}
        ]}}
    ]},
    ok = couch_set_view_test_util:update_ddoc(test_set_name(), DDoc),
    DocList = create_docs(1, num_docs()),
    ok = couch_set_view_test_util:populate_set_sequentially(
        test_set_name(),
        lists:seq(0, num_set_partitions() - 1),
        DocList),
    etap:diag("Configuring set view with partitions [0 .. 7] as active"),
    Params = #set_view_params{
        max_partitions = num_set_partitions(),
        active_partitions = lists:seq(0, 7),
        passive_partitions = [],
        use_replica_index = true
    },
    ok = couch_set_view:define_group(
           spatial_view, test_set_name(), ddoc_id(), Params).
