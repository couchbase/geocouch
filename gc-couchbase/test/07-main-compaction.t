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
-define(i2l(I), integer_to_list(I)).

-include_lib("couch_set_view/include/couch_set_view.hrl").

test_set_name() -> <<"couch_test_spatial_index_main_compact">>.
num_set_partitions() -> 64.
ddoc_id() -> <<"_design/test">>.
num_docs() -> 24128.


main(_) ->
    test_util:init_code_path(),

    etap:plan(64),
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

    etap:diag("Verifying group snapshot before marking partitions [ 8 .. 31 ] "
        " for cleanup"),
    #set_view_group{index_header = Header0} = get_group_snapshot(false),
    etap:is(
        [P || {P, _} <- Header0#set_view_index_header.seqs],
        lists:seq(0, 31),
        "Right list of partitions in the header's seq field"),
    etap:is(
        Header0#set_view_index_header.has_replica,
        true,
        "Header has replica support flag set to true"),
    lists:foreach(
        fun({PartId, Seq}) ->
            DocCount = couch_set_view_test_util:doc_count(
                test_set_name(), [PartId]),
            etap:is(Seq, DocCount,
                "Right update seq for partition " ++ ?i2l(PartId))
        end,
        Header0#set_view_index_header.seqs),

    DiskSizeBefore = main_index_disk_size(),

    verify_group_info_before_cleanup_request(),
    GroupPid = couch_set_view:get_group_pid(
        spatial_view, test_set_name(), ddoc_id(), prod),
    ok = gen_server:call(GroupPid, {set_auto_cleanup, false}, infinity),
    ok = couch_set_view:set_partition_states(
        spatial_view, test_set_name(), ddoc_id(), [], [], lists:seq(8, 63)),
    verify_group_info_after_cleanup_request(),

    GroupBefore = get_group_snapshot(false),

    etap:is(
        couch_ref_counter:count(GroupBefore#set_view_group.ref_counter),
        1,
        "Main group's ref counter count is 1"),

    etap:diag("Triggering main group compaction"),
    {ok, CompactPid} = couch_set_view_compactor:start_compact(
        spatial_view, test_set_name(), ddoc_id(), main),
    Ref = erlang:monitor(process, CompactPid),
    etap:diag("Waiting for main group compaction to finish"),
    receive
    {'DOWN', Ref, process, CompactPid, normal} ->
        ok;
    {'DOWN', Ref, process, CompactPid, noproc} ->
        ok;
    {'DOWN', Ref, process, CompactPid, Reason} ->
        etap:bail("Failure compacting main group: " ++
            couch_util:to_list(Reason))
    after ?MAX_WAIT_TIME ->
        etap:bail("Timeout waiting for main group compaction to finish")
    end,

    GroupAfter = get_group_snapshot(false),

    etap:isnt(
        GroupAfter#set_view_group.ref_counter,
        GroupBefore#set_view_group.ref_counter,
        "Different ref counter for main group after compaction"),
    etap:isnt(
        GroupAfter#set_view_group.fd,
        GroupBefore#set_view_group.fd,
        "Different fd for main group after compaction"),

    etap:is(
        couch_ref_counter:count(GroupAfter#set_view_group.ref_counter),
        1,
        "Main group's new ref counter count is 1 after compaction"),

    etap:is(
        is_process_alive(GroupBefore#set_view_group.ref_counter),
        false,
        "Old group ref counter is dead"),

    etap:is(
        is_process_alive(GroupBefore#set_view_group.fd),
        false,
        "Old group fd is dead"),

    GroupInfo = get_main_group_info(),
    {Stats} = couch_util:get_value(stats, GroupInfo),
    etap:is(couch_util:get_value(compactions, Stats), 1,
        "Main group had 1 full compaction in stats"),
    etap:is(couch_util:get_value(cleanups, Stats), 1,
        "Main group had 1 full cleanup in stats"),

    verify_group_info_after_main_compact(),

    DiskSizeAfter = main_index_disk_size(),
    etap:is(DiskSizeAfter < DiskSizeBefore, true,
        "Index file size is smaller after compaction"),

    etap:diag("Verifying group snapshot after main group compaction"),
    #set_view_group{index_header = Header1} = get_group_snapshot(false),
    etap:is(
        [P || {P, _} <- Header1#set_view_index_header.seqs],
        lists:seq(0, 7),
        "Right list of partitions in the header's seq field"),
    etap:is(
        Header1#set_view_index_header.has_replica,
        true,
        "Header has replica support flag set to true"),
    etap:is(
        Header1#set_view_index_header.num_partitions,
        Header0#set_view_index_header.num_partitions,
        "Compaction preserved header field num_partitions"),
    lists:foreach(
        fun({PartId, Seq}) ->
            DocCount = couch_set_view_test_util:doc_count(
                test_set_name(), [PartId]),
            etap:is(Seq, DocCount,
                "Right update seq for partition " ++ ?i2l(PartId))
        end,
        Header1#set_view_index_header.seqs),

    couch_set_view_test_util:delete_set_dbs(
        test_set_name(), num_set_partitions()),
    ok = timer:sleep(1000),
    couch_set_view_test_util:stop_server(),
    ok.


get_group_snapshot(StaleType) ->
    {ok, Group} = couch_set_view:get_group(
        spatial_view, test_set_name(), ddoc_id(),
        #set_view_group_req{stale = StaleType, debug = true}),
    couch_ref_counter:drop(Group#set_view_group.ref_counter),
    Group.


verify_group_info_before_cleanup_request() ->
    etap:diag("Verifying main group info before marking partitions "
          "[ 8 .. 31 ] for cleanup"),
    GroupInfo = get_main_group_info(),
    etap:is(
        couch_util:get_value(active_partitions, GroupInfo),
        lists:seq(0, 31),
        "Main group has [ 0 .. 31 ] as active partitions"),
    etap:is(
        couch_util:get_value(passive_partitions, GroupInfo),
        [],
        "Main group has [ ] as passive partitions"),
    etap:is(
        couch_util:get_value(cleanup_partitions, GroupInfo),
        [],
        "Main group has [ ] as cleanup partitions").


verify_group_info_after_cleanup_request() ->
    etap:diag("Verifying main group info after marking partitions [ 8 .. 31 ] "
          "for cleanup"),
    GroupInfo = get_main_group_info(),
    etap:is(
        couch_util:get_value(active_partitions, GroupInfo),
        lists:seq(0, 7),
        "Main group has [ 0 .. 7 ] as active partitions"),
    etap:is(
        couch_util:get_value(passive_partitions, GroupInfo),
        [],
        "Main group has [ ] as passive partitions"),
    CleanupParts = couch_util:get_value(cleanup_partitions, GroupInfo),
    etap:is(
        length(CleanupParts) > 0,
        true,
        "Main group has non-empty set of cleanup partitions"),
    etap:is(
        ordsets:intersection(CleanupParts,
            lists:seq(0, 7) ++ lists:seq(32, 63)),
        [],
        "Main group doesn't have any cleanup partition with ID in"
        " [ 0 .. 7, 32 .. 63 ]").


verify_group_info_after_main_compact() ->
    etap:diag("Verifying main group info after compaction"),
    GroupInfo = get_main_group_info(),
    etap:is(
        couch_util:get_value(active_partitions, GroupInfo),
        lists:seq(0, 7),
        "Main group has [ 0 .. 7 ] as active partitions"),
    etap:is(
        couch_util:get_value(passive_partitions, GroupInfo),
        [],
        "Main group has [ ] as passive partitions"),
    etap:is(
        couch_util:get_value(cleanup_partitions, GroupInfo),
        [],
        "Main group has [ ] as cleanup partitions").


get_main_group_info() ->
    {ok, MainInfo} = couch_set_view:get_group_info(
        spatial_view, test_set_name(), ddoc_id(), prod),
    MainInfo.


main_index_disk_size() ->
    Info = get_main_group_info(),
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
    etap:diag("Populating the " ++ ?i2l(num_set_partitions()) ++
        " databases with " ++ ?i2l(num_docs()) ++ " documents"),
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
    etap:diag("Configuring set view with partitions [0 .. 31] as active"),
    Params = #set_view_params{
        max_partitions = num_set_partitions(),
        active_partitions = lists:seq(0, 31),
        passive_partitions = [],
        use_replica_index = true
    },
    ok = couch_set_view:define_group(
           spatial_view, test_set_name(), ddoc_id(), Params).

