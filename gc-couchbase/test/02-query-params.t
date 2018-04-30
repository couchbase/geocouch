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

%-record(view_query_args, {
%    start_key,
%    end_key,
%    start_docid = ?MIN_STR,
%    end_docid = ?MAX_STR,
%    direction = fwd,
%    inclusive_end = true,
%    limit = 10000000000,
%    skip = 0,
%    group_level = 0,
%    view_type = nil,
%    include_docs = false,
%    conflicts = false,
%    stale = false,
%    multi_get = false,
%    callback = nil,
%    list = nil,
%    run_reduce = true,
%    keys = nil,
%    view_name = nil,
%    debug = false,
%    filter = true,
%    type = main
%}).

test_set_name() -> <<"couch_test_spatial_view_query_params">>.
num_set_partitions() -> 4.
ddoc_id() -> <<"_design/test">>.
num_docs() -> 1024.  % keep it a multiple of num_set_partitions()


main(_) ->
    test_util:init_code_path(),

    etap:plan(12),
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

    etap:diag("Testing query parameters of spatial views"),

    test_spatial_query_all(),
    test_spatial_query_range(),
    test_spatial_query_range_error(),

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    spatial_test_util:stop_server(),
    ok.


% Return just all the data
test_spatial_query_all() ->
    setup_test(),
    ok = configure_spatial_group(ddoc_id()),

    ViewArgs = #spatial_query_args{
        %view_name = ViewName
    },

    {ok, Rows} = (catch query_spatial_view(<<"test">>, ViewArgs)),
    etap:is(length(Rows), num_docs(),
        "Got all view rows (" ++ integer_to_list(num_docs()) ++ ")"),
    verify_rows(Rows),

    shutdown_group().


test_spatial_query_range() ->
    setup_test(),
    ok = configure_spatial_group(ddoc_id()),

    Tests = [{[{740, 1500}, {300, 1765}, {80, 628}, {51, 457}],
              283, "All dimensions limit the result"},
             {[{631.482, 963.315}, {-10.8, 8576.5}, {83.1, 584.27}, {80, 300}],
              84, "2nd dimension doesn't limit the result"},
             {[{740, 1500}, {300, 1765}, {80, 80}, {51, 457}],
              34, "3rd dimension is a collapsed to point"},
             {[{740, 1500}, {300, 1765}, {80, 628}, {nil, nil}],
              322, "4th dimension is wildcard"},
             {[{740, 1500}, {300, 1765}, {80, 628}, {nil, 457}],
              302, "4th dimension is open at start"},
             {[{740, 1500}, {300, 1765}, {80, 628}, {51, nil}],
              303, "4th dimension is open at end"},
             {[{740, 1500}, {nil, nil}, {80, 628}, {51, nil}],
              349, "4th dimension is open at end and 2nd is a wildcard"},
             {[{nil, nil}, {nil, nil}, {nil, nil}, {nil, nil}],
              num_docs(), "All dimensions are wildcards"}],
    lists:foreach(fun({Range, Expected, Message}) ->
                          query_for_expected_result(
                            Range, Expected, Message)
                  end, Tests),

    shutdown_group().


test_spatial_query_range_error() ->
    setup_test(),
    ok = configure_spatial_group(ddoc_id()),

    Tests = [{[{1, 1}, {2, 2}, {3, 3}],
              <<"The query range must have the same "
                "dimensionality as the index. "
                "Your range was `[{1,1},{2,2},{3,3}]`, "
                "but the index has a dimensionality of `4`.">>,
              "Only 3 dimensions given for a 4 dimensional index"},
             {[{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}],
              <<"The query range must have the same "
                "dimensionality as the index. "
                "Your range was `[{1,1},{2,2},{3,3},{4,4},{5,5}]`, "
                "but the index has a dimensionality of `4`.">>,
              "5 dimensions given for a 4 dimensional index"}],
    lists:foreach(fun({Range, Expected, Message}) ->
                          query_for_expected_error(
                            Range, Expected, Message)
                  end, Tests),

    shutdown_group().


query_for_expected_result(Range, Expected, Message) ->
    ViewArgs = #spatial_query_args{ range = Range},
    {ok, Rows} = (catch query_spatial_view(<<"test">>, ViewArgs)),
    etap:is(length(Rows), Expected, Message).


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


query_for_expected_error(Range, Error, Message) ->
    ViewArgs = #spatial_query_args{range = Range},
    etap:throws_ok(
      fun() -> query_spatial_view(<<"test">>, ViewArgs) end,
      Error,
      Message).


query_spatial_view(ViewName, ViewArgs) ->
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
                {<<"test">>, <<"function(doc, meta) { "
                               "emit([[doc.min, doc.max], "
                               "[doc.min2, doc.max2], "
                               "[doc.min3, doc.max3], "
                               "[doc.min4, doc.max4]], "
                               "'val'+doc.value); }">>}
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
            RandomMin3 = rand:uniform(651),
            RandomMax3 = RandomMin3 + rand:uniform(201),
            RandomMin4 = rand:uniform(482),
            RandomMax4 = RandomMin4 + rand:uniform(26),
            {[
              {<<"meta">>, {[{<<"id">>, iolist_to_binary(["doc", integer_to_list(I)])}]}},
              {<<"json">>, {[
                             {<<"value">>, I},
                             {<<"min">>, RandomMin},
                             {<<"max">>, RandomMax},
                             {<<"min2">>, RandomMin2},
                             {<<"max2">>, RandomMax2},
                             {<<"min3">>, RandomMin3},
                             {<<"max3">>, RandomMax3},
                             {<<"min4">>, RandomMin4},
                             {<<"max4">>, RandomMax4}
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
