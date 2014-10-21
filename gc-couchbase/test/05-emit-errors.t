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


test_set_name() -> <<"couch_test_spatial_emit_errors">>.
num_set_partitions() -> 4.
ddoc_id() -> <<"_design/test">>.


main(_) ->
    test_util:init_code_path(),

    etap:plan(10),
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

    etap:diag("Testing erros in emits in spatial views"),

    test_spatial_emit_error_single_item_array(),
    test_spatial_emit_error_min_bigger_than_max(),
    test_spatial_emit_error_empty_range(),
    test_spatial_emit_error_non_geojson_legacy(),
    test_spatial_emit_error_non_geojson(),
    test_spatial_emit_error_geometry_not_as_first(),
    test_spatial_emit_error_nan_single(),
    test_spatial_emit_error_nan_range_min(),
    test_spatial_emit_error_nan_range_max(),
    test_spatial_emit_error_nan_range(),

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    spatial_test_util:stop_server(),
    ok.


test_spatial_emit_error_single_item_array() ->
    etap:diag("Error when emitting a single item array"),
    throw_error_test(
      <<"function(doc, meta) {emit([[doc.value]], doc.value);}">>,
      {emit_key, <<"A range cannot be single element array.">>},
      "Single element array error was thrown").


test_spatial_emit_error_min_bigger_than_max() ->
    etap:diag("Error when emitting a range where the minimum is bigger than "
              "the maximum"),
    throw_error_test(
      <<"function(doc, meta) {emit([[doc.value, doc.value - 1]], "
        "doc.value);}">>,
      {emit_key, <<"The minimum of a range must be smaller than "
                   "the maximum.">>},
      "Min > max error was thrown").


test_spatial_emit_error_empty_range() ->
    etap:diag("Error when emitting an empty range"),
    throw_error_test(
      <<"function(doc, meta) {emit([doc.value, []], doc.value);}">>,
      {emit_key, <<"A range cannot be an empty array.">>},
      "Empty array error was thrown").


test_spatial_emit_error_non_geojson_legacy() ->
    etap:diag("Error when emitting JSON that is not GeoJSON (legacy)"),
    throw_error_test(
      <<"function(doc, meta) {emit({\"foo\": \"bar\"}, doc.value);}">>,
      {emit_key, <<"The supplied geometry must be valid GeoJSON.">>},
      "Non valid GeoJSON error was thrown (legacy)").


test_spatial_emit_error_non_geojson() ->
    etap:diag("Error when emitting JSON that is not GeoJSON"),
    throw_error_test(
      <<"function(doc, meta) {emit([{\"foo\": \"bar\"}], doc.value);}">>,
      {emit_key, <<"The supplied geometry must be valid GeoJSON.">>},
      "Non valid GeoJSON error was thrown").


test_spatial_emit_error_geometry_not_as_first() ->
    etap:diag("Error when emitting a geometry not as the first item"),
    throw_error_test(
      <<"function(doc, meta) {emit([doc.value, "
        "{\"foo\": \"bar\"}], doc.value);}">>,
      {emit_key, <<"A geometry is only allowed as the first element "
                   "in the array.">>},
      "Geometry not allowed error was thrown").


test_spatial_emit_error_nan_single() ->
    etap:diag("Error when emitting other things than numbers as single item"),
    throw_error_test(
      <<"function(doc, meta) {emit([3, \"foo\"], doc.value);}">>,
      {emit_key, <<"The values of the key must be numbers or "
                   "a GeoJSON geometry.">>},
      "Not a number error was thrown").


test_spatial_emit_error_nan_range_min() ->
    etap:diag("Error when emitting other things than numbers as minimum "
              "in range"),
    throw_error_test(
      <<"function(doc, meta) {emit([7, [\"foo\", 5]], doc.value);}">>,
      {emit_key, <<"Ranges must be numbers.">>},
      "Not a number error was thrown (range min)").


test_spatial_emit_error_nan_range_max() ->
    etap:diag("Error when emitting other things than numbers as maximum "
              "in range"),
    throw_error_test(
      <<"function(doc, meta) {emit([9, [2, \"foo\"]], doc.value);}">>,
      {emit_key, <<"Ranges must be numbers.">>},
      "Not a number error was thrown (range max)").


test_spatial_emit_error_nan_range() ->
    etap:diag("Error when emitting other things than numbers as range"),
    throw_error_test(
      <<"function(doc, meta) {emit([9, [2, \"foo\"]], doc.value);}">>,
      {emit_key, <<"Ranges must be numbers.">>},
      "Not a number error was thrown (range)").


throw_error_test(ViewFun, Error, Message) ->
    setup_test(),
    create_ddoc(<<"emiterror">>, ViewFun),
    ok = configure_spatial_group(ddoc_id()),

    etap:throws_ok(
      fun() -> query_spatial_view(<<"emiterror">>, #spatial_query_args{}) end,
      Error,
      Message),
    shutdown_group().


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
    populate_set().


create_ddoc(ViewName, ViewFun) ->
    DDoc = {[
             {<<"meta">>, {[{<<"id">>, ddoc_id()}]}},
             {<<"json">>, {[{<<"spatial">>, {[{ViewName, ViewFun}]}}]}}
            ]},
    ok = couch_set_view_test_util:update_ddoc(test_set_name(), DDoc).


create_docs() ->
    lists:map(
      fun(Val) ->
              Id = list_to_binary(["item" | integer_to_list(Val)]),
              {[
                {<<"meta">>, {[{<<"id">>, Id}]}},
                {<<"json">>, {[{<<"value">>, Val}]}}
               ]}
      end, lists:seq(1, num_set_partitions())).


populate_set() ->
    etap:diag("Populating the " ++ integer_to_list(num_set_partitions()) ++
        " databases with some documents"),
    DocList = create_docs(),
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
