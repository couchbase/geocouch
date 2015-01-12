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
-define(JSON_DECODE(V), ejson:decode(V)).
-define(JSON_ENCODE(V), ejson:encode(V)).


test_set_name() -> <<"couch_test_spatial_emit">>.
num_set_partitions() -> 4.
ddoc_id() -> <<"_design/test">>.


main(_) ->
    test_util:init_code_path(),

    etap:plan(31),
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

    etap:diag("Testing emits in spatial views"),

    test_spatial_emit_geom_only(),
    test_spatial_emit_geom_and_value(),
    test_spatial_emit_without_geometry_point(),
    test_spatial_emit_without_geometry_range(),
    test_spatial_emit_without_geometry_point_and_range(),


    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    spatial_test_util:stop_server(),
    ok.


test_spatial_emit_geom_only() ->
    etap:diag("Testing emits with a geometry only"),
    setup_test(),
    ok = configure_spatial_group(ddoc_id()),
    Tests = [{[{105.0, 105.0}, {5.0, 5.0}],
              [{<<"geoPoint">>,
                [[105.0, 105.0], [5.0, 5.0]]}],
              "Point geometry was returned"},
             {[{200.0, 200.4}, {-1.0, 1.0}],
              [{<<"geoLineString">>,
                [[200.0, 201.0], [0.0, 2.0]]}],
              "LineString geometry was returned"},
             {[{400.8, 401.0}, {0.8, 1.0}],
              [{<<"geoPolygon">>,
                [[400.0,401.0], [0.0, 1.0]]}],
              "Polygon geometry was returned"},
             {[{300.8, 301.0}, {0.8, 1.0}],
              [{<<"geoPolygonWithHole">>,
                [[300.0, 301.0], [0.0,1.0]]}],
              "Polygon geometry (with hole) was returned"},
             {[{500.1, 500.2}, {0.8, 1.5}],
              [{<<"geoMultiPoint">>,
                [[500.0, 501.0],[0.0, 1.0]]}],
              "MultiPoint geometry was returned"},
             {[{601.2, 601.6}, {1.3, 1.5}],
              [{<<"geoMultiLineString">>,
                [[600.0, 603.0], [0.0, 3.0]]}],
              "MultiLineString geometry was returned"},
             {[{701.2, 701.6}, {2.3, 3.5}],
              [{<<"geoMultiPolygon">>,
                [[700.0, 703.0], [0.0,3.0]]}],
              "MultiPolygon geometry was returned"},
             {[{802, 802}, {0, 0}],
              [{<<"geoGeometryCollection">>,
                [[800.0, 802.0],[0.0, 1.0]]}],
              "GeometryCollection geometry was returned"},
             {[{-55, 27}, {-35, 16}],
              [{<<"geoLshapedPolygon">>,
                [[-11.953125, 61.171875], [-11.6015625, 48.1640625]]}],
              "L-shaped Polygon geometry was returned as range queries"
              "only compare on a boundinx box level and not on the"
              "geometry level"},
             {[{400.8, 500.2}, {0.8, 2.0}],
              [{<<"geoPolygon">>, [[400.0, 401.0], [0.0, 1.0]]},
               {<<"geoMultiPoint">>, [[500.0, 501.0], [0.0, 1.0]]}],
              "Polygon and MultiPoint geometry was returned"}
            ],
    lists:foreach(fun({Range, Expected, Message}) ->
                          query_for_expected_result(
                            <<"geomonly">>, Range, Expected, Message)
                  end, Tests),
    shutdown_group().


test_spatial_emit_geom_and_value() ->
    etap:diag("Testing emits with a geometry and an additional dimension"),
    setup_test(),
    ok = configure_spatial_group(ddoc_id()),
    Tests = [{[{105.0, 105.0}, {5.0, 5.0}, {0, 10}],
              [{<<"geoPoint">>,
                [[105.0, 105.0], [5.0, 5.0], [1.0, 1.0]]}],
              "Point geometry was returned"},
             {[{200.0, 200.4}, {-1.0, 1.0}, {0, 10}],
              [{<<"geoLineString">>,
                [[200.0, 201.0], [0.0, 2.0], [2.0, 2.0]]}],
              "LineString geometry was returned"},
             {[{400.8, 401.0}, {0.8, 1.0}, {0, 10}],
              [{<<"geoPolygon">>,
                [[400.0, 401.0],[0.0,1.0], [3.0, 3.0]]}],
              "Polygon geometry was returned"},
             {[{300.8, 301.0}, {0.8, 1.0}, {0, 10}],
              [{<<"geoPolygonWithHole">>,
                [[300.0, 301.0], [0.0, 1.0], [5.0, 5.0]]}],
              "Polygon geometry (with hole) was returned"},
             {[{500.1, 500.2}, {0.8, 1.5}, {0, 10}],
              [{<<"geoMultiPoint">>,
                [[500.0, 501.0], [0.0, 1.0], [6.0, 6.0]]}],
              "MultiPoint geometry was returned"},
             {[{601.2, 601.6}, {1.3, 1.5}, {0, 10}],
              [{<<"geoMultiLineString">>,
                [[600.0, 603.0], [0.0, 3.0], [7.0, 7.0]]}],
              "MultiLineString geometry was returned"},
             {[{701.2, 701.6}, {2.3, 3.5}, {0, 10}],
              [{<<"geoMultiPolygon">>,
                [[700.0, 703.0], [0.0, 3.0], [8.0, 8.0]]}],
              "MultiPolygon geometry was returned"},
             {[{802, 802}, {0, 0}, {0, 10}],
              [{<<"geoGeometryCollection">>,
                [[800.0, 802.0], [0.0, 1.0], [9.0, 9.0]]}],
              "GeometryCollection geometry was returned"},
             {[{-55, 27}, {-35, 16}, {0, 10}],
              [{<<"geoLshapedPolygon">>,
                [[-11.953125, 61.171875], [-11.6015625, 48.1640625],
                 [4.0, 4.0]]}],
              "L-shaped Polygon geometry was returned as range queries"
              "only compare on a boundinx box level and not on the"
              "geometry level"},
             {[{400.8, 500.2}, {0.8, 2.0}, {0, 10}],
              [{<<"geoPolygon">>, [[400.0, 401.0],[0.0, 1.0], [3.0, 3.0]]},
               {<<"geoMultiPoint">>, [[500.0, 501.0],[0.0, 1.0], [6.0, 6.0]]}],
              "Polygon and MultiPoint geometry was returned"},
             {[{0, 250}, {0, 230}, {0, 2.9}],
              [{<<"geoPoint">>, [[105.0, 105.0], [5.0, 5.0], [1.0, 1.0]]},
               {<<"geoLineString">>,
                [[200.0, 201.0], [0.0, 2.0], [2.0, 2.0]]}],
              "Point and Linestring geometry was returned"},
             {[{400.8, 401.0}, {0.8, 1.0}, {-20, -10}],
              [],
              "Polygon geometry would've been returned, but wasn't "
              "because of the 3rd dimension"},
             {[{0, 250}, {0, 230}, {0, 1.6}],
              [{<<"geoPoint">>, [[105.0, 105.0], [5.0, 5.0], [1.0, 1.0]]}],
              "Linestring geometry would've been returned, but wasn't "
              "because of the 3rd dimension"},
             {[{0, 250}, {3, 8}, {0, 2.9}],
              [{<<"geoPoint">>, [[105.0, 105.0], [5.0, 5.0], [1.0, 1.0]]}],
              "Linestring geometry would've been returned, but wasn't "
              "because of the 2nd dimension"},
             {[{0, 150}, {0, 8}, {0, 2.9}],
              [{<<"geoPoint">>, [[105.0, 105.0], [5.0, 5.0], [1.0, 1.0]]}],
              "Linestring geometry would've been returned, but wasn't "
              "because of the 1st dimension"}
            ],
    lists:foreach(fun({Range, Expected, Message}) ->
                          query_for_expected_result(
                            <<"geomandvalue">>, Range, Expected, Message)
                  end, Tests),
    shutdown_group().


test_spatial_emit_without_geometry_point() ->
    etap:diag("Testing emits without a geometry (point emit)"),
    setup_test(),
    ok = configure_spatial_group(ddoc_id()),
    Tests = [{[{2.7, 3.4}, {4.1, 6.8}],
              [{<<"item3">>, [[3, 3], [6, 6]]}],
              "Correct item was returned"},
             {[{0.3, 2.3}, {0.3, 4.1}],
              [{<<"item1">>, [[1, 1], [2, 2]]},
               {<<"item2">>, [[2, 2], [4, 4]]}],
              "Correct items were returned"}
            ],
    lists:foreach(fun({Range, Expected, Message}) ->
                          query_for_expected_result_no_geom(
                            <<"withoutgeompoint">>, Range, Expected, Message)
                  end, Tests),
    shutdown_group().


test_spatial_emit_without_geometry_range() ->
    etap:diag("Testing emits without a geometry (range emit)"),
    setup_test(),
    ok = configure_spatial_group(ddoc_id()),
    Tests = [{[{2.1, 2.4}, {4.1, 6.8}],
              [{<<"item2">>, [[2, 4], [4, 12]]}],
              "Correct item was returned"},
             {[{0.3, 2.3}, {0.3, 4.1}],
              [{<<"item1">>, [[1, 2], [2, 11]]},
               {<<"item2">>, [[2, 4], [4, 12]]}],
              "Correct items were returned"}
            ],
    etap:diag("Testing emits without a geometry (range emits)"),
    lists:foreach(fun({Range, Expected, Message}) ->
                          query_for_expected_result_no_geom(
                            <<"withoutgeomrange">>, Range, Expected, Message)
                  end, Tests),
    shutdown_group().


test_spatial_emit_without_geometry_point_and_range() ->
    etap:diag("Testing emits without a geometry (point and range emit)"),
    setup_test(),
    ok = configure_spatial_group(ddoc_id()),
    Tests = [{[{2.1, 2.4}, {8.7, 12.8}],
              [{<<"item2">>, [[2, 4], [12, 12]]}],
              "Correct item was returned"},
             {[{0.3, 2.3}, {11.0, 24.1}],
              [{<<"item1">>, [[1, 2], [11, 11]]},
               {<<"item2">>, [[2, 4], [12, 12]]}],
              "Correct items were returned"}
            ],
    lists:foreach(fun({Range, Expected, Message}) ->
                          query_for_expected_result_no_geom(
                            <<"withoutgeompointandrange">>, Range, Expected,
                            Message)
                  end, Tests),
    shutdown_group().


query_for_expected_result(View, Range, Expected, Message) ->
    ViewArgs = #spatial_query_args{range = Range},
    {ok, Rows} = (catch query_spatial_view(View, ViewArgs)),
    verify_rows(Rows, Expected, Message).

verify_rows(Rows, Expected, Message) ->
    ExpectedRows = lists:foldl(
                      fun({DocId, {Val, Geom}}, Acc) ->
                              case lists:keyfind(DocId, 1, Expected) of
                                  false ->
                                      Acc;
                                  {DocId, Mbb} ->
                                     [{DocId, Mbb, Val, {Geom}} | Acc]
                              end
                      end, [], geojson_docs()),
    RowsWithoutPartId = [{DocId, Key, ?JSON_DECODE(Value), Geom} ||
        {Key, DocId, {_PartId, Value, Geom}} <- Rows],
    etap:is(lists:sort(RowsWithoutPartId), lists:sort(ExpectedRows), Message).


query_for_expected_result_no_geom(View, Range, Expected, Message) ->
    ViewArgs = #spatial_query_args{range = Range},
    {ok, Rows} = (catch query_spatial_view(View, ViewArgs)),
    verify_rows_no_geom(Rows, Expected, Message).

verify_rows_no_geom(Rows, Expected, Message) ->
    ExpectedRows = lists:foldl(
                      fun({DocId, {_, _, Val}}, Acc) ->
                              case lists:keyfind(DocId, 1, Expected) of
                                  false ->
                                      Acc;
                                  {DocId, Mbb} ->
                                     [{DocId, Mbb, Val} | Acc]
                              end
                      end, [], nogeom_docs()),
    RowsWithoutPartId = [{DocId, Key, ?JSON_DECODE(Value)} ||
        {Key, DocId, {_PartId, Value, nil}} <- Rows],
    etap:is(lists:sort(RowsWithoutPartId), lists:sort(ExpectedRows), Message).


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
                {<<"geomonly">>,
                 <<"function(doc, meta) {if (doc.geom) {"
                   "emit(doc.geom, doc.value);}}">>},
                {<<"geomandvalue">>,
                     <<"function(doc, meta) {if (doc.geom) {"
                       "emit([doc.geom, doc.value], doc.value);}}">>},
                {<<"withoutgeompoint">>,
                     <<"function(doc, meta) {if (doc.geom === undefined) {"
                       "emit([doc.value1, doc.value2], doc.value10);}}">>},
                {<<"withoutgeomrange">>,
                     <<"function(doc, meta) {if (doc.geom === undefined) {"
                       "emit([[doc.value1, doc.value2], "
                       "[doc.value2, doc.value10]], doc.value10);}}">>},
                {<<"withoutgeompointandrange">>,
                     <<"function(doc, meta) {if (doc.geom === undefined) {"
                       "emit([[doc.value1, doc.value2], doc.value10], "
                       "doc.value10);}}">>}
            ]}}
        ]}}
    ]},
    populate_set(DDoc).


geojson_docs() ->
% some geometries are based on the GeoJSON specification
% http://geojson.org/geojson-spec.html (2010-08-17)
[
 {<<"geoPoint">>, {
      1,
      [{<<"type">>, <<"Point">>}, {<<"coordinates">>, [105.0, 5.0]}]}},
 {<<"geoLineString">>, {
      2,
      [{<<"type">>, <<"LineString">>},
       {<<"coordinates">>, [[200.0, 0.0], [201.0, 2.0]]}]}},
 {<<"geoPolygon">>, {
      3,
      [{<<"type">>, <<"Polygon">>},
       {<<"coordinates">>, [[[400.0, 0.0], [401.0, 0.0], [400.0, 1.0],
                             [400.0, 0.0]]]}]}},
 {<<"geoLshapedPolygon">>, {
    4,
    [{<<"type">>, <<"Polygon">>},
     {<<"coordinates">>, [[[-11.25, 48.1640625], [-11.953125, 22.8515625],
                           [35.859375, 21.4453125], [35.859375, -10.8984375],
                           [61.171875, -11.6015625], [60.46875, 47.4609375],
                           [60.46875, 46.0546875], [-11.25, 48.1640625]]]}]}},
 {<<"geoPolygonWithHole">>, {
    5,
    [{<<"type">>, <<"Polygon">>},
     {<<"coordinates">>,
      [[ [300.0, 0.0], [301.0, 0.0], [300.0, 1.0], [300.0, 0.0] ],
       [ [300.2, 0.2], [300.6, 0.2], [300.2, 0.6], [300.2, 0.2] ]]}]}},
 {<<"geoMultiPoint">>, {
    6,
    [{<<"type">>, <<"MultiPoint">>},
     {<<"coordinates">>,  [ [500.0, 0.0], [501.0, 1.0] ]}]}},
 {<<"geoMultiLineString">>, {
    7,
    [{<<"type">>, <<"MultiLineString">>},
     {<<"coordinates">>, [[ [600.0, 0.0], [601.0, 1.0] ],
                          [ [602.0, 2.0], [603.0, 3.0] ]]}]}},
 {<<"geoMultiPolygon">>, {
    8,
    [{<<"type">>, <<"MultiPolygon">>},
     {<<"coordinates">>,
      [
       [
        [[702.0, 2.0], [703.0, 2.0], [703.0, 3.0], [702.0, 3.0], [702.0, 2.0]]
       ],
       [
        [[700.0, 0.0], [701.0, 0.0], [701.0, 1.0], [700.0, 1.0], [700.0, 0.0]],
        [[700.2, 0.2], [700.8, 0.2], [700.8, 0.8], [700.2, 0.8], [700.2, 0.2]]
       ]]}]}},
 {<<"geoGeometryCollection">>, {
    9,
    [{<<"type">>, <<"GeometryCollection">>},
     {<<"geometries">>,
      [{[{<<"type">>, <<"Point">>}, {<<"coordinates">>, [800.0, 0.0] }]},
       {[{<<"type">>, <<"LineString">>}, {<<"coordinates">>,
                                      [ [801.0, 0.0], [802.0, 1.0] ]}]}]}]}}
].

nogeom_docs() ->
    lists:map(
      fun(Val) ->
              {list_to_binary(["item" | integer_to_list(Val)]),
               {Val, Val * 2, Val + 10}}
      end, lists:seq(1, 5)).

create_docs() ->
    Geoms = lists:map(
      fun({Id, {Val, Geom}}) ->
            {[
              {<<"meta">>, {[{<<"id">>, Id}]}},
              {<<"json">>, {[
                             {<<"value">>, Val},
                             {<<"geom">>, {Geom}}
                            ]}}
            ]}
      end, geojson_docs()),
    Values = lists:map(
      fun({Id, {Val1, Val2, Val3}}) ->
            {[
              {<<"meta">>, {[{<<"id">>, Id}]}},
              {<<"json">>, {[
                             {<<"value1">>, Val1},
                             {<<"value2">>, Val2},
                             {<<"value10">>, Val3}
                            ]}}
            ]}
      end, nogeom_docs()),
    Geoms ++ Values.


populate_set(DDoc) ->
    etap:diag("Populating the " ++ integer_to_list(num_set_partitions()) ++
        " databases with some GeoJSON documents"),
    ok = couch_set_view_test_util:update_ddoc(test_set_name(), DDoc),
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
