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

-define(MOD, couch_spatial_updater).
-define(JSON_ENCODE(V), ejson:encode(V)).

main(_) ->
    code:add_pathz(filename:dirname(escript:script_name())),
    gc_test_util:init_code_path(),
    etap:plan(28),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail(Other)
    end,
    ok.

test() ->
    test_bbox(),
    test_bbox_initbbox(),
    test_extract_bbox(),
    test_process_result_geometrycollection(),
    test_process_result_point(),
    test_process_result_point_bbox(),
    test_process_result_linestring(),
    test_process_result_linestring_toosmallbbox(),
    test_geojsongeom_to_geocouch_point(),
    test_geojsongeom_to_geocouch_linestring(),
    test_geojsongeom_to_geocouch_geometrycollection(),
    test_geojsongeom_to_geocouch_nested_geometrycollection(),
    test_geocouch_to_geojsongeom_point(),
    test_geocouch_to_geojsongeom_linestring(),
    test_geocouch_to_geojsongeom_geometrycollection(),
    test_geocouch_to_geojsongeom_nested_geometrycollection(),
    ok.

% The tests are based on the examples of the GeoJSON format specification
test_bbox() ->
    etap:is(?MOD:bbox([[100.0, 0.0], [101.0, 1.0]], nil),
        [100.0, 0.0, 101.0, 1.0],
        "Bounding box of LineString with 2 points (initial bbox==nil)"),
    etap:is(?MOD:bbox([[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0],
            [100.0, 0.0]], nil),
        [100.0, 0.0, 101.0, 1.0],
        "Bounding box of LineString with 4 points (initial bbox==nil)"),

    etap:is(?MOD:bbox([[-10.0, 0.0, 50.4, 58.69], [101.0, -1.0, -72.8, 9.5]],
        nil),
            [-10.0, -1.0, -72.8, 9.5, 101.0, 0.0, 50.4, 58.69],
        "Bounding box of LineString with 4 dimensional coordinates "
        "(initial bbox==nil)").

test_bbox_initbbox() ->
    etap:is(?MOD:bbox([[100.0, 0.0], [110.0, 1.0]],
            [105.4, 20.3, 200.36, 0.378]),
        [100.0, 0.0, 200.36, 1.0],
        "Bounding box with initial bounding box (a)"),
    etap:is(?MOD:bbox([[100.0, 0.0], [110.0, 1.0]],
            {[105.4, 20.3], [200.36, 0.378]}),
        [100.0, 0.0, 200.36, 1.0],
        "Bounding box with initial bounding box (b)").

test_extract_bbox() ->
    etap:is(?MOD:extract_bbox('Point', [100.0, 0.0]),
         [100.0, 0.0, 100.0, 0.0],
         "Extract bounding box of a Point"),
    etap:is(?MOD:extract_bbox('LineString', [[100.0, 0.0], [101.0, 1.0]]),
         [100.0, 0.0, 101.0, 1.0],
         "Extract bounding box of a LineString"),
    etap:is(?MOD:extract_bbox('Polygon', [
        [[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]],
        [[100.2, 0.2], [100.8, 0.2], [100.8, 0.8], [100.2, 0.8], [100.2, 0.2]]
            ]),
        [100.0, 0.0, 101.0, 1.0],
         "Extract bounding box of a Polygon"),
    etap:is(?MOD:extract_bbox('MultiLineString', [
            [[100.0, 0.0], [101.0, 1.0]],
            [[102.0, 2.0], [103.0, 3.0]]
            ]),
        [100.0, 0.0, 103.0, 3.0],
         "Extract bounding box of a Polygon"),
    etap:is(?MOD:extract_bbox('MultiPolygon', [
            [[[102.0, 2.0], [103.0, 2.0], [103.0, 3.0], [102.0, 3.0],
             [102.0, 2.0]]],
            [[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0],
             [100.0, 0.0]],
            [[100.2, 0.2], [100.8, 0.2], [100.8, 0.8], [100.2, 0.8],
             [100.2, 0.2]]]
            ]),
         [100.0, 0.0, 103.0, 3.0],
         "Extract bounding box of a MultiPolygon").



test_process_result_geometrycollection() ->
    Geojson = {[{<<"type">>,<<"GeometryCollection">>},
                {<<"geometries">>,
                 [{[{<<"type">>,<<"Point">>},
                    {<<"coordinates">>,[100.0,0.0]}]},
                  {[{<<"type">>,<<"LineString">>},
                    {<<"coordinates">>,[[101.0,0.0],[102.0,1.0]]}]}]}]},
    {Bbox, {Geom, <<"somedoc">>}} = ?MOD:process_result(
        {?JSON_ENCODE(Geojson), ?JSON_ENCODE(<<"somedoc">>)}),
    etap:is(Geom,
            {'GeometryCollection', [
                {'Point', [100.0,0.0]},
                {'LineString', [[101.0,0.0],[102.0,1.0]]}]},
        "GeometryCollection was processed correctly"),
    etap:is(Bbox,
        {100.0, 0.0, 102.0, 1.0},
        "Bounding box of GeometryCollection is correct").

% XXX vmx (2011-02-16) Nested GeometryCollections are currently not supported
%process_result_nested_geometrycollection() ->
%    Geojson = {[{<<"type">>,<<"GeometryCollection">>},
%                {<<"geometries">>,
%                 [{[{<<"type">>,<<"GeometryCollection">>},
%                  {<<"geometries">>,
%                   [{[{<<"type">>,<<"Point">>},
%                    {<<"coordinates">>,[100.0,0.0]}]},
%                    {[{<<"type">>,<<"LineString">>},
%                      {<<"coordinates">>,[[101.0,0.0],[102.0,1.0]]}]}]}]}
%                 ]}]},
%    {Bbox, {Geom, <<"somedoc">>}} = process_result([Geojson, <<"somedoc">>]),
%    ?assertEqual({'GeometryCollection', [{'GeometryCollection', [
%            {'Point', [100.0,0.0]},
%            {'LineString', [[101.0,0.0],[102.0,1.0]]}]}]},
%        Geom),
%    ?assertEqual({100.0, 0.0, 102.0, 1.0}, Bbox).

% XXX vmx (2011-03-09) Need to find a way to test failures with etap
%test_process_result_geometrycollection_fail() ->
%    % collection contains geometries with different dimensions
%    Geojson = {[{<<"type">>,<<"GeometryCollection">>},
%                {<<"geometries">>,
%                 [{[{<<"type">>,<<"Point">>},
%                    {<<"coordinates">>,[100.0,0.0,54.5]}]},
%                  {[{<<"type">>,<<"LineString">>},
%                    {<<"coordinates">>,[[101.0,0.0],[102.0,1.0]]}]}]}]},
%    ?assertError(function_clause, process_result([Geojson, <<"somedoc">>])).

test_process_result_point() ->
    Geojson = {[{<<"type">>,<<"Point">>},
                {<<"coordinates">>,[100.0,0.0]}]},
    {Bbox, {Geom, <<"somedoc">>}} = ?MOD:process_result(
        {?JSON_ENCODE(Geojson), ?JSON_ENCODE(<<"somedoc">>)}),
    etap:is(Geom, {'Point', [100.0,0.0]},
        "Point was processed correctly"),
    etap:is(Bbox, {100.0, 0.0, 100.0, 0.0},
        "Bounding box of Point is correct").

test_process_result_point_bbox() ->
    Geojson = {[{<<"type">>,<<"Point">>},
                {<<"coordinates">>,[100.0,0.0]},
                {<<"bbox">>,[100.0,0.0,105.54,8.614]}]},
    {Bbox, {Geom, <<"somedoc">>}} = ?MOD:process_result(
        {?JSON_ENCODE(Geojson), ?JSON_ENCODE(<<"somedoc">>)}),
    etap:is(Geom, {'Point', [100.0,0.0]},
        "Point was processed correctly (with pre set bounding box)"),
    etap:is(Bbox, {100.0, 0.0, 105.54, 8.614},
        "Bounding box of Point is correct (with pre set bounding box)").


test_process_result_linestring() ->
    Geojson = {[{<<"type">>,<<"LineString">>},
                {<<"coordinates">>,[[101.0,0.0],[102.0,1.0]]}]},
    {Bbox, {Geom, <<"somedoc">>}} = ?MOD:process_result(
        {?JSON_ENCODE(Geojson), ?JSON_ENCODE(<<"somedoc">>)}),
    etap:is(Geom, {'LineString', [[101.0,0.0],[102.0,1.0]]},
        "LineString was processed correctly"),
    etap:is(Bbox, {101.0, 0.0, 102.0, 1.0},
        "Bounding box of LineString is correct").


test_process_result_linestring_toosmallbbox() ->
    Geojson = {[{<<"type">>,<<"LineString">>},
                {<<"coordinates">>,[[101.0,0.0],[102.0,1.0]]},
                {<<"bbox">>,[101.0,0.0,101.54,0.614]}]},
    {Bbox, {Geom, <<"somedoc">>}} = ?MOD:process_result(
        {?JSON_ENCODE(Geojson), ?JSON_ENCODE(<<"somedoc">>)}),
    etap:is(Geom, {'LineString', [[101.0,0.0],[102.0,1.0]]},
        "LineString was processed correctly (with too small bounding box)"),
    etap:is(Bbox, {101.0, 0.0, 101.54, 0.614},
        "Bounding box of LineString is correct (with too small bounding box)").


test_geojsongeom_to_geocouch_point() ->
    Geojson = [{<<"type">>,<<"Point">>},
                {<<"coordinates">>,[100.0,0.0]}],
    Geom = ?MOD:geojsongeom_to_geocouch(Geojson),
    etap:is(Geom, {'Point', [100.0,0.0]},
        "Transform Point from GeoJSON to a GeoCouch geometry").

test_geojsongeom_to_geocouch_linestring() ->
    Geojson = [{<<"type">>,<<"LineString">>},
                {<<"coordinates">>,[[101.0,0.0],[102.0,1.0]]}],
    Geom = ?MOD:geojsongeom_to_geocouch(Geojson),
    etap:is(Geom, {'LineString', [[101.0,0.0],[102.0,1.0]]},
        "Transform LineString from GeoJSON to a GeoCouch geometry").

test_geojsongeom_to_geocouch_geometrycollection() ->
    Geojson = [{<<"type">>,<<"GeometryCollection">>},
                {<<"geometries">>,
                 [{[{<<"type">>,<<"Point">>},
                    {<<"coordinates">>,[100.0,0.0]}]},
                  {[{<<"type">>,<<"LineString">>},
                    {<<"coordinates">>,[[101.0,0.0],[102.0,1.0]]}]}]}],
    Geom = ?MOD:geojsongeom_to_geocouch(Geojson),
    etap:is(Geom, {'GeometryCollection', [
            {'Point', [100.0,0.0]},
            {'LineString', [[101.0,0.0],[102.0,1.0]]}]},
        "Transform GeometryCollection from GeoJSON to a GeoCouch geometry").

test_geojsongeom_to_geocouch_nested_geometrycollection() ->
    Geojson = [{<<"type">>,<<"GeometryCollection">>},
                {<<"geometries">>,
                 [{[{<<"type">>,<<"GeometryCollection">>},
                  {<<"geometries">>,
                   [{[{<<"type">>,<<"Point">>},
                    {<<"coordinates">>,[100.0,0.0]}]},
                    {[{<<"type">>,<<"LineString">>},
                      {<<"coordinates">>,[[101.0,0.0],[102.0,1.0]]}]}]}]}
                 ]}],
    Geom = ?MOD:geojsongeom_to_geocouch(Geojson),
    etap:is(Geom, {'GeometryCollection', [{'GeometryCollection', [
            {'Point', [100.0,0.0]},
            {'LineString', [[101.0,0.0],[102.0,1.0]]}]}]},
        "Transform nested GeometryCollection from GeoJSON to a "
        "GeoCouch geometry").


test_geocouch_to_geojsongeom_point() ->
    Geom = {'Point', [100.0,0.0]},
    Geojson = ?MOD:geocouch_to_geojsongeom(Geom),
    etap:is(Geojson,
        {[{<<"type">>,'Point'}, {<<"coordinates">>,[100.0,0.0]}]},
        "Transform Point from GeoCouch geometry to GeoJSON").

test_geocouch_to_geojsongeom_linestring() ->
    Geom = {'LineString', [[101.0,0.0],[102.0,1.0]]},
    Geojson = ?MOD:geocouch_to_geojsongeom(Geom),
    etap:is(Geojson,
        {[{<<"type">>,'LineString'},
                {<<"coordinates">>,[[101.0,0.0],[102.0,1.0]]}]},
        "Transform LineString from GeoCouch geometry to GeoJSON").

test_geocouch_to_geojsongeom_geometrycollection() ->
    Geom = {'GeometryCollection', [
            {'Point', [100.0,0.0]},
            {'LineString', [[101.0,0.0],[102.0,1.0]]}]},
    Geojson = ?MOD:geocouch_to_geojsongeom(Geom),
    etap:is(Geojson,
        {[{<<"type">>,'GeometryCollection'},
                {"geometries",
                 [{[{<<"type">>,'Point'},
                    {<<"coordinates">>,[100.0,0.0]}]},
                  {[{<<"type">>,'LineString'},
                    {<<"coordinates">>,[[101.0,0.0],[102.0,1.0]]}]}]}]},
        "Transform GeometryCollection from GeoCouch geometry to GeoJSON").

test_geocouch_to_geojsongeom_nested_geometrycollection() ->
    Geom = {'GeometryCollection', [{'GeometryCollection', [
            {'Point', [100.0,0.0]},
            {'LineString', [[101.0,0.0],[102.0,1.0]]}]}]},
    Geojson = ?MOD:geocouch_to_geojsongeom(Geom),
    etap:is(Geojson,
        {[{<<"type">>,'GeometryCollection'},
                {"geometries",
                 [{[{<<"type">>,'GeometryCollection'},
                  {"geometries",
                   [{[{<<"type">>,'Point'},
                    {<<"coordinates">>,[100.0,0.0]}]},
                    {[{<<"type">>,'LineString'},
                      {<<"coordinates">>,[[101.0,0.0],[102.0,1.0]]}]}]}]}
                 ]}]},
        "Transform nested GeometryCollection from GeoCouch geometry "
        "to GeoJSON").
