% Copyright 2015 Couchbase Inc.
%
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
-module(wkb_reader).

-include("wkb.hrl").

-export([wkb_to_geojson/1]).

-type coords() :: [float()] | [[float()]] | [[[float()]]] | [[[[float()]]]].
-type geojson() :: {[{binary(), binary() | coords()}]}.


-spec wkb_to_geojson(binary()) -> {ok, geojson()}.
wkb_to_geojson(Data) ->
    {GeoJson, <<>>} = parse_wkb(Data),
    {ok, GeoJson}.



-spec parse_wkb(binary()) -> {geojson(), binary()}.
parse_wkb(<<?BIG_ENDIAN:8, ?wkbPoint:32, _/binary>> = Wkb) ->
    {Coords, Rest} = parse_coords(Wkb),
    {create_json(?POINT, Coords), Rest};

parse_wkb(<<?BIG_ENDIAN:8, ?wkbLineString:32, _/binary>> = Wkb) ->
    {Coords, Rest} = parse_coords(Wkb),
    {create_json(?LINESTRING, Coords), Rest};

parse_wkb(<<?BIG_ENDIAN:8, ?wkbPolygon:32, _/binary>> = Wkb) ->
    {Coords, Rest} = parse_coords(Wkb),
    {create_json(?POLYGON, Coords), Rest};

parse_wkb(<<?BIG_ENDIAN:8, ?wkbMultiPoint:32, _/binary>> = Wkb) ->
    {Coords, Rest} = parse_coords(Wkb),
    {create_json(?MULTIPOINT, Coords), Rest};

parse_wkb(<<?BIG_ENDIAN:8, ?wkbMultiLineString:32, _/binary>> = Wkb) ->
    {Coords, Rest} = parse_coords(Wkb),
    {create_json(?MULTILINESTRING, Coords), Rest};

parse_wkb(<<?BIG_ENDIAN:8, ?wkbMultiPolygon:32, _/binary>> = Wkb) ->
    {Coords, Rest} = parse_coords(Wkb),
    {create_json(?MULTIPOLYGON, Coords), Rest};

parse_wkb(<<?BIG_ENDIAN:8, ?wkbGeometryCollection:32, NumWkbGeometries:32,
            Rest/binary>>) ->
    Geoms = parse_wkb_multi(Rest, NumWkbGeometries),
    {{[{<<"type">>, ?GEOMETRYCOLLECTION}, {<<"geometries">>, Geoms}]}, <<>>}.


-spec parse_coords(binary()) -> {coords(), binary()}.
parse_coords(<<?BIG_ENDIAN:8, ?wkbPoint:32, X:64/float, Y:64/float,
            Rest/binary>>) ->
    {[X, Y], Rest};

parse_coords(<<?BIG_ENDIAN:8, ?wkbLineString:32, NumPoints:32,
            Points:NumPoints/binary-unit:128, Rest/binary>>) ->
    {parse_points(Points), Rest};

parse_coords(<<?BIG_ENDIAN:8, ?wkbPolygon:32, NumRings:32, Rest/binary>>) ->
    parse_rings(Rest, NumRings);

parse_coords(<<?BIG_ENDIAN:8, ?wkbMultiPoint:32, NumWkbPoints:32,
            Rest/binary>>) ->
    parse_coords_multi(Rest, NumWkbPoints);

parse_coords(<<?BIG_ENDIAN:8, ?wkbMultiLineString:32, NumWkbLineStrings:32,
            Rest/binary>>) ->
    parse_coords_multi(Rest, NumWkbLineStrings);

parse_coords(<<?BIG_ENDIAN:8, ?wkbMultiPolygon:32, NumWkbPolygons:32,
            Rest/binary>>) ->
    parse_coords_multi(Rest, NumWkbPolygons).


-spec parse_points(binary()) -> [[float()]].
parse_points(Points) ->
    [[X, Y] || <<X:64/float, Y:64/float>> <= Points].


-spec parse_rings(binary(), non_neg_integer()) -> {[[[float()]]], binary()}.
parse_rings(Rings, NumRings) ->
    parse_rings(Rings, NumRings, []).

-spec parse_rings(binary(), non_neg_integer(), [[[float()]]]) ->
                         {[[[float()]]], binary()}.
parse_rings(Rest, NumRings, Acc) when length(Acc) =:= NumRings ->
    {lists:reverse(Acc), Rest};

parse_rings(<<NumPoints:32, Points:NumPoints/binary-unit:128, Rest/binary>>,
            NumRings, Acc) ->
    Parsed = parse_points(Points),
    parse_rings(Rest, NumRings, [Parsed | Acc]).


-spec parse_coords_multi(binary(), non_neg_integer()) -> {coords(), binary()}.
parse_coords_multi(WkbMulti, NumWkbMulti) ->
    parse_coords_multi(WkbMulti, NumWkbMulti, []).

-spec parse_coords_multi(binary(), non_neg_integer(), coords()) ->
                                {coords(), binary()}.
parse_coords_multi(Rest, NumWkbMulti, Acc) when length(Acc) =:= NumWkbMulti ->
    {lists:reverse(Acc), Rest};

parse_coords_multi(<<WkbMulti/binary>>, NumWkbMulti, Acc) ->
    {Parsed, Rest} = parse_coords(WkbMulti),
    parse_coords_multi(Rest, NumWkbMulti, [Parsed | Acc]).


-spec parse_wkb_multi(binary(), non_neg_integer()) -> [geojson()].
parse_wkb_multi(WkbMulti, NumWkbMulti) ->
    parse_wkb_multi(WkbMulti, NumWkbMulti, []).

-spec parse_wkb_multi(binary(), non_neg_integer(), [geojson()]) -> [geojson()].
parse_wkb_multi(<<>>, NumWkbMulti, Acc) when length(Acc) =:= NumWkbMulti ->
    lists:reverse(Acc);

parse_wkb_multi(<<WkbMulti/binary>>, NumWkbMulti, Acc) ->
    {Parsed, Rest} = parse_wkb(WkbMulti),
    parse_wkb_multi(Rest, NumWkbMulti, [Parsed | Acc]).


-spec create_json(binary(), coords()) -> geojson().
create_json(Type, Coordinates) ->
    {[{<<"type">>, Type}, {<<"coordinates">>, Coordinates}]}.
