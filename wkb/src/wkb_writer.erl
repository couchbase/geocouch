% Copyright 2012-2014 Cloudant
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
-module(wkb_writer).

-include("wkb.hrl").

-export([geojson_to_wkb/1]).

geojson_to_wkb(Data) when is_tuple(Data)->
	{ok, _Dims, Wkb} = parse_geom(Data),
	{ok, Wkb};

geojson_to_wkb(Data) when is_list(Data) ->
	geojson_to_wkb(list_to_binary(Data));

geojson_to_wkb(Json) ->    
	JsonData = jiffy:decode(Json),
	{ok, _Dims, Wkb} = parse_geom(JsonData),
	{ok, Wkb}.

% private api 
%
% The default signedness is unsigned.
% The default endianness is big.
%
parse_geom({JsonData}) ->
	% geometry collection is a special case
	case lists:keyfind(<<"coordinates">>, 1, JsonData) of 
		{_, Coords} ->
			{_, Type} = lists:keyfind(<<"type">>, 1, JsonData),
			parse_geom(Type, Coords);
		_ ->
			case lists:keyfind(<<"geometries">>, 1, JsonData) of 
				{_, Geometries} ->
					{_, Type} = lists:keyfind(<<"type">>, 1, JsonData),
					parse_geom(Type, Geometries);
				_ ->
					throw({error, "error parsing geometries"})
			end
	end.

parse_geom(?POINT, Coords) ->
	{ok, Dims, _Cnt, NewAcc} = make_pts([Coords], 0, <<>>),
	Type = create_ewb_type(?wkbPoint, length(Coords)),
	{ok, Dims, <<0:8, Type:32, NewAcc/binary>>};

parse_geom(?LINESTRING, Coords) ->
	{ok, Dims, Num, NewAcc} = make_pts(Coords, 0, <<>>),
	Type = create_ewb_type(?wkbLineString, Dims),
	{ok, Dims, <<0:8, Type:32, Num:32, NewAcc/binary>>};

parse_geom(?POLYGON, Coords) ->
	{ok, Dims, Num, NewAcc} = make_linear_ring(Coords, 0, <<>>),
	Type = create_ewb_type(?wkbPolygon, Dims),
	{ok, Dims, <<0:8, Type:32, Num:32, NewAcc/binary>>};

parse_geom(?MULTIPOINT, Coords) ->
	{ok, Dims, Num, NewAcc} = parse_nested_geoms(Coords, ?POINT, 0, <<>>),
	Type = create_ewb_type(?wkbMultiPoint, Dims),
	{ok, Dims, <<0:8, Type:32, Num:32, NewAcc/binary>>};

parse_geom(?MULTILINESTRING, Coords) ->
	{ok, Dims, Num, NewAcc} = parse_nested_geoms(Coords, ?LINESTRING, 0, <<>>),
	Type = create_ewb_type(?wkbMultiLineString, Dims),
	{ok, Dims, <<0:8, Type:32, Num:32, NewAcc/binary>>};

parse_geom(?MULTIPOLYGON, Coords) ->
	{ok, Dims, Num, NewAcc} = parse_nested_geoms(Coords, ?POLYGON, 0, <<>>),
	Type = create_ewb_type(?wkbMultiPolygon, Dims),
	{ok, Dims, <<0:8, Type:32, Num:32, NewAcc/binary>>};

parse_geom(?GEOMETRYCOLLECTION, Geometries) ->
	{Dims, Num, NewAcc} = lists:foldl(fun(C, {_, Cntr, Acc1}) ->
			{ok, Dims, Acc2} = parse_geom(C),
			{Dims, Cntr + 1, <<Acc1/binary, Acc2/binary>>}
		end, {0, 0, <<>>}, Geometries),
	Type = create_ewb_type(?wkbGeometryCollection, Dims),
	{ok, Dims, <<0:8, Type:32, Num:32, NewAcc/binary>>};

parse_geom(false, _Data) ->
	throw({error, "error parsing geojson, geometry type not defined."}).

parse_nested_geoms(GeomList, Type, Cntr, Acc) ->
  parse_nested_geoms(GeomList, Type, 0, Cntr, Acc).

parse_nested_geoms([], _Type, Dims, Cntr, Acc) ->
	{ok, Dims, Cntr, Acc};

parse_nested_geoms([Coord | Rem], Type, _, Cntr, Acc) ->
	{ok, Dims, Acc1} = parse_geom(Type, Coord),
	parse_nested_geoms(Rem, Type, Dims, Cntr + 1, <<Acc/binary, Acc1/binary>>).

make_linear_ring(CoordList, Cntr, Acc) ->
  make_linear_ring(CoordList, 0, Cntr, Acc).

make_linear_ring([], Dims, Cntr, Acc) ->
	{ok, Dims, Cntr, Acc};

make_linear_ring([Coords | Rem], _, Cntr, Acc) ->
	{ok, Dims, Num, NewAcc} = make_pts(Coords, 0, <<>>),
	make_linear_ring(Rem, Dims, Cntr + 1, <<Acc/binary, Num:32, NewAcc/binary>>).

make_pts(CoordList, Cntr, Acc) ->
	make_pts(CoordList, 0, Cntr, Acc).

make_pts([], Dims, Cntr, Acc) ->
	{ok, Dims, Cntr, Acc};

make_pts([C|_] = CoordList, 0, Cntr, Acc) ->
	make_pts(CoordList, length(C), Cntr, Acc);

make_pts([Coords | Rem], Dims, Cntr, Acc) ->
	NewAcc = lists:foldl(fun(P, Acc2) ->
		<<Acc2/binary, P:64/float>>
	end, Acc, Coords),
	make_pts(Rem, Dims, Cntr + 1, NewAcc).

create_ewb_type(Type, Dims) ->
	case Dims of 
		2 ->
			Type;
		3 ->
		  Type bor ?wkbZ;
		4 ->
		  (Type bor ?wkbZ) bor ?wkbM;
		_ ->
			throw({error, "error setting WKB type"})
	end.



