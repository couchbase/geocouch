#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable

% Copyright 2014-Present Couchbase, Inc.
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

-include_lib("../include/couch_spatial.hrl").


main(_) ->
    test_util:init_code_path(),

    etap:plan(36),
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
    etap:diag("Testing query parameters parsing of spatial views"),

    test_spatial_no_params(),
    test_spatial_params_bbox(),
    test_spatial_params_stale(),
    test_spatial_params_limit(),
    test_spatial_params_skip(),
    test_spatial_params_range(),
    test_spatial_params_debug(),
    test_spatial_params_extra(),
    ok.


test_spatial_no_params() ->
    Result = spatial_http:parse_qs("", nil, #spatial_query_args{}),
    etap:is(Result, #spatial_query_args{}, "No argument parsed as expected").


test_spatial_params_bbox() ->
    #spatial_query_args{
        range = Range
    } = spatial_http:parse_qs("bbox", "-1,-2,3,4", #spatial_query_args{}),
    etap:is(Range, [{-1, 3}, {-2, 4}], "Bbox parsed correctly"),

    Error = <<"bounding box was invalid, it needs to be bbox=W,S,E,N "
              "where each direction is a number. Your bounding box was `">>,
    ErrorVal1 = "-1,-2,3",
    etap:throws_ok(
      fun() -> spatial_http:parse_qs(
                 "bbox", ErrorVal1, #spatial_query_args{}) end,
      {query_parse_error, iolist_to_binary([Error, ErrorVal1, "`"])},
      "error when bbox had 3 numbers only"),

    ErrorVal2 = "-1,-2,3,4,5",
    etap:throws_ok(
      fun() -> spatial_http:parse_qs(
                 "bbox", ErrorVal2, #spatial_query_args{}) end,
      {query_parse_error, iolist_to_binary([Error, ErrorVal2, "`"])},
      "error when bbox had 5 numbers"),

    ErrorVal3 = "[-1,-2,3,4]",
    etap:throws_ok(
      fun() -> spatial_http:parse_qs(
                 "bbox", ErrorVal3, #spatial_query_args{}) end,
      {query_parse_error, iolist_to_binary([Error, ErrorVal3, "`"])},
      "error when bbox was supplied as array"),

    ErrorVal4 = "-1,nan,3,4",
    etap:throws_ok(
      fun() -> spatial_http:parse_qs(
                 "bbox", ErrorVal4, #spatial_query_args{}) end,
      {query_parse_error, iolist_to_binary([Error, ErrorVal4, "`"])},
      "error when bbox doesn't contain other things than numbers (a)"),

    ErrorVal5 = "-1,true,3,4",
    etap:throws_ok(
      fun() -> spatial_http:parse_qs(
                 "bbox", ErrorVal5, #spatial_query_args{}) end,
      {query_parse_error, iolist_to_binary([Error, ErrorVal5, "`"])},
      "error when bbox doesn't contain other things than numbers (b)"),

    ErrorVal6 = "-1,\"str\",3,4",
    etap:throws_ok(
      fun() -> spatial_http:parse_qs(
                 "bbox", ErrorVal6, #spatial_query_args{}) end,
      {query_parse_error, iolist_to_binary([Error, ErrorVal6, "`"])},
      "error when bbox doesn't contain other things than numbers (c)").


test_spatial_params_stale() ->
    #spatial_query_args{
        stale = Stale1
    } = spatial_http:parse_qs("stale", "ok", #spatial_query_args{}),
    etap:is(Stale1, ok, "stale=ok parsed correctly"),

    #spatial_query_args{
        stale = Stale2
    } = spatial_http:parse_qs("stale", "true", #spatial_query_args{}),
    etap:is(Stale2, ok, "stale=true parsed correctly"),

    #spatial_query_args{
        stale = Stale3
    } = spatial_http:parse_qs("stale", "false", #spatial_query_args{}),
    etap:is(Stale3, false, "stale=false parsed correctly"),

    #spatial_query_args{
        stale = Stale4
    } = spatial_http:parse_qs("stale", "update_after", #spatial_query_args{}),
    etap:is(Stale4, update_after, "stale=update_after parsed correctly"),

    etap:throws_ok(
      fun() -> spatial_http:parse_qs(
                 "stale", "notvalid", #spatial_query_args{}) end,
      {query_parse_error, <<"stale only available as stale=ok, "
                            "stale=update_after or stale=false and not as "
                            "stale=notvalid">>},
      "stale with invalid parameter").


test_spatial_params_limit() ->
    #spatial_query_args{
        limit = Limit
    } = spatial_http:parse_qs("limit", "5", #spatial_query_args{}),
    etap:is(Limit, 5, "limit=5 parsed correctly"),

    etap:throws_ok(
      fun() -> spatial_http:parse_qs(
                 "limit", "notvalid", #spatial_query_args{}) end,
      {query_parse_error, <<"Invalid value for integer parameter: "
                            "\"notvalid\"">>},
      "limit with invalid parameter (a)"),

    etap:throws_ok(
      fun() -> spatial_http:parse_qs(
                 "limit", "6.2", #spatial_query_args{}) end,
      {query_parse_error, <<"Invalid value for integer parameter: "
                            "\"6.2\"">>},
      "limit with invalid parameter (b)").


test_spatial_params_skip() ->
    #spatial_query_args{
        skip = Skip
    } = spatial_http:parse_qs("skip", "8", #spatial_query_args{}),
    etap:is(Skip, 8, "skip=8 parsed correctly"),

    etap:throws_ok(
      fun() -> spatial_http:parse_qs(
                 "skip", "notvalid", #spatial_query_args{}) end,
      {query_parse_error, <<"Invalid value for integer parameter: "
                            "\"notvalid\"">>},
      "skip with invalid parameter (a)"),

    etap:throws_ok(
      fun() -> spatial_http:parse_qs(
                 "skip", "1.87534", #spatial_query_args{}) end,
      {query_parse_error, <<"Invalid value for integer parameter: "
                            "\"1.87534\"">>},
      "skip with invalid parameter (b)").


test_spatial_params_range() ->
    SpatialArgs1 = #spatial_query_args{
        range = Range1
    } = spatial_http:parse_qs("start_range", "[1,2,3]", #spatial_query_args{}),
    etap:is(Range1, [1, 2, 3], "start_range=[1,2,3] parsed correctly"),

    SpatialArgs2 = #spatial_query_args{
        range = Range2
    } = spatial_http:parse_qs("end_range", "[4,5]", #spatial_query_args{}),
    etap:is(Range2, [4, 5], "end_range=[4,5] parsed correctly"),

    #spatial_query_args{
        range = Range3
    } = spatial_http:parse_qs("end_range", "[4,5,6]", SpatialArgs1),
    etap:is(Range3, [{1, 4}, {2, 5}, {3, 6}],
            "start_range=[1,2,3] and end_range=[4,5,6] parsed correctly"),

    #spatial_query_args{
        range = Range4
    } = spatial_http:parse_qs("start_range", "[7,8]", SpatialArgs2),
    etap:is(Range4, [{7, 4}, {8, 5}],
            "start_range=[7,8] and end_range=[4,5] parsed correctly"),

    #spatial_query_args{
        range = Range5
    } = spatial_http:parse_qs("start_range", "[4,null]",
                              #spatial_query_args{}),
    etap:is(Range5, [4, null], "start_range=[4,null] parsed correctly"),

    #spatial_query_args{
        range = Range6
    } = spatial_http:parse_qs("end_range", "[4,null,8]",
                              #spatial_query_args{}),
    etap:is(Range6, [4, null, 8], "end_range=[4,null,8] parsed correctly"),

    #spatial_query_args{
        range = Range7
    } = spatial_http:parse_qs("end_range", "[4,null,8]", SpatialArgs1),
    etap:is(Range7, [{1, 4}, {2, nil}, {3, 8}],
            "start_range=[1,2,3] and end_range=[4,null,8] parsed correctly"),

    #spatial_query_args{
        range = Range8
    } = spatial_http:parse_qs("start_range", "[4,null]", SpatialArgs2),
    etap:is(Range8, [{4, 4}, {nil, 5}],
            "start_range=[4,null] and end_range=[4,5] parsed correctly"),

    etap:throws_ok(
      fun() -> spatial_http:parse_qs("end_range", "[4,5]", SpatialArgs1) end,
      {query_parse_error,
       <<"start_range and end_range must have the same number of dimensions. "
         "Your ranges were [1,2,3] and [4,5]">>},
      "end_range has less dimensions that start_range"),

    etap:throws_ok(
      fun() -> spatial_http:parse_qs("start_range", "[10]", SpatialArgs2) end,
      {query_parse_error,
       <<"start_range and end_range must have the same number of dimensions. "
         "Your ranges were [10] and [4,5]">>},
      "start_range has less dimensions that end_range"),

    etap:throws_ok(
      fun() -> spatial_http:parse_qs(
                 "start_range", "[true]", #spatial_query_args{}) end,
      {query_parse_error,
       <<"range must be an array containing numbers or `null`s. "
         "Your range was `[true]`">>},
      "start_range with invalid input"),

    etap:throws_ok(
      fun() -> spatial_http:parse_qs(
                 "end_range", "[\"str\", 6]", #spatial_query_args{}) end,
      {query_parse_error,
       <<"range must be an array containing numbers or `null`s. "
         "Your range was `[\"str\", 6]`">>},
      "end_range with invalid input").


test_spatial_params_debug() ->
    #spatial_query_args{
        debug = Debug1
    } = spatial_http:parse_qs("debug", "true", #spatial_query_args{}),
    etap:is(Debug1, true, "debug=true parsed correctly"),

    #spatial_query_args{
        debug = Debug2
    } = spatial_http:parse_qs("debug", "false", #spatial_query_args{}),
    etap:is(Debug2, false, "debug=false parsed correctly"),

    etap:throws_ok(
      fun() -> spatial_http:parse_qs(
                 "debug", "notvalid", #spatial_query_args{}) end,
      {query_parse_error, <<"Invalid boolean parameter: \"notvalid\"">>},
      "debug with invalid parameter").


test_spatial_params_extra() ->
    SpatialArgs1 = #spatial_query_args{
        extra = Extra1
    } = spatial_http:parse_qs("extra", "something", #spatial_query_args{}),
    etap:is(Extra1, [{<<"extra">>, <<"something">>}],
            "extra=something parsed correctly"),

    #spatial_query_args{
        extra = Extra2
    } = spatial_http:parse_qs("several", "5", SpatialArgs1),
    etap:is(Extra2,
            [{<<"several">>, <<"5">>}, {<<"extra">>, <<"something">>}],
            "extra=something and several=5 parsed correctly").

