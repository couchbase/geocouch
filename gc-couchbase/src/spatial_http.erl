% Copyright 2013-Present Couchbase, Inc.
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

-module(spatial_http).

-export([handle_req/1, parse_qs/1]).

-include("couch_db.hrl").
-include("couch_spatial.hrl").
-include_lib("couch_index_merger/include/couch_index_merger.hrl").


handle_req(#httpd{path_parts = [<<"_spatial_view">>, SetName, <<"_design">>,
        DDocId, <<"_info">>], method = 'GET'} = Req) ->
    {ok, Info} = couch_set_view:get_group_info(
        spatial_view, SetName, <<"_design/", DDocId/binary>>, prod),
    couch_httpd:send_json(Req, 200, {Info});

% For spatial view merger
handle_req(#httpd{method = 'GET'} = Req) ->
    Views = couch_index_merger_validation:views_param(
        couch_httpd:qs_json_value(Req, "views", nil)),
    DDocRevision = couch_index_merger_validation:revision_param(
        couch_httpd:qs_json_value(Req, <<"ddoc_revision">>, nil)),
    MergeParams0 = #index_merge{
        indexes = Views,
        ddoc_revision = DDocRevision,
        extra = nil
    },
    MergeParams1 = couch_httpd_view_merger:apply_http_config(
        Req, [], MergeParams0),
    couch_index_merger:query_index(spatial_merger, MergeParams1, Req);

% For spatial view merger
handle_req(#httpd{method = 'POST'} = Req) ->
    {Props} = couch_httpd:json_body_obj(Req),
    Views = couch_index_merger_validation:views_param(
        couch_util:get_value(<<"views">>, Props)),
    DDocRevision = couch_index_merger_validation:revision_param(
        couch_util:get_value(<<"ddoc_revision">>, Props, nil)),
    MergeParams0 = #index_merge{
        indexes = Views,
        ddoc_revision = DDocRevision,
        extra = nil
    },
    MergeParams1 = couch_httpd_view_merger:apply_http_config(
        Req, Props, MergeParams0),
    couch_index_merger:query_index(spatial_merger, MergeParams1, Req);

handle_req(Req) ->
    couch_httpd:send_method_not_allowed(Req, "GET,POST").


parse_qs(Req) ->
    QueryArgs = lists:foldl(fun({K, V}, Acc) ->
        parse_qs(K, V, Acc)
    end, #spatial_query_args{}, couch_httpd:qs(Req)),
    lists:foreach(fun
        ({_, _}) -> ok;
        (_) ->
             throw({query_parse_error,
                 <<"start_range and end_range must be specified">>})
    end, QueryArgs#spatial_query_args.range),
    QueryArgs.


parse_qs(Key, Val, Args) ->
    case Key of
    "" ->
        Args;
    % bbox is for legacy support. Use start_range and end_range instead.
    "bbox" ->
        Error = iolist_to_binary(
                  [<<"bounding box was invalid, it needs to be bbox=W,S,E,N "
                     "where each direction is a number. "
                     "Your bounding box was `">>,
                   Val, <<"`">>]),
        try
            {W, S, E, N} = list_to_tuple(?JSON_DECODE("[" ++ Val ++ "]")),
            case lists:all(fun(Num) -> is_number(Num) end, [W, S, E, N]) of
            true ->
                Args#spatial_query_args{range=[{W, E}, {S, N}]};
            false ->
                throw({query_parse_error, Error})
            end
        catch
        error:{badmatch, _} ->
            throw({query_parse_error, Error});
        throw:{invalid_json, _} ->
            throw({query_parse_error, Error})
        end;
    "stale" when Val == "ok" orelse Val == "true"->
        Args#spatial_query_args{stale=ok};
    "stale" when Val == "false" ->
        Args#spatial_query_args{stale=false};
    "stale" when Val == "update_after" ->
        Args#spatial_query_args{stale=update_after};
    "stale" ->
        throw({query_parse_error,
            iolist_to_binary(
              [<<"stale only available as stale=ok, stale=update_after or "
                 "stale=false and not as stale=">>, Val])});
    "limit" ->
        Args#spatial_query_args{limit=parse_int(Val)};
    "skip" ->
        Args#spatial_query_args{skip=parse_int(Val)};
    "start_range" ->
        Decoded = parse_range(Val),
        case Args#spatial_query_args.range of
        [] ->
            Args#spatial_query_args{range=Decoded};
        % end_range already set the range
        Range ->
            Args#spatial_query_args{range=merge_range(Decoded, Range)}
        end;
    "end_range" ->
        Decoded = parse_range(Val),
        case Args#spatial_query_args.range of
        [] ->
            Args#spatial_query_args{range=Decoded};
        % start_range already set the range
        Range ->
            Args#spatial_query_args{range=merge_range(Range, Decoded)}
        end;
    "debug" ->
        Args#spatial_query_args{debug=parse_bool_param(Val)};
    _ ->
        BKey = list_to_binary(Key),
        BVal = list_to_binary(Val),
        case Args#spatial_query_args.extra of
        nil ->
            Args#spatial_query_args{extra=[{BKey, BVal}]};
        _ ->
            Args#spatial_query_args{
                extra=[{BKey, BVal} | Args#spatial_query_args.extra]}
        end
    end.


% Verbatim copy from couch_mrview_http
parse_int(Val) ->
    case (catch list_to_integer(Val)) of
    IntVal when is_integer(IntVal) ->
        IntVal;
    _ ->
        Msg = io_lib:format("Invalid value for integer parameter: ~p", [Val]),
        throw({query_parse_error, ?l2b(Msg)})
    end.

% Verbatim copy from couch_httpd_view
parse_bool_param(Val) ->
    case string:to_lower(Val) of
    "true" -> true;
    "false" -> false;
    _ ->
        Msg = io_lib:format("Invalid boolean parameter: ~p", [Val]),
        throw({query_parse_error, ?l2b(Msg)})
    end.


parse_range(Val) ->
    Decoded = ?JSON_DECODE(Val),
    case is_list(Decoded) andalso
        lists:all(fun(Num) -> is_number(Num) orelse Num =:= null end, Decoded) of
    true ->
        Decoded;
    false ->
        throw({query_parse_error,
            iolist_to_binary(
              [<<"range must be an array containing numbers or `null`s. "
                 "Your range was `">>,
               Val, "`"])})
    end.


% Merge the start_range and stop_range values into one list
merge_range(RangeA, RangeB) when length(RangeA) =/= length(RangeB) ->
    Msg = io_lib:format(
            "start_range and end_range must have the same number of "
            "dimensions. Your ranges were ~w and ~w",
            [RangeA, RangeB]),
    throw({query_parse_error, ?l2b(Msg)});
merge_range(RangeA, RangeB) ->
    lists:zipwith(
        % `null` is a wildcard which will return all values
        fun(null, null) ->
            {nil, nil};
        % `null` means an open range here
        (A, null) ->
            {A, nil};
        (null, B) ->
            {nil, B};
        (A, B) ->
            {A, B}
    end, RangeA, RangeB).
