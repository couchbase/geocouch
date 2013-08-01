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
    lists:foldl(fun({K, V}, Acc) ->
        parse_qs(K, V, Acc)
    end, #spatial_query_args{}, couch_httpd:qs(Req)).


parse_qs(Key, Val, Args) ->
    case Key of
    "" ->
        Args;
    % bbox is for legacy support. Use start_range and end_range instead.
    "bbox" ->
        {W, S, E, N} = list_to_tuple(?JSON_DECODE("[" ++ Val ++ "]")),
        Args#spatial_query_args{range=[{W, E}, {S, N}]};
    "stale" when Val == "ok" orelse Val == "true"->
        Args#spatial_query_args{stale=ok};
    "stale" when Val == "false" ->
        Args#spatial_query_args{stale=false};
    "stale" when Val == "update_after" ->
        Args#spatial_query_args{stale=update_after};
    "stale" ->
        throw({query_parse_error,
            <<"stale only available as stale=ok, stale=update_after or "
              "stale=false">>});
    "limit" ->
        Args#spatial_query_args{limit=parse_int(Val)};
    "skip" ->
        Args#spatial_query_args{skip=parse_int(Val)};
    "start_range" ->
        case Args#spatial_query_args.range of
        %nil ->
        [] ->
            Args#spatial_query_args{range=?JSON_DECODE(Val)};
        % end_range already set the range
        Range ->
            Args#spatial_query_args{range=merge_range(?JSON_DECODE(Val), Range)}
        end;
    "end_range" ->
        case Args#spatial_query_args.range of
        %nil ->
        [] ->
            Args#spatial_query_args{range=?JSON_DECODE(Val)};
        % start_range already set the range
        Range ->
            Args#spatial_query_args{range=merge_range(Range, ?JSON_DECODE(Val))}
        end;
    "debug" ->
        Args#spatial_query_args{debug=parse_bool_param(Val)};
    _ ->
        BKey = list_to_binary(Key),
        BVal = list_to_binary(Val),
        Args#spatial_query_args{extra=[{BKey, BVal} | Args#spatial_query_args.extra]}
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

% Merge the start_range and stop_range values into one list
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
