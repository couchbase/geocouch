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

-module(couch_httpd_spatial_merger).

-export([handle_req/1]).

-include("couch_db.hrl").
-include_lib("couch_index_merger/include/couch_index_merger.hrl").
-include("../lhttpc/lhttpc.hrl").

-import(couch_util, [
    get_value/2,
    get_value/3,
    to_binary/1
]).
-import(couch_httpd, [
    qs_json_value/3
]).


handle_req(#httpd{method = 'GET'} = Req) ->
    Indexes = validate_spatial_param(qs_json_value(Req, "spatial", nil)),
    DDocRevision = couch_index_merger:validate_revision_param(
        qs_json_value(Req, <<"ddoc_revision">>, nil)),
    MergeParams0 = #index_merge{
        indexes = Indexes,
        ddoc_revision = DDocRevision
    },
    MergeParams1 = couch_httpd_view_merger:apply_http_config(
        Req, [], MergeParams0),
    couch_index_merger:query_index(couch_spatial_merger, MergeParams1, Req);

handle_req(#httpd{method = 'POST'} = Req) ->
    couch_httpd:validate_ctype(Req, "application/json"),
    {Props} = couch_httpd:json_body_obj(Req),
    Indexes = validate_spatial_param(get_value(<<"spatial">>, Props)),
    DDocRevision = couch_index_merger:validate_revision_param(
        get_value(<<"ddoc_revision">>, Props, nil)),
    MergeParams0 = #index_merge{
        indexes = Indexes,
        ddoc_revision = DDocRevision
    },
    MergeParams1 = couch_httpd_view_merger:apply_http_config(
        Req, Props, MergeParams0),
    couch_index_merger:query_index(couch_spatial_merger, MergeParams1, Req);

handle_req(Req) ->
    couch_httpd:send_method_not_allowed(Req, "GET,POST").

%% Valid `spatial` example:
%%
%% {
%%   "spatial": {
%%     "localdb1": ["ddocname/spatialname", ...],
%%     "http://server2/dbname": ["ddoc/spatial"],
%%     "http://server2/_spatial_merge": {
%%       "spatial": {
%%         "localdb3": "spatialname", // local to server2
%%         "localdb4": "spatialname"  // local to server2
%%       }
%%     }
%%   }
%% }

validate_spatial_param({[_ | _] = Indexes}) ->
    lists:flatten(lists:map(
        fun({DbName, SpatialName}) when is_binary(SpatialName) ->
            {DDocDbName, DDocId, Vn} = parse_spatial_name(SpatialName),
            #simple_index_spec{
                database = DbName, ddoc_id = DDocId, index_name = Vn,
                ddoc_database = DDocDbName
            };
        ({DbName, SpatialNames}) when is_list(SpatialNames) ->
            lists:map(
                fun(SpatialName) ->
                    {DDocDbName, DDocId, Vn} = parse_spatial_name(SpatialName),
                    #simple_index_spec{
                        database = DbName, ddoc_id = DDocId, index_name = Vn,
                        ddoc_database = DDocDbName
                    }
                end, SpatialNames);
        ({MergeUrl, {[_ | _] = Props} = EJson}) ->
            case (catch lhttpc_lib:parse_url(?b2l(MergeUrl))) of
            #lhttpc_url{} ->
                ok;
            _ ->
                throw({bad_request, "Invalid spatial merge definition object."})
            end,
            case get_value(<<"ddoc_revision">>, Props) of
            undefined ->
                ok;
            _ ->
                Msg = "Nested 'ddoc_revision' specifications are not allowed.",
                throw({bad_request, Msg})
            end,
            case get_value(<<"spatial">>, Props) of
            {[_ | _]} = SubSpatial ->
                SubSpatialSpecs = validate_spatial_param(SubSpatial),
                case lists:any(
                    fun(#simple_index_spec{}) -> true; (_) -> false end,
                    SubSpatialSpecs) of
                true ->
                    ok;
                false ->
                    SubMergeError = io_lib:format("Could not find a"
                        " non-composed spatial spec in the spatial merge"
                        " targeted at `~s`",
                        [couch_index_merger:rem_passwd(MergeUrl)]),
                    throw({bad_request, SubMergeError})
                end,
                #merged_index_spec{url = MergeUrl, ejson_spec = EJson};
            _ ->
                SubMergeError = io_lib:format("Invalid spatial merge"
                    " definition for sub-merge done at `~s`.",
                    [couch_index_merger:rem_passwd(MergeUrl)]),
                throw({bad_request, SubMergeError})
            end;
        (_) ->
            throw({bad_request, "Invalid spatial merge definition object."})
        end, Indexes));

validate_spatial_param(_) ->
    throw({bad_request, <<"`spatial` parameter must be an object with at ",
                          "least 1 property.">>}).

parse_spatial_name(Name) ->
    Tokens = string:tokens(couch_util:trim(?b2l(Name)), "/"),
    case [?l2b(couch_httpd:unquote(Token)) || Token <- Tokens] of
    [DDocName, ViewName] ->
        {nil, <<"_design/", DDocName/binary>>, ViewName};
    [<<"_design">>, DDocName, ViewName] ->
        {nil, <<"_design/", DDocName/binary>>, ViewName};
    [DDocDbName, DDocName, ViewName] ->
        {DDocDbName, <<"_design/", DDocName/binary>>, ViewName};
    [DDocDbName, <<"_design">>, DDocName, ViewName] ->
        {DDocDbName, <<"_design/", DDocName/binary>>, ViewName};
    _ ->
        throw({bad_request, "A `spatial` property must have the shape"
            " `ddoc_name/spatial_name`."})
    end.
