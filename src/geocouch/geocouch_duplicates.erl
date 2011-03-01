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

-module(geocouch_duplicates).

% This module contains functions that are needed by GeoCouch but that
% are not exported by CouchDB. Here's the place to put the code from
% those functions.

-include("couch_db.hrl").
-export([get_os_process/1, start_list_resp/6, send_non_empty_chunk/2]).

% Needed for get_os_process/1
-record(proc, {
    pid,
    lang,
    ddoc_keys = [],
    prompt_fun,
    set_timeout_fun,
    stop_fun
}).

% From couch_query_servers.erl
get_os_process(Lang) ->
    case gen_server:call(couch_query_servers, {get_proc, Lang}) of
    {ok, Proc, {QueryConfig}} ->
        case (catch couch_query_servers:proc_prompt(Proc, [<<"reset">>, {QueryConfig}])) of
        true ->
            proc_set_timeout(Proc, couch_util:get_value(<<"timeout">>, QueryConfig)),
            link(Proc#proc.pid),
            gen_server:call(couch_query_servers, {unlink_proc, Proc#proc.pid}),
            Proc;
        _ ->
            catch proc_stop(Proc),
            get_os_process(Lang)
        end;
    Error ->
        throw(Error)
    end.
% Needed for get_os_process/1
proc_set_timeout(Proc, Timeout) ->
    {Mod, Func} = Proc#proc.set_timeout_fun,
    apply(Mod, Func, [Proc#proc.pid, Timeout]).
% Needed for get_os_process/1
proc_stop(Proc) ->
    {Mod, Func} = Proc#proc.stop_fun,
    apply(Mod, Func, [Proc#proc.pid]).


% From couch_httpd_show
start_list_resp(QServer, LName, Req, Db, Head, Etag) ->
    JsonReq = couch_httpd_external:json_req_obj(Req, Db),
    [<<"start">>,Chunks,JsonResp] = couch_query_servers:ddoc_proc_prompt(QServer,
        [<<"lists">>, LName], [Head, JsonReq]),
    JsonResp2 = apply_etag(JsonResp, Etag),
    #extern_resp_args{
        code = Code,
        ctype = CType,
        headers = ExtHeaders
    } = couch_httpd_external:parse_external_response(JsonResp2),
    JsonHeaders = couch_httpd_external:default_or_content_type(CType, ExtHeaders),
    {ok, Resp} = couch_httpd:start_chunked_response(Req, Code, JsonHeaders),
    {ok, Resp, ?b2l(?l2b(Chunks))}.
% Needed for start_list_resp/6
apply_etag({ExternalResponse}, CurrentEtag) ->
    % Here we embark on the delicate task of replacing or creating the
    % headers on the JsonResponse object. We need to control the Etag and
    % Vary headers. If the external function controls the Etag, we'd have to
    % run it to check for a match, which sort of defeats the purpose.
    case couch_util:get_value(<<"headers">>, ExternalResponse, nil) of
    nil ->
        % no JSON headers
        % add our Etag and Vary headers to the response
        {[{<<"headers">>, {[{<<"Etag">>, CurrentEtag}, {<<"Vary">>, <<"Accept">>}]}} | ExternalResponse]};
    JsonHeaders ->
        {[case Field of
        {<<"headers">>, JsonHeaders} -> % add our headers
            JsonHeadersEtagged = couch_util:json_apply_field({<<"Etag">>, CurrentEtag}, JsonHeaders),
            JsonHeadersVaried = couch_util:json_apply_field({<<"Vary">>, <<"Accept">>}, JsonHeadersEtagged),
            {<<"headers">>, JsonHeadersVaried};
        _ -> % skip non-header fields
            Field
        end || Field <- ExternalResponse]}
    end.


% From couch_httpd_show
send_non_empty_chunk(Resp, Chunk) ->
    case Chunk of
        [] -> ok;
        _ -> couch_httpd:send_chunk(Resp, Chunk)
    end.
