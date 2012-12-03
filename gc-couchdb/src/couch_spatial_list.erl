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

-module(couch_spatial_list).

-include("couch_db.hrl").
-include("couch_spatial.hrl").

-export([handle_view_list_req/3]).
% deprecated API
-export([handle_view_list_req_deprecated/3]).


-record(acc, {
    db,
    req,
    resp,
    query_server,
    list_name,
    etag
}).


handle_view_list_req(#httpd{method=Method}=Req, Db, DDoc)
        when Method =:= 'GET' orelse Method =:= 'OPTIONS' ->
    case Req#httpd.path_parts of
    % spatial-list request with spatial index and list from same design doc.
    [_, _, _DesignName, _, _, ListName, ViewName] ->
        % Same design doc for view and list
        handle_view_list(Req, Db, DDoc, ListName, DDoc, ViewName);
    % spatial-list request with spatial index and list from different design
    % doc.
    [_, _, _, _, _, ListName, DesignName, ViewName] ->
        % Different design docs for view and list
        ViewDocId = <<"_design/", DesignName/binary>>,
        {ok, ViewDDoc} = couch_db:open_doc(Db, ViewDocId, [ejson_body]),
        handle_view_list(Req, Db, DDoc, ListName, ViewDDoc, ViewName);
    _ ->
        couch_httpd:send_error(Req, 404, <<"list_error">>, <<"Bad path.">>)
    end;
% POST isn't supported as spatial indexes don't support mutli-key fetch
handle_view_list_req(Req, _Db, _DDoc) ->
    couch_httpd:send_method_not_allowed(Req, "GET,HEAD").

% Deprecated API call
handle_view_list_req_deprecated(#httpd{method=Method}=Req, Db, DDoc)
        when Method =:= 'GET' orelse Method =:= 'OPTIONS' ->
    Req2 = case Req#httpd.path_parts of
    [A, B, DesignName, C, ListName, SpatialName] ->
        Req#httpd{path_parts=
            [A, B, DesignName, C, <<"foo">>, ListName, SpatialName]};
    [A, B, C, D, ListName, DesignName, SpatialName] ->
        Req#httpd{path_parts=
            [A, B, C, D, <<"foo">>, ListName, DesignName, SpatialName]}
    end,
    ?LOG_INFO("WARNING: Request to deprecated _spatiallist handler, " ++
              "please use _spatial/_list instead!", []),
    handle_view_list_req(Req2, Db, DDoc);
handle_view_list_req_deprecated(#httpd{method='GET'}=Req, _Db, _DDoc) ->
    couch_httpd:send_error(Req, 404, <<"list_error">>, <<"Invalid path.">>);
handle_view_list_req_deprecated(Req, _Db, _DDoc) ->
    couch_httpd:send_method_not_allowed(Req, "GET,HEAD").


handle_view_list(Req, Db, DDoc, ListName, ViewDDoc, ViewName) ->
    Args0 = couch_spatial_http:parse_qs(Req),
    ETagFun = fun(BaseSig, Acc0) ->
        UserCtx = Req#httpd.user_ctx,
        Name = UserCtx#user_ctx.name,
        Roles = UserCtx#user_ctx.roles,
        Accept = couch_httpd:header_value(Req, "Accept"),
        Parts = {couch_httpd:doc_etag(DDoc), Accept, {Name, Roles}},
        ETag = couch_httpd:make_etag({BaseSig, Parts}),
        case couch_httpd:etag_match(Req, ETag) of
            true -> throw({etag_match, ETag});
            false -> {ok, Acc0#acc{etag=ETag}}
        end
    end,
    Args = Args0#spatial_args{preflight_fun=ETagFun},
    couch_httpd:etag_maybe(Req, fun() ->
        couch_query_servers:with_ddoc_proc(DDoc, fun(QueryServer) ->
            Acc = #acc{
                db = Db,
                req = Req,
                query_server = QueryServer,
                list_name = ListName
            },
            couch_spatial:query_view(
                Db, ViewDDoc, ViewName, Args, fun list_cb/2, Acc)
        end)
    end).


% This is basically a copy of the start_list_resp/2 in couch_mrview_show,
% the major difference is that the send_list_row function is different.
list_cb({meta, Meta}, #acc{resp=undefined} = Acc) ->
    MetaProps = case couch_util:get_value(offset, Meta) of
        undefined -> [];
        Offset -> [{offset, Offset}]
    end ++ case couch_util:get_value(update_seq, Meta) of
        undefined -> [];
        UpdateSeq -> [{update_seq, UpdateSeq}]
    end,
    start_list_resp({MetaProps}, Acc);
list_cb({row, Row}, #acc{resp=undefined} = Acc) ->
    {ok, NewAcc} = start_list_resp({[]}, Acc),
    send_list_row(Row, NewAcc);
list_cb({row, Row}, Acc) ->
    send_list_row(Row, Acc);
list_cb(complete, Acc) ->
    #acc{
        query_server = {Proc, _},
        resp = Resp0
    } = Acc,
    if Resp0 =:= nil ->
        {ok, #acc{resp = Resp}} = start_list_resp({[]}, Acc);
    true ->
        Resp = Resp0
    end,
    [<<"end">>, Data] = couch_query_servers:proc_prompt(Proc, [<<"list_end">>]),
    send_non_empty_chunk(Resp, Data),
    couch_httpd:last_chunk(Resp),
    {ok, Resp}.


% This is basically a copy of the start_list_resp/2 in couch_mrview_show,
% perhaps this should be the default for all indexers (or at least be exported)
start_list_resp(Head, Acc) ->
    #acc{
        db = Db,
        req = Req,
        query_server = QueryServer,
        list_name = ListName,
        etag = Etag
    } = Acc,
    JsonReq = couch_httpd_external:json_req_obj(Req, Db),

    [<<"start">>,Chunk,JsonResp] = couch_query_servers:ddoc_proc_prompt(
        QueryServer, [<<"lists">>, ListName], [Head, JsonReq]),
    JsonResp2 = apply_etag(JsonResp, Etag),
    #extern_resp_args{
        code = Code,
        ctype = CType,
        headers = ExtHeaders
    } = couch_httpd_external:parse_external_response(JsonResp2),
    JsonHeaders = couch_httpd_external:default_or_content_type(
        CType, ExtHeaders),
    {ok, Resp} = couch_httpd:start_chunked_response(Req, Code, JsonHeaders),
    send_non_empty_chunk(Resp, Chunk),
    {ok, Acc#acc{resp=Resp}}.


send_list_row(Row, Acc) ->
    #acc{
        query_server = {Proc, _},
        resp = Resp
    } = Acc,
    RowObj = couch_spatial_util:row_to_ejson(Row),
    try couch_query_servers:proc_prompt(Proc, [<<"list_row">>, RowObj]) of
    [<<"chunks">>, Chunk] ->
        send_non_empty_chunk(Resp, Chunk),
        {ok, Acc};
    [<<"end">>, Chunk] ->
        send_non_empty_chunk(Resp, Chunk),
        couch_httpd:last_chunk(Resp),
        {stop, Acc}
    catch Error ->
        couch_httpd:send_chunked_error(Resp, Error),
        {stop, Acc}
    end.


% This is a verbatim copy from couch_mrview_show
send_non_empty_chunk(_, []) ->
    ok;
send_non_empty_chunk(Resp, Chunk) ->
    couch_httpd:send_chunk(Resp, Chunk).

% This is a verbatim copy from couch_mrview_show
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
            JsonHeadersEtagged = json_apply_field({<<"Etag">>, CurrentEtag}, JsonHeaders),
            JsonHeadersVaried = json_apply_field({<<"Vary">>, <<"Accept">>}, JsonHeadersEtagged),
            {<<"headers">>, JsonHeadersVaried};
        _ -> % skip non-header fields
            Field
        end || Field <- ExternalResponse]}
    end.

% This is a verbatim copy from couch_mrview_show
% Maybe this is in the proplists API
% todo move to couch_util
json_apply_field(H, {L}) ->
    json_apply_field(H, L, []).


json_apply_field({Key, NewValue}, [{Key, _OldVal} | Headers], Acc) ->
    % drop matching keys
    json_apply_field({Key, NewValue}, Headers, Acc);
json_apply_field({Key, NewValue}, [{OtherKey, OtherVal} | Headers], Acc) ->
    % something else is next, leave it alone.
    json_apply_field({Key, NewValue}, Headers, [{OtherKey, OtherVal} | Acc]);
json_apply_field({Key, NewValue}, [], Acc) ->
    % end of list, add ours
    {[{Key, NewValue}|Acc]}.
