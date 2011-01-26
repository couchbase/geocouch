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

-module(couch_httpd_spatial_list).
-include("couch_db.hrl").
-include("couch_spatial.hrl").

-export([handle_spatial_list_req/3]).
% deprecated API
-export([handle_spatial_list_req_deprecated/3]).

-import(couch_httpd, [send_json/2, send_method_not_allowed/2,
                      send_error/4, send_chunked_error/2]).


% spatial-list request with spatial index and list from same design doc.
handle_spatial_list_req(#httpd{method='GET',
        path_parts=[_, _, DesignName, _, _, ListName, SpatialName]}=Req, Db, DDoc) ->
    handle_spatial_list(Req, Db, DDoc, ListName, {DesignName, SpatialName});

% spatial-list request with spatial index and list from different design docs.
handle_spatial_list_req(#httpd{method='GET',
        path_parts=[_, _, _, _, _, ListName, DesignName, SpatialName]}=Req,
        Db, DDoc) ->
    handle_spatial_list(Req, Db, DDoc, ListName, {DesignName, SpatialName});

handle_spatial_list_req(#httpd{method='GET'}=Req, _Db, _DDoc) ->
    send_error(Req, 404, <<"list_error">>, <<"Invalid path.">>);

% POST isn't supported as spatial indexes don't support mutli-key fetch
handle_spatial_list_req(Req, _Db, _DDoc) ->
    send_method_not_allowed(Req, "GET,HEAD").

handle_spatial_list(Req, Db, DDoc, LName, {SpatialDesignName, SpatialName}) ->
    SpatialDesignId = <<"_design/", SpatialDesignName/binary>>,
    {ok, Index, Group, QueryArgs} = couch_httpd_spatial:load_index(
            Req, Db, {SpatialDesignId, SpatialName}),
    Etag = list_etag(Req, Db, Group, Index, couch_httpd:doc_etag(DDoc)),
    couch_httpd:etag_respond(Req, Etag, fun() ->
        output_list(Req, Db, DDoc, LName, Index, QueryArgs, Etag, Group)
    end).

% Deprecated API call
handle_spatial_list_req_deprecated(#httpd{method='GET',
        path_parts=[A, B, DesignName, C, ListName, SpatialName]}=Req, Db, DDoc) ->
    Req2 = Req#httpd{path_parts=
        [A, B, DesignName, C, <<"foo">>, ListName, SpatialName]},
    handle_spatial_list_req(Req2, Db, DDoc);
handle_spatial_list_req_deprecated(#httpd{method='GET',
        path_parts=[A, B, C, D, ListName, DesignName, SpatialName]}=Req,
        Db, DDoc) ->
    Req2 = Req#httpd{path_parts=
        [A, B, C, D, <<"foo">>, ListName, DesignName, SpatialName]},
    handle_spatial_list_req(Req2, Db, DDoc);
handle_spatial_list_req_deprecated(#httpd{method='GET'}=Req, _Db, _DDoc) ->
    send_error(Req, 404, <<"list_error">>, <<"Invalid path.">>);
handle_spatial_list_req_deprecated(Req, _Db, _DDoc) ->
    send_method_not_allowed(Req, "GET,HEAD").


list_etag(#httpd{user_ctx=UserCtx}=Req, Db, Group, Index, More) ->
    Accept = couch_httpd:header_value(Req, "Accept"),
    couch_httpd_spatial:spatial_etag(
        Db, Group, Index, {More, Accept, UserCtx#user_ctx.roles}).

output_list(_, _, _, _, _, #spatial_query_args{bbox=nil}, _, _) ->
    throw({spatial_query_error, <<"Bounding box not specified.">>});

output_list(Req, Db, DDoc, LName, Index, QueryArgs, Etag, Group) ->
    #spatial_query_args{
        bbox = Bbox,
        bounds = Bounds
    } = QueryArgs,

    couch_query_servers:with_ddoc_proc(DDoc, fun(QServer) ->
        StartListRespFun = make_spatial_start_resp_fun(QServer, Db, LName),
        CurrentSeq = Group#spatial_group.current_seq,
        {ok, Resp, BeginBody} = StartListRespFun(Req, Etag, [], CurrentSeq),
        geocouch_duplicates:send_non_empty_chunk(Resp, BeginBody),
        SendRowFun = make_spatial_get_row_fun(QServer, Resp),
        FoldAccInit = {undefined, ""},
        {ok, {_Resp, Go}} = couch_spatial:fold(
            Group, Index, SendRowFun, FoldAccInit, Bbox, Bounds),
        case Go of
        [] ->
            {Proc, _DDocId} = QServer,
            [<<"end">>, Chunks] = couch_query_servers:proc_prompt(
                                      Proc, [<<"list_end">>]),
%            Chunk = BeginBody ++ ?b2l(?l2b(Chunks)),
            Chunk = ?b2l(?l2b(Chunks)),
            geocouch_duplicates:send_non_empty_chunk(Resp, Chunk);
        stop ->
            ok
        end,
        couch_httpd:last_chunk(Resp)
    end).


% Counterpart to make_map_start_resp_fun/3 in couch_http_show.
make_spatial_start_resp_fun(QueryServer, Db, LName) ->
    fun(Req, Etag, _Acc, UpdateSeq) ->
        Head = {[{<<"update_seq">>, UpdateSeq}]},
        geocouch_duplicates:start_list_resp(QueryServer, LName, Req, Db, Head, Etag)
%        couch_httpd_show:start_list_resp(QueryServer, LName, Req, Db, Head, Etag)
    end.

% Counterpart to make_map_send_row_fun/1 in couch_http_show.
make_spatial_get_row_fun(QueryServer, Resp) ->
    fun({_Bbox, _DocId, _Value}=Row, _Acc) ->
        {State, Result} = send_list_row(Resp, QueryServer, Row),
        {State, {Resp, Result}}
    end.

send_list_row(Resp, QueryServer, Row) ->
    try
        [Go, Chunks] = prompt_list_row(QueryServer, Row),
        Chunk = ?b2l(?l2b(Chunks)),
        geocouch_duplicates:send_non_empty_chunk(Resp, Chunk),
        case Go of
            <<"chunks">> ->
                {ok, ""};
            <<"end">> ->
                {stop, stop}
        end
    catch
        throw:Error ->
            send_chunked_error(Resp, Error),
            throw({already_sent, Resp, Error})
    end.

prompt_list_row({Proc, _DDocId}, {Bbox, DocId, Value}) ->
    JsonRow = {[{id, DocId}, {key, tuple_to_list(Bbox)}, {value, Value}]},
    couch_query_servers:proc_prompt(Proc, [<<"list_row">>, JsonRow]).
