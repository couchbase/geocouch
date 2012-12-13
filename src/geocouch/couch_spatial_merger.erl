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

-module(couch_spatial_merger).

%-export([query_spatial/2]).
-export([parse_http_params/4, make_funs/3, get_skip_and_limit/1,
    http_index_folder_req_details/3, make_event_fun/2]).

-include("couch_db.hrl").
-include_lib("couch_index_merger/include/couch_index_merger.hrl").
-include("couch_spatial.hrl").

-define(LOCAL, <<"local">>).

% callback!
parse_http_params(Req, _DDoc, _IndexName, _Extra) ->
    couch_httpd_spatial:parse_spatial_params(Req).

% callback!
make_funs(_DDoc, _IndexName, _IndexMergeParams) ->
    {fun spatial_less_fun/2,
    fun spatial_folder/6,
    fun merge_spatial/1,
    fun(NumFolders, Callback, UserAcc) ->
        fun(Item) ->
            couch_index_merger:collect_row_count(
                NumFolders, 0, fun spatial_row_obj/1, Callback, UserAcc, Item)
        end
    end,
    nil}.

% callback!
get_skip_and_limit(#spatial_query_args{skip=Skip, limit=Limit}) ->
    {Skip, Limit}.

% callback!
make_event_fun(_SpatialArgs, Queue) ->
    fun(Ev) ->
        http_spatial_fold(Ev, Queue)
    end.

% callback!
http_index_folder_req_details(#merged_index_spec{} = Spec, MergeParams, DDoc) ->
    #merged_index_spec{
        url = MergeUrl0,
        ejson_spec = {EJson}
    } = Spec,
    #index_merge{
        conn_timeout = Timeout,
        http_params = SpatialArgs
    } = MergeParams,
    {ok, #httpdb{url = Url, lhttpc_options = Options} = Db} =
        couch_index_merger:open_db(MergeUrl0, nil, Timeout),
    MergeUrl = Url ++ spatial_qs(SpatialArgs),
    Headers = [{"Content-Type", "application/json"} | Db#httpdb.headers],

    EJson2 = case couch_index_merger:should_check_rev(MergeParams, DDoc) of
    true ->
        P = fun (Tuple) -> element(1, Tuple) =/= <<"ddoc_revision">> end,
        [{<<"ddoc_revision">>, couch_index_merger:ddoc_rev_str(DDoc)} |
            lists:filter(P, EJson)];
    false ->
        EJson
    end,

    Body = {EJson2},
    put(from_url, Url),
    {MergeUrl, post, Headers, ?JSON_ENCODE(Body), Options};

http_index_folder_req_details(#simple_index_spec{} = Spec, MergeParams, _DDoc) ->
    #simple_index_spec{
        database = DbUrl,
        ddoc_id = DDocId,
        index_name = SpatialName
    } = Spec,
    #index_merge{
        conn_timeout = Timeout,
        http_params = SpatialArgs
    } = MergeParams,
    {ok, #httpdb{url = Url, lhttpc_options = Options}} =
        couch_index_merger:open_db(DbUrl, nil, Timeout),
    SpatialUrl = Url ++ couch_httpd:quote(DDocId) ++ "/_spatial/" ++
        couch_httpd:quote(SpatialName) ++ spatial_qs(SpatialArgs),
    put(from_url, DbUrl),
    {SpatialUrl, get, [], [], Options}.

spatial_row_obj({{Key, error}, Reason}) ->
    <<"{\"key\":", (?JSON_ENCODE(Key))/binary,
      ",\"error\":",
      (?JSON_ENCODE(couch_util:to_binary(Reason)))/binary, "}">>;
spatial_row_obj({{Bbox, DocId}, {{Type, Coords}, Value}}) ->
    <<"{\"id\":", (?JSON_ENCODE(DocId))/binary,
      ",\"bbox\":", (?JSON_ENCODE(tuple_to_list(Bbox)))/binary,
      ",\"geometry\":",
      (?JSON_ENCODE({[{type, Type}, {coordinates, Coords}]}))/binary,
      ",\"value\":", (?JSON_ENCODE(Value))/binary, "}">>.

spatial_less_fun(A, B) ->
    A < B.

% Counterpart to map_view_folder/6 in couch_view_merger
spatial_folder(Db, SpatialSpec, MergeParams, _UserCtx, DDoc, Queue) ->
    #simple_index_spec{
        ddoc_database = DDocDbName, ddoc_id = DDocId, index_name = SpatialName
    } = SpatialSpec,
    #spatial_query_args{
        bbox = Bbox,
        bounds = Bounds,
        stale = Stale
    } = MergeParams#index_merge.http_params,
    FoldlFun = make_spatial_fold_fun(Queue),
    {DDocDb, Index} = get_spatial_index(Db, DDocDbName, DDocId,
        SpatialName, Stale),

    case not(couch_index_merger:should_check_rev(MergeParams, DDoc)) orelse
            couch_index_merger:ddoc_unchanged(DDocDb, DDoc) of
    true ->
        % The spatial index doesn't output a total_rows property, hence
        % we don't need a proper row_count (but we need it in the queue to
        % make the index merging work correctly)
        ok = couch_view_merger_queue:queue(Queue, {row_count, 0}),
        couch_spatial:fold(Index, FoldlFun, nil, Bbox, Bounds);
    false ->
        ok = couch_view_merger_queue:queue(Queue, revision_mismatch)
    end,
    catch couch_db:close(DDocDb).

% Counterpart to get_map_view/5 in couch_view_merger
get_spatial_index(Db, DDocDbName, DDocId, SpatialName, Stale) ->
    GroupId = couch_index_merger:get_group_id(DDocDbName, DDocId),
    {ok, Index, _Group} = couch_spatial:get_spatial_index(Db, GroupId,
        SpatialName, Stale),
    case GroupId of
        {DDocDb, DDocId} -> {DDocDb, Index};
        DDocId -> {nil, Index}
    end.

% Counterpart to http_view_fold/3 in couch_view_merger
http_spatial_fold(object_start, Queue) ->
    ok = couch_view_merger_queue:queue(Queue, {row_count, 0}),
    fun(Ev) -> http_spatial_fold_rows_1(Ev, Queue) end.

% Counterpart to http_view_fold_rows_1/2 in couch_view_merger
http_spatial_fold_rows_1({key, <<"rows">>}, Queue) ->
    fun(array_start) -> fun(Ev) -> http_spatial_fold_rows_2(Ev, Queue) end end;
http_spatial_fold_rows_1(_Ev, Queue) ->
    fun(Ev) -> http_spatial_fold_rows_1(Ev, Queue) end.

% Counterpart to http_view_fold_fold_rows_2/2 in couch_view_merger
http_spatial_fold_rows_2(array_end, Queue) ->
    fun(Ev) -> http_spatial_fold_errors_1(Ev, Queue) end;
http_spatial_fold_rows_2(object_start, Queue) ->
    fun(Ev) ->
        json_stream_parse:collect_object(
            Ev,
            fun(Row) ->
                http_spatial_fold_queue_row(Row, Queue),
                fun(Ev2) -> http_spatial_fold_rows_2(Ev2, Queue) end
            end)
    end.

% Counterpart to http_view_fold_errors_1/2 in couch_view_merger
http_spatial_fold_errors_1({key, <<"errors">>}, Queue) ->
    fun(array_start) -> fun(Ev) -> http_spatial_fold_errors_2(Ev, Queue) end end;
http_spatial_fold_errors_1(_Ev, _Queue) ->
    fun couch_index_merger:void_event/1.

% Counterpart to http_view_fold_errors_2/2 in couch_view_merger
http_spatial_fold_errors_2(array_end, _Queue) ->
    fun couch_index_merger:void_event/1;
http_spatial_fold_errors_2(object_start, Queue) ->
    fun(Ev) ->
        json_stream_parse:collect_object(
            Ev,
            fun(Error) ->
                http_view_fold_queue_error(Error, Queue),
                fun(Ev2) -> http_spatial_fold_errors_2(Ev2, Queue) end
            end)
    end.

% Carbon copy of http_view_fold_queue_error/2 in couch_view_merger
http_view_fold_queue_error({Props}, Queue) ->
    From0 = couch_util:get_value(<<"from">>, Props, ?LOCAL),
    From = case From0 of
        ?LOCAL ->
        get(from_url);
    _ ->
        From0
    end,
    Reason = couch_util:get_value(<<"reason">>, Props, null),
    ok = couch_view_merger_queue:queue(Queue, {error, From, Reason}).

% Counterpart to http_view_fold_queue_row/2 in couch_view_merger
% Used for merges of remote DBs
http_spatial_fold_queue_row({Props}, Queue) ->
    Id = couch_util:get_value(<<"id">>, Props, nil),
    Bbox = couch_util:get_value(<<"bbox">>, Props, null),
    {Geom} = couch_util:get_value(<<"geometry">>, Props, null),
    Val = couch_util:get_value(<<"value">>, Props),
    Row = case couch_util:get_value(<<"error">>, Props, nil) of
    nil ->
        GeomType = couch_util:get_value(<<"type">>, Geom),
        Coords = couch_util:get_value(<<"coordinates">>, Geom),
        case couch_util:get_value(<<"doc">>, Props, nil) of
        nil ->
            {{list_to_tuple(Bbox), Id}, {{GeomType,Coords}, Val}};
        % NOTE vmx 20110818: GeoCouch doesn't support include_docs atm,
        %     but I'll just leave the code here
        Doc ->
            {{list_to_tuple(Bbox), Id}, {{GeomType,Coords}, Val}, {doc, Doc}}
        end;
    Error ->
        % error in a map row
        {{list_to_tuple(Bbox), error}, Error}
    end,
    ok = couch_view_merger_queue:queue(Queue, Row).



% Counterpart to make_map_fold_fun/4 in couch_view_merger
% Used for merges of local DBs
make_spatial_fold_fun(Queue) ->
    fun({{_Bbox, _DocId}, {_Geom, _Value}}=Row, Acc) ->
        ok = couch_view_merger_queue:queue(Queue, Row),
        {ok, Acc}
    end.

% Counterpart to merge_map_views/6 in couch_view_merger
merge_spatial(#merge_params{limit = 0} = Params) ->
    couch_index_merger:merge_indexes_no_limit(Params);

merge_spatial(#merge_params{row_acc = []} = Params) ->
    case couch_index_merger:merge_indexes_no_acc(
            Params, fun merge_spatial_min_row/2) of
    {params, Params2} ->
        merge_spatial(Params2);
    Else ->
        Else
    end;

% ??? vmx 20110805: Does this case ever happen in the spatial index?
merge_spatial(Params) ->
    Params2 = couch_index_merger:handle_skip(Params),
    merge_spatial(Params2).

% Counterpart to merge_map_min_row/2 in couch_view_merger
merge_spatial_min_row(Params, MinRow) ->
    ok = couch_view_merger_queue:flush(Params#merge_params.queue),
    couch_index_merger:handle_skip(Params#merge_params{row_acc=[MinRow]}).

% Counterpart to view_qs/1 in couch_view_merger
spatial_qs(SpatialArgs) ->
    DefSpatialArgs = #spatial_query_args{},
    #spatial_query_args{
        bbox = Bbox,
        stale = Stale,
        count = Count,
        bounds = Bounds
    } = SpatialArgs,
    QsList = case Bbox =:= DefSpatialArgs#spatial_query_args.bbox of
    true ->
        [];
    false ->
        ["bbox=" ++ ?b2l(iolist_to_binary(
            lists:nth(2, hd(io_lib:format("~p", [Bbox])))))]
    end ++
    case Stale =:= DefSpatialArgs#spatial_query_args.stale of
    true ->
        [];
    false ->
        ["stale=" ++ atom_to_list(Stale)]
    end ++
    case Count =:= DefSpatialArgs#spatial_query_args.count of
    true ->
        [];
    false ->
        ["count=" ++ atom_to_list(Count)]
    end ++
    case Bounds =:= DefSpatialArgs#spatial_query_args.bounds of
    true ->
        [];
    false ->
        ["bounds=" ++ ?b2l(iolist_to_binary(
            lists:nth(2, hd(io_lib:format("~p", [Bounds])))))]
    end,
    case QsList of
    [] ->
        [];
    _ ->
        "?" ++ string:join(QsList, "&")
    end.
