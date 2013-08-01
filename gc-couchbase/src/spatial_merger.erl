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

% This is where all request parameter processing happens. The
% `capi_spatial`/`capi_indexer` just pass on the request (hence the query
% arguments) into this module.

-module(spatial_merger).

-export([parse_http_params/4, make_funs/3, get_skip_and_limit/1,
    make_event_fun/2, view_qs/2, process_extra_params/2]).
-export([simple_set_view_query/3]).

-include("couch_db.hrl").
-include_lib("couch_spatial.hrl").
-include_lib("couch_index_merger/include/couch_index_merger.hrl").
-include_lib("couch_index_merger/include/couch_view_merger.hrl").
-include_lib("couch_set_view/include/couch_set_view.hrl").

-define(LOCAL, <<"local">>).


% callback!
parse_http_params(Req, _DDoc, _IndexName, _Extra) ->
    spatial_http:parse_qs(Req).

% callback!
make_funs(_DDoc, _IndexName, IndexMergeParams) ->
    #index_merge{
        http_params = #spatial_query_args{
            debug = DebugMode
        }
    } = IndexMergeParams,
    {fun spatial_less_fun/2,
    fun spatial_view_folder/6,
    fun merge_spatial/1,
    fun(NumFolders, Callback, UserAcc) ->
        fun(Item) ->
            MakeRowFun = fun(RowDetails) ->
               spatial_row_obj(RowDetails, DebugMode)
            end,
            couch_index_merger:collect_row_count(
                NumFolders, 0, MakeRowFun, Callback, UserAcc, Item)
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
process_extra_params(_, EJson) ->
    EJson.


% Optimized path, row assembled by couch_http_view_streamer
% NOTE vmx 2013-07-19: Not sure if that case is ever used by the spatial
%     indexer
spatial_row_obj({_KeyDocId, {row_json, RowJson}}, _Debug) ->
    RowJson;
spatial_row_obj({{Key, error}, Reason}, _Debug) ->
    <<"{\"key\":", (?JSON_ENCODE(Key))/binary,
      ",\"error\":",
      (?JSON_ENCODE(couch_util:to_binary(Reason)))/binary, "}">>;
% NOTE vmx 2013-07-10: Those parameters are the ones the function in
%     spatial_view:fold_fun() gets. It might be pre-processes by the
%     `FoldFun` (in e.g. simple_spatial_view_query/4).
% Row from local node, query with ?debug=true
spatial_row_obj({{Mbb, DocId}, {PartId, Value}}, true) when is_integer(PartId) ->
    <<"{\"id\":", (?JSON_ENCODE(DocId))/binary,
      ",\"key\":", (?JSON_ENCODE(Mbb))/binary,
      ",\"partition\":", (?l2b(integer_to_list(PartId)))/binary,
      ",\"node\":\"", (?LOCAL)/binary, "\"",
      ",\"value\":", (?JSON_ENCODE(Value))/binary, "}">>;

% Row from remote node, using Erlang based stream JSON parser, query with ?debug=true
spatial_row_obj({{Mbb, DocId}, {PartId, Node, Value}}, true) when is_integer(PartId) ->
    <<"{\"id\":", (?JSON_ENCODE(DocId))/binary,
      ",\"key\":", (?JSON_ENCODE(Mbb))/binary,
      ",\"partition\":", (?l2b(integer_to_list(PartId)))/binary,
      ",\"node\":", (?JSON_ENCODE(Node))/binary,
      ",\"value\":", (?JSON_ENCODE(Value))/binary, "}">>;

% Row from local node, query with ?debug=false
spatial_row_obj({{Mbb, DocId}, {PartId, Value}}, false) when is_integer(PartId)  ->
    <<"{\"id\":", (?JSON_ENCODE(DocId))/binary,
      ",\"key\":", (?JSON_ENCODE(Mbb))/binary,
      ",\"value\":", (?JSON_ENCODE(Value))/binary, "}">>.

spatial_less_fun(A, B) ->
    A < B.


% This wrapper is needed to be compatible with the mapreduce views, which
% also have the case for _all_docs.
spatial_view_folder(_Db, ViewSpec, MergeParams, _UserCtx, DDoc, Queue) ->
    spatial_view_folder(ViewSpec, MergeParams, DDoc, Queue).
spatial_view_folder(ViewSpec, MergeParams, DDoc, Queue) ->
    #set_view_spec{
        name = SetName,
        ddoc_id = DDocId,
        partitions = WantedPartitions0
    } = ViewSpec,
    #index_merge{
        http_params = ViewArgs
    } = MergeParams,
    #spatial_query_args{
        stale = Stale,
        debug = Debug,
        type = IndexType
    } = ViewArgs,
    DDocDbName = ?master_dbname(SetName),

    PrepareResult = case (ViewSpec#set_view_spec.view =/= nil) andalso
        (ViewSpec#set_view_spec.group =/= nil) of
    true ->
        ViewGroupReq = nil,
        {ViewSpec#set_view_spec.view, ViewSpec#set_view_spec.group};
    false ->
        WantedPartitions = case IndexType of
        main ->
            WantedPartitions0;
        replica ->
            []
        end,
        ViewGroupReq = #set_view_group_req{
            stale = Stale,
            update_stats = true,
            wanted_partitions = WantedPartitions,
            debug = Debug,
            type = IndexType
        },
        couch_view_merger:prepare_set_view(ViewSpec, ViewGroupReq, DDoc, Queue,
                                           fun spatial_view:get_spatial_view/4)
    end,

    case PrepareResult of
    error ->
        %%  handled by prepare_set_view
        ok;
    {View, Group} ->
        couch_view_merger:queue_debug_info(Debug, Group, ViewGroupReq, Queue),
        try
            % No include_docs for now
            FoldFun = make_spatial_fold_fun(Queue),

            case not(couch_index_merger:should_check_rev(MergeParams, DDoc)) orelse
                couch_index_merger:ddoc_unchanged(DDocDbName, DDoc) of
            true ->
                RowCount = couch_set_view:get_row_count(Group, View),
                ok = couch_view_merger_queue:queue(Queue,
                    {row_count, RowCount}),
                %ok = couch_view_merger_queue:queue(Queue, {row_count, 0}),
                {ok, _, _} = couch_set_view:fold(Group, View, FoldFun, [], ViewArgs);
            false ->
                ok = couch_view_merger_queue:queue(Queue, revision_mismatch)
            end
        catch
        ddoc_db_not_found ->
            ok = couch_view_merger_queue:queue(
                Queue, {error, ?LOCAL,
                    couch_index_merger:ddoc_not_found_msg(DDocDbName, DDocId)});
        _Tag:Error ->
            Stack = erlang:get_stacktrace(),
            ?LOG_ERROR("Caught unexpected error "
                       "while serving view query ~s/~s: ~p~n~p",
                       [SetName, DDocId, Error, Stack]),
            couch_view_merger_queue:queue(Queue,
                {error, ?LOCAL, couch_util:to_binary(Error)})
        after
            couch_set_view:release_group(Group),
            ok = couch_view_merger_queue:done(Queue)
        end
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
    Mbb = couch_util:get_value(<<"key">>, Props, null),
    Val = couch_util:get_value(<<"value">>, Props),
    Value = case couch_util:get_value(<<"partition">>, Props, nil) of
    nil ->
        Val;
    PartId ->
        % we're in debug mode, add node info
        {PartId, get(from_url), Val}
    end,
    Row = case couch_util:get_value(<<"error">>, Props, nil) of
    nil ->
        case couch_util:get_value(<<"doc">>, Props, nil) of
        nil ->
            {{Mbb, Id}, Value};
        % NOTE vmx 20110818: GeoCouch doesn't support include_docs atm,
        %     but I'll just leave the code here
        Doc ->
            {{Mbb, Id}, Value, Doc}
        end;
    Error ->
        % error in a map row
        {{Mbb, error}, Error}
    end,
    ok = couch_view_merger_queue:queue(Queue, Row).


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


% Counterpart to make_map_fold_fun/4 in couch_view_merger
% Used for merges of local DBs
make_spatial_fold_fun(Queue) ->
    fun({{_Mbb, _DocId}, {_PartId, _Value}}=Row, Acc) ->
        ok = couch_view_merger_queue:queue(Queue, Row),
        {ok, Acc}
    end.

% Counterpart to view_qs/2 in couch_view_merger
view_qs(SpatialArgs, MergeParams) ->
    DefSpatialArgs = #spatial_query_args{},
    #spatial_query_args{
        range = Range,
        stale = Stale,
        limit = Limit,
        debug = Debug,
        skip = Skip
    } = SpatialArgs,
    #index_merge{on_error = OnError} = MergeParams,

    QsList =
    case Range =:= DefSpatialArgs#spatial_query_args.range of
    true ->
        [];
    false ->
        {Start, End} = lists:unzip(Range),
        ["start_range=" ++ ?b2l(iolist_to_binary(
            io_lib:format("~w", [Start])))] ++
        ["end_range=" ++ ?b2l(iolist_to_binary(
            io_lib:format("~w", [End])))]
    end ++
    case Stale =:= DefSpatialArgs#spatial_query_args.stale of
    true ->
        [];
    false ->
        ["stale=" ++ atom_to_list(Stale)]
    end ++
    case Limit =:= DefSpatialArgs#spatial_query_args.limit of
    true ->
        [];
    false ->
        ["limit=" ++ integer_to_list(Limit + Skip)]
    end ++
    case OnError =:= ?ON_ERROR_DEFAULT of
    true ->
        [];
    false ->
        ["on_error=" ++ atom_to_list(OnError)]
    end ++
    case Debug =:= DefSpatialArgs#spatial_query_args.debug of
    true ->
        [];
    false ->
        ["debug=" ++ atom_to_list(Debug)]
    end,

    case QsList of
    [] ->
        [];
    _ ->
        "?" ++ string:join(QsList, "&")
    end.


% Query with a single view to merge, trigger a simpler code path
% (no queue, no child processes, etc).
simple_set_view_query(Params, DDoc, Req) ->
    #index_merge{
        callback = Callback,
        user_acc = UserAcc,
        indexes = [SetViewSpec]
    } = Params,
    #set_view_spec{
        name = SetName,
        partitions = Partitions0,
        ddoc_id = DDocId,
        view_name = ViewName,
        category = Category
    } = SetViewSpec,

    Stale = list_to_existing_atom(string:to_lower(
        couch_httpd:qs_value(Req, "stale", "update_after"))),
    Debug = couch_set_view_http:parse_bool_param(
        couch_httpd:qs_value(Req, "debug", "false")),
    IndexType = list_to_existing_atom(
        couch_httpd:qs_value(Req, "_type", "main")),
    Partitions = case IndexType of
    main ->
        Partitions0;
    replica ->
        []
    end,
    GroupReq = #set_view_group_req{
        stale = Stale,
        update_stats = true,
        wanted_partitions = Partitions,
        debug = Debug,
        type = IndexType,
        category = Category
    },

    case couch_view_merger:get_set_view(
        fun spatial_view:get_spatial_view/4, SetName, DDoc, ViewName, GroupReq) of
    {ok, View, Group, MissingPartitions} ->
        ok;
    Error ->
        MissingPartitions = Group = View = nil,
        ErrorMsg = io_lib:format("Error opening view `~s`, from set `~s`, "
            "design document `~s`: ~p", [ViewName, SetName, DDocId, Error]),
        throw({not_found, iolist_to_binary(ErrorMsg)})
    end,

    case MissingPartitions of
    [] ->
        ok;
    _ ->
        couch_set_view:release_group(Group),
        ?LOG_INFO("Set view `~s`, group `~s`, missing partitions: ~w",
                  [SetName, DDocId, MissingPartitions]),
        throw({error, set_view_outdated})
    end,

    % This code path is never triggered for _all_docs, hence we don't need
    % to handle the special case to do raw collation for the query parameters
    QueryArgs = spatial_http:parse_qs(Req),
    QueryArgs2 = QueryArgs#spatial_query_args{
        view_name = ViewName,
        stale = Stale
     },

    case couch_view_merger:debug_info(Debug, Group, GroupReq) of
    nil ->
        Params2 = Params#index_merge{user_ctx = Req#httpd.user_ctx};
    DebugInfo ->
        {ok, UserAcc2} = Callback(DebugInfo, UserAcc),
        Params2 = Params#index_merge{
            user_ctx = Req#httpd.user_ctx,
            user_acc = UserAcc2
        }
    end,

    try
        simple_spatial_view_query(Params2, Group, View, QueryArgs2)
    after
        couch_set_view:release_group(Group)
    end.


simple_spatial_view_query(Params, Group, View, ViewArgs) ->
    #index_merge{
        callback = Callback,
        user_acc = UserAcc
    } = Params,
    #spatial_query_args{
        limit = Limit,
        skip = Skip,
        debug = DebugMode
    } = ViewArgs,

    FoldFun = fun(_Kv, {0, _, _} = Acc) ->
            {stop, Acc};
        (_Kv, {AccLim, AccSkip, UAcc}) when AccSkip > 0 ->
            {ok, {AccLim, AccSkip - 1, UAcc}};
        ({{_Key, _DocId}, {_PartId, _Value}} = Kv, {AccLim, 0, UAcc}) ->
            Row = spatial_row_obj(Kv, DebugMode),
            {ok, UAcc2} = Callback({row, Row}, UAcc),
            {ok, {AccLim - 1, 0, UAcc2}}
    end,

    RowCount = couch_set_view:get_row_count(Group, View),
    {ok, UserAcc2} = Callback({start, RowCount}, UserAcc),

    {ok, _, {_, _, UserAcc3}} = couch_set_view:fold(
        Group, View, FoldFun, {Limit, Skip, UserAcc2}, ViewArgs),
    Callback(stop, UserAcc3).
