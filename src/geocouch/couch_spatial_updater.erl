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

-module(couch_spatial_updater).

-ifdef(makecheck).
-compile(export_all).
-endif.


-export([update/2]).

% for benchmark script
-export([geojson_get_bbox/1]).

% for output (couch_http_spatial, couch_http_spatial_list)
-export([geocouch_to_geojsongeom/1]).

% for polygon search
-export([extract_bbox/2, geojsongeom_to_geocouch/1]).

-include("couch_db.hrl").
-include("couch_spatial.hrl").

update(Owner, Group) ->
    #spatial_group{
        db = #db{name=DbName} = Db,
        name = GroupName,
        current_seq = Seq,
        indexes = Indexes
        %purge_seq = PurgeSeq
    } = Group,
    % XXX vmx: what are purges? when do they happen?
    %DbPurgeSeq = couch_db:get_purge_seq(Db),
    %Group2 =
    %if DbPurgeSeq == PurgeSeq ->
    %    Group;
    %DbPurgeSeq == PurgeSeq + 1 ->
    %    couch_task_status:update(<<"Removing purged entries from view index.">>),
    %    purge_index(Group);
    %true ->
    %    couch_task_status:update(<<"Resetting view index due to lost purge entries.">>),
    %    % NOTE vmx:probably  needs handle_info({'EXIT', FromPid, reset}
    %    %     in couch_spatial_group.erl
    %    exit(reset)
    %end,

    %ViewEmptyKVs = [{View, []} || View <- Group2#group.views],
    % List of indexes with their (initially empty) results
    IndexEmptyKVs = [{Index, []} || Index <- Group#spatial_group.indexes],
    % compute on all docs modified since we last computed.
    TotalChanges = couch_db:count_changes_since(Db, Seq),
    couch_task_status:add_task([
        {type, indexer},
        {database, DbName},
        {design_document, GroupName},
        {progress, 0},
        {changes_done, 0},
        {total_changes, TotalChanges}
    ]),
    couch_task_status:set_update_frequency(500),
    {ok, MapCtx} = mapreduce:start_map_context([I#spatial.def || I <- Indexes]),
    EmptyResults = [[] || _ <- Indexes],

    {ok, _, {_,{UncomputedDocs, Group3, ViewKVsToAdd, DocIdViewIdKeys}}}
        = couch_db:enum_docs_since(Db, Seq,
        fun(DocInfo, _, {ChangesProcessed, Acc}) ->
            Progress = (ChangesProcessed*100) div TotalChanges,
            couch_task_status:update([
                {progress, Progress},
                {changes_done, ChangesProcessed}
            ]),
            %?LOG_DEBUG("enum_doc_since: ~p", [Acc]),
            Acc2 = process_doc(Db, Owner, MapCtx, EmptyResults, DocInfo, Acc),
            {ok, {ChangesProcessed+1, Acc2}}
        end, {0, {[], Group, IndexEmptyKVs, []}}, []),
     %?LOG_DEBUG("enum_doc_since results: ~p~n~p~n~p", [UncomputedDocs, ViewKVsToAdd, DocIdViewIdKeys]),
    Results = spatial_docs(MapCtx, UncomputedDocs, EmptyResults),
    % Output is way to huge
    %?LOG_DEBUG("spatial_docs results: ~p", [Results]),
    {ViewKVsToAdd2, DocIdViewIdKeys2} = view_insert_query_results(
            UncomputedDocs, Results, ViewKVsToAdd, DocIdViewIdKeys),
    NewSeq = couch_db:get_update_seq(Db),
    ?LOG_DEBUG("new seq num: ~p", [NewSeq]),
    {ok, Group4} = write_changes(Group3, ViewKVsToAdd2, DocIdViewIdKeys2,
                NewSeq),
    exit({new_group, Group4}).




% NOTE vmx: whatever it does, it seems to be doing a good job
view_insert_query_results([], [], ViewKVs, DocIdViewIdKeysAcc) ->
    {ViewKVs, DocIdViewIdKeysAcc};
view_insert_query_results([Doc|RestDocs], [QueryResults | RestResults], ViewKVs, DocIdViewIdKeysAcc) ->
    {NewViewKVs, NewViewIdKeys} = view_insert_doc_query_results(Doc, QueryResults, ViewKVs, [], []),
    NewDocIdViewIdKeys = [{Doc#doc.id, NewViewIdKeys} | DocIdViewIdKeysAcc],
    view_insert_query_results(RestDocs, RestResults, NewViewKVs, NewDocIdViewIdKeys).


view_insert_doc_query_results(_Doc, [], [], ViewKVsAcc, ViewIdKeysAcc) ->
    {lists:reverse(ViewKVsAcc), lists:reverse(ViewIdKeysAcc)};
view_insert_doc_query_results(#doc{id=DocId}=Doc, [ResultKVs|RestResults], [{View, KVs}|RestViewKVs], ViewKVsAcc, ViewIdKeysAcc) ->
    % Take any identical keys and combine the values
    ResultKVs2 = lists:foldl(
        % Key is the bounding box of the geometry,
        % Value is a tuple of the the geometry and the actual value
        fun({Key,Value}, [{PrevKey,PrevVal}|AccRest]) ->
            case Key == PrevKey of
            true ->
                case PrevVal of
                {dups, Dups} ->
                    [{PrevKey, {dups, [Value|Dups]}} | AccRest];
                _ ->
                    [{PrevKey, {dups, [Value,PrevVal]}} | AccRest]
                end;
            false ->
                [{Key,Value},{PrevKey,PrevVal}|AccRest]
            end;
        (KV, []) ->
           [KV]
        end, [], lists:sort(ResultKVs)),
    NewKVs = [{{Key, DocId}, Value} || {Key, Value} <- ResultKVs2],
    NewViewKVsAcc = [{View, NewKVs ++ KVs} | ViewKVsAcc],
    NewViewIdKeys = [{View#spatial.id_num, Key} || {Key, _Value} <- ResultKVs2],
    NewViewIdKeysAcc = NewViewIdKeys ++ ViewIdKeysAcc,
    view_insert_doc_query_results(Doc, RestResults, RestViewKVs, NewViewKVsAcc, NewViewIdKeysAcc).

spatial_docs(MapCtx, Docs, EmptyResults) ->
    spatial_docs(MapCtx, Docs, EmptyResults, []).

spatial_docs(_MapCtx, [], _EmptyResults, Acc) ->
    lists:reverse(Acc);
spatial_docs(MapCtx, [Doc | RestDocs], EmptyResults, Acc) ->
    {DocBody, DocMeta} = couch_doc:to_raw_json_binary_views(Doc),
    % NOTE vmx: perhaps should map_doc renamed to something more
    % general as it can be used for most indexers
    case mapreduce:map_doc(MapCtx, DocBody, DocMeta) of
    {ok, FunsResults} ->
        % the results are a json array of function map yields like this:
        % [FunResults1, FunResults2 ...]
        % where funresults is are json arrays of key value pairs:
        % [{Geom1Json, Value1Json}, {Geom2Json, Value2Json}]
        % Convert the key, value pairs to tuples like
        % [{Bbox1, {Geom1, Value1}}, {Bbox2, {Geom2, Value2}}]
        SpatialResults = lists:map(
            fun(FunRs) ->
                case FunRs of
                    [] -> [];
                    % do some post-processing of the result documents
                    FunRs -> process_results(FunRs)
                end
            end,
            FunsResults),
        spatial_docs(MapCtx, RestDocs, EmptyResults, [SpatialResults | Acc]);
    {error, Reason} ->
        ?LOG_ERROR("Error computing spatial result for document `~s`: ~p",
            [Doc#doc.id, Reason]),
        spatial_docs(MapCtx, RestDocs, EmptyResults, [EmptyResults | Acc])
    end.


% This fun computes once for each document
% This is from an old revision (796805) of couch_view_updater
process_doc(Db, Owner, MapCtx, EmptyResults, DocInfo, {Docs, Group, IndexKVs, DocIdIndexIdKeys}) ->
    #spatial_group{ design_options = DesignOptions } = Group,
    #doc_info{id=DocId, deleted=Deleted} = DocInfo,
    LocalSeq = proplists:get_value(<<"local_seq">>,
        DesignOptions, false),
    DocOpts = case LocalSeq of
        true ->
            [conflicts, deleted_conflicts, local_seq];
        _ ->
            [conflicts, deleted_conflicts]
    end,
    case DocId of
    <<?DESIGN_DOC_PREFIX, _/binary>> -> % we skip design docs
        {Docs, Group, IndexKVs, DocIdIndexIdKeys};
    _ ->
        {Docs2, DocIdIndexIdKeys2} =
        if Deleted ->
            {Docs, [{DocId, []} | DocIdIndexIdKeys]};
        true ->
            {ok, Doc} = couch_db:open_doc_int(Db, DocInfo,
                DocOpts),
            {[Doc | Docs], DocIdIndexIdKeys}
        end,

        case couch_util:should_flush() of
        true ->
            Results = spatial_docs(MapCtx, Docs2, EmptyResults),
            {ViewKVs3, DocIdViewIdKeys3} = view_insert_query_results(Docs2,
                Results, IndexKVs, DocIdIndexIdKeys2),
            {ok, Group2} = write_changes(Group, ViewKVs3, DocIdViewIdKeys3,
                DocInfo#doc_info.local_seq),
            if is_pid(Owner) ->
                ok = gen_server:cast(Owner, {partial_update, self(), Group2});
            true -> ok end,
            garbage_collect(),
            IndexEmptyKVs = [{Index, []} || Index <- Group#spatial_group.indexes],
            {[], Group2, IndexEmptyKVs, []};
        false ->
            {Docs2, Group, IndexKVs, DocIdIndexIdKeys2}
        end
    end.


write_changes(Group, IndexKeyValuesToAdd, DocIdIndexIdKeys, NewSeq) ->
    #spatial_group{id_btree=IdBtree, fd=Fd} = Group,
    AddDocIdIndexIdKeys = [{DocId, IndexIdKeys} || {DocId, IndexIdKeys} <- DocIdIndexIdKeys, IndexIdKeys /= []],
    RemoveDocIds = [DocId || {DocId, IndexIdKeys} <- DocIdIndexIdKeys, IndexIdKeys == []],
    LookupDocIds = [DocId || {DocId, _IndexIdKeys} <- DocIdIndexIdKeys],
    {ok, LookupResults, IdBtree2}
        = couch_btree:query_modify(IdBtree, LookupDocIds, AddDocIdIndexIdKeys, RemoveDocIds),
    KeysToRemoveByIndex = lists:foldl(
        fun(LookupResult, KeysToRemoveByIndexAcc) ->
            case LookupResult of
            {ok, {DocId, IndexIdKeys}} ->
                lists:foldl(
                    fun({IndexId, Key}, KeysToRemoveByIndexAcc2) ->
                        dict:append(IndexId, {Key, DocId}, KeysToRemoveByIndexAcc2)
                    end,
                    KeysToRemoveByIndexAcc, IndexIdKeys);
            {not_found, _} ->
                KeysToRemoveByIndexAcc
            end
        end,
        dict:new(), LookupResults),
    Indexes2 = lists:zipwith(fun(Index, {_Index, AddKeyValues}) ->
        KeysToRemove = couch_util:dict_find(Index#spatial.id_num, KeysToRemoveByIndex, []),
        %?LOG_DEBUG("storing spatial data: ~n~p~n~p~n~p",
        %           [Index, AddKeyValues, KeysToRemove]),
        {ok, IndexTreePos, IndexTreeHeight} = vtree:add_remove(
                Fd, Index#spatial.treepos, Index#spatial.treeheight,
                AddKeyValues, KeysToRemove),
        case IndexTreePos =/= Index#spatial.treepos of
        true ->
             Index#spatial{treepos=IndexTreePos, treeheight=IndexTreeHeight,
                 update_seq=NewSeq};
        _ ->
             Index#spatial{treepos=IndexTreePos, treeheight=IndexTreeHeight}
        end
    end, Group#spatial_group.indexes, IndexKeyValuesToAdd),
    couch_file:flush(Fd),
    Group2 = Group#spatial_group{indexes=Indexes2, current_seq=NewSeq, id_btree=IdBtree2},
    lists:foreach(fun(Index) ->
        ?LOG_INFO("Position of the spatial index (~p) root node: ~p",
                [Index#spatial.id_num, Index#spatial.treepos])
    end, Indexes2),
    {ok, Group2}.


% NOTE vmx: This is kind of ugly. This function is needed for a benchmark for
%     the replication filter
% Return the bounding box of a GeoJSON geometry. "Geo" is wrapped in
% brackets ({}) as returned from proplists:get_value()
geojson_get_bbox(Geo) ->
    {Bbox, {_, nil}} = process_result([Geo|[nil]]),
    Bbox.


process_results(Results) ->
    % NOTE vmx (2011-02-01): the ordering of the results doesn't matter
    %     therefore we don't need to reverse the list.
    lists:foldl(fun(Result, Acc) ->
        [process_result(Result)|Acc]
    end, [], Results).

process_result({K, V}) ->
    {Geo} = ?JSON_DECODE(K),
    Value = ?JSON_DECODE(V),
    Type = binary_to_atom(proplists:get_value(<<"type">>, Geo), utf8),
    Bbox = case Type of
    'GeometryCollection' ->
        Geometries = proplists:get_value(<<"geometries">>, Geo),
        lists:foldl(fun({Geometry}, CurBbox) ->
            Type2 = binary_to_atom(
                proplists:get_value(<<"type">>, Geometry), utf8),
            Coords = proplists:get_value(<<"coordinates">>, Geometry),
            case proplists:get_value(<<"bbox">>, Geo) of
            undefined ->
                extract_bbox(Type2, Coords, CurBbox);
            Bbox2 ->
                Bbox2
            end
        end, nil, Geometries);
    _ ->
        Coords = proplists:get_value(<<"coordinates">>, Geo),
        case proplists:get_value(<<"bbox">>, Geo) of
        undefined ->
            extract_bbox(Type, Coords);
        Bbox2 ->
            Bbox2
        end
    end,

    Geom = geojsongeom_to_geocouch(Geo),
    {erlang:list_to_tuple(Bbox), {Geom, Value}}.


extract_bbox(Type, Coords) ->
    extract_bbox(Type, Coords, nil).

extract_bbox(Type, Coords, InitBbox) ->
    case Type of
    'Point' ->
        bbox([Coords], InitBbox);
    'LineString' ->
        bbox(Coords, InitBbox);
    'Polygon' ->
        % holes don't matter for the bounding box
        bbox(hd(Coords), InitBbox);
    'MultiPoint' ->
        bbox(Coords, InitBbox);
    'MultiLineString' ->
        lists:foldl(fun(Linestring, CurBbox) ->
            bbox(Linestring, CurBbox)
        end, InitBbox, Coords);
    'MultiPolygon' ->
        lists:foldl(fun(Polygon, CurBbox) ->
            bbox(hd(Polygon), CurBbox)
        end, InitBbox, Coords)
    end.

bbox([], {Min, Max}) ->
    Min ++ Max;
bbox([Coords|Rest], nil) ->
    bbox(Rest, {Coords, Coords});
bbox(Coords, Bbox) when is_list(Bbox)->
    MinMax = lists:split(length(Bbox) div 2, Bbox),
    bbox(Coords, MinMax);
bbox([Coords|Rest], {Min, Max}) ->
    Min2 = lists:zipwith(fun(X, Y) -> erlang:min(X,Y) end, Coords, Min),
    Max2 = lists:zipwith(fun(X, Y) -> erlang:max(X,Y) end, Coords, Max),
    bbox(Rest, {Min2, Max2}).


% @doc Transforms a GeoJSON geometry (as Erlang terms), to an internal
% structure
geojsongeom_to_geocouch(Geom) ->
    Type = proplists:get_value(<<"type">>, Geom),
    Coords = case Type of
    <<"GeometryCollection">> ->
        Geometries = proplists:get_value(<<"geometries">>, Geom),
        [geojsongeom_to_geocouch(G) || {G} <- Geometries];
    _ ->
        proplists:get_value(<<"coordinates">>, Geom)
    end,
    {binary_to_atom(Type, utf8), Coords}.

% @doc Transforms internal structure to a GeoJSON geometry (as Erlang terms)
geocouch_to_geojsongeom({Type, Coords}) ->
    Coords2 = case Type of
    'GeometryCollection' ->
        Geoms = [geocouch_to_geojsongeom(C) || C <- Coords],
        {"geometries", Geoms};
    _ ->
        {<<"coordinates">>, Coords}
    end,
    {[{<<"type">>, Type}, Coords2]}.
