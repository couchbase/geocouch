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
-include_lib("eunit/include/eunit.hrl").

-export([update/2]).

% for benchmark script
-export([geojson_get_bbox/1]).

-include("couch_db.hrl").
-include("couch_spatial.hrl").

update(Owner, Group) ->
    #spatial_group{
        db = #db{name=DbName} = Db,
        name = GroupName,
        current_seq = Seq,
        purge_seq = PurgeSeq
    } = Group,
    couch_task_status:add_task(<<"Spatial Group Indexer">>, <<DbName/binary," ",GroupName/binary>>, <<"Starting index update">>),

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
    % update status every half second
    couch_task_status:set_update_frequency(500),
    {ok, _, {_,{UncomputedDocs, Group3, ViewKVsToAdd, DocIdViewIdKeys}}}
        = couch_db:enum_docs_since(Db, Seq,
        fun(DocInfo, _, {ChangesProcessed, Acc}) ->
                couch_task_status:update("Processed ~p of ~p changes (~p%)",
                        [ChangesProcessed, TotalChanges, (ChangesProcessed*100) div TotalChanges]),
            %?LOG_DEBUG("enum_doc_since: ~p", [Acc]),
            {ok, {ChangesProcessed+1, process_doc(Db, Owner, DocInfo, Acc)}}
        end, {0, {[], Group, IndexEmptyKVs, []}}, []),
     %?LOG_DEBUG("enum_doc_since results: ~p~n~p~n~p", [UncomputedDocs, ViewKVsToAdd, DocIdViewIdKeys]),
    couch_task_status:set_update_frequency(0),
    couch_task_status:update("Finishing."),
    {Group4, Results} = spatial_compute(Group3, UncomputedDocs),
    % Output is way to huge
    %?LOG_DEBUG("spatial_compute results: ~p", [Results]),
    % XXX vmx: I don't quite understand what view_insert_query_results does.
    %    It seems to optimize the result
    {ViewKVsToAdd2, DocIdViewIdKeys2} = view_insert_query_results(
            UncomputedDocs, Results, ViewKVsToAdd, DocIdViewIdKeys),
    couch_query_servers:stop_doc_map(Group4#spatial_group.query_server),
    NewSeq = couch_db:get_update_seq(Db),
?LOG_DEBUG("new seq num: ~p", [NewSeq]),
    {ok, Group5} = write_changes(Group4, ViewKVsToAdd2, DocIdViewIdKeys2,
                NewSeq),
    exit({new_group, Group5#spatial_group{query_server=nil}}).




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





% Pendant to couch_view_updater:view_compute/2
spatial_compute(Group, []) ->
    {Group, []};
spatial_compute(#spatial_group{def_lang=DefLang, query_server=QueryServerIn}=Group, Docs) ->
    {ok, QueryServer} =
    case QueryServerIn of
    nil -> % spatial funs not started
        Functions = [Index#spatial.def || Index <- Group#spatial_group.indexes],
        start_spatial(DefLang, Functions);
    _ ->
        {ok, QueryServerIn}
    end,
    {ok, Results} = spatial_docs(QueryServer, Docs),
    {Group#spatial_group{query_server=QueryServer}, Results}.

    

% Pendant to couch_query_servers:start_doc_map/2
start_spatial(Lang, Functions) ->
    Proc = geocouch_duplicates:get_os_process(Lang),
    lists:foreach(fun(FunctionSource) ->
        true = couch_query_servers:proc_prompt(Proc, [<<"add_fun">>, FunctionSource])
    end, Functions),
    {ok, Proc}.

% Pendant to couch_query_servers:map_docs/2
spatial_docs(Proc, Docs) ->
    % send the documents
    Results = lists:map(
        fun(Doc) ->
            Json = couch_doc:to_json_obj(Doc, []),

            % NOTE vmx: perhaps should map_doc renamed to something more
            % general as it can be used for most indexers
            FunsResults = couch_query_servers:proc_prompt(Proc, [<<"map_doc">>, Json]),
            % the results are a json array of function map yields like this:
            % [FunResults1, FunResults2 ...]
            % where funresults is are json arrays of key value pairs:
            % [[Key1, Value1], [Key2, Value2]]
            % Convert the key, value pairs to tuples like
            % [{Key1, Value1}, {Key2, Value2}]
            lists:map(
                fun(FunRs) ->
                    case FunRs of
                    [] -> [];
                    FunRs ->
                        % do some post-processing of the result documents
                        FunRs2 = process_result(FunRs),
                        %JsonDoc = couch_query_servers:json_doc(Doc),
                        ?LOG_DEBUG("spatial_docs:~n~p~n~p", [FunRs, FunRs2]),
                        %[list_to_tuple(FunResult) || FunResult <- [FunRs]]
                        %[list_to_tuple(FunResult) || FunResult <- [FunRs2]]
                        [FunRs2]
                    end
                end,
            FunsResults)
        end,
        Docs),
    {ok, Results}.


% This fun computes once for each document
% This is from an old revision (796805) of couch_view_updater
process_doc(Db, Owner, DocInfo, {Docs, Group, IndexKVs, DocIdIndexIdKeys}) ->
    #spatial_group{ design_options = DesignOptions } = Group,

    #doc_info{id=DocId, revs=[#rev_info{deleted=Deleted}|_]} = DocInfo,
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
            {Group1, Results} = spatial_compute(Group, Docs2),
            {ViewKVs3, DocIdViewIdKeys3} = view_insert_query_results(Docs2,
                Results, IndexKVs, DocIdIndexIdKeys2),
            {ok, Group2} = write_changes(Group1, ViewKVs3, DocIdViewIdKeys3,
                DocInfo#doc_info.high_seq),
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
                Fd, Index#spatial.treepos, AddKeyValues, KeysToRemove),
        Index#spatial{treepos=IndexTreePos, treeheight=IndexTreeHeight}
    end, Group#spatial_group.indexes, IndexKeyValuesToAdd),
    Group2 = Group#spatial_group{indexes=Indexes2, current_seq=NewSeq, id_btree=IdBtree2},
    {ok, Group2}.


% NOTE vmx: This is kind of ugly. This function is needed for a benchmark for
%     the replication filter
% Return the bounding box of a GeoJSON geometry. "Geo" is wrapped in
% brackets ({}) as returned from proplists:get_value()
geojson_get_bbox(Geo) ->
    {Bbox, nil} = process_result([[Geo|[nil]]]),
    Bbox.


process_result([[{Geo}|[Value]]]) ->
    Type = proplists:get_value(<<"type">>, Geo),
    Bbox = case Type of
    <<"GeometryCollection">> ->
        Geometries = proplists:get_value(<<"geometries">>, Geo),
        lists:foldl(fun({Geometry}, CurBbox) ->
            Type2 = proplists:get_value(<<"type">>, Geometry),
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
    {erlang:list_to_tuple(Bbox), Value}.


extract_bbox(Type, Coords) ->
    extract_bbox(Type, Coords, nil).

extract_bbox(Type, Coords, InitBbox) ->
    case Type of
    <<"Point">> ->
        bbox([Coords], InitBbox);
    <<"LineString">> ->
        bbox(Coords, InitBbox);
    <<"Polygon">> ->
        % holes don't matter for the bounding box
        bbox(hd(Coords), InitBbox);
    <<"MultiPoint">> ->
        bbox(Coords, InitBbox);
    <<"MultiLineString">> ->
        lists:foldl(fun(Linestring, CurBbox) ->
            bbox(Linestring, CurBbox)
        end, InitBbox, Coords);
    <<"MultiPolygon">> ->
        lists:foldl(fun(Polygon, CurBbox) ->
            bbox(hd(Polygon), CurBbox)
        end, InitBbox, Coords)
    end.

bbox(Coords) ->
    bbox(Coords, nil).

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


% The tests are based on the examples of the GeoJSON format specification
bbox_test() ->
    ?assertEqual([100.0, 0.0, 101.0, 1.0], 
                 bbox([[100.0, 0.0], [101.0, 1.0]])),
    ?assertEqual([100.0, 0.0, 101.0, 1.0], 
                 bbox([[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0],
                       [100.0, 0.0]])),
    ?assertEqual([-10.0, -1.0, -72.8, 9.5, 101.0, 0.0, 50.4, 58.69], 
                 bbox([[-10.0, 0.0, 50.4, 58.69], [101.0, -1.0, -72.8, 9.5]])).

bbox_nil_test() ->
    ?assertEqual([100.0, 0.0, 101.0, 1.0], 
                 bbox([[100.0, 0.0], [101.0, 1.0]], nil)),
    ?assertEqual([100.0, 0.0, 101.0, 1.0], 
                 bbox([[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0],
                       [100.0, 0.0]], nil)),
    ?assertEqual([-10.0, -1.0, -72.8, 9.5, 101.0, 0.0, 50.4, 58.69], 
                 bbox([[-10.0, 0.0, 50.4, 58.69], [101.0, -1.0, -72.8, 9.5]],
                 nil)).

bbox_initbbox_test() ->
    ?assertEqual([100.0, 0.0, 200.36, 1.0],
                 bbox([[100.0, 0.0], [110.0, 1.0]],
                      [105.4, 20.3, 200.36, 0.378])),
    ?assertEqual([100.0, 0.0, 200.36, 1.0], 
                 bbox([[100.0, 0.0], [110.0, 1.0]],
                      {[105.4, 20.3], [200.36, 0.378]})).

extract_bbox_point_test() ->
    ?assertEqual([100.0, 0.0, 100.0, 0.0],
                 extract_bbox(<<"Point">>, [100.0, 0.0])).

extract_bbox_linestring_test() ->
    ?assertEqual([100.0, 0.0, 101.0, 1.0],
                 extract_bbox(<<"LineString">>, [[100.0, 0.0], [101.0, 1.0]])).

extract_bbox_polygon_test() ->
    ?assertEqual([100.0, 0.0, 101.0, 1.0],
                 extract_bbox(<<"Polygon">>, [
        [[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]],
        [[100.2, 0.2], [100.8, 0.2], [100.8, 0.8], [100.2, 0.8], [100.2, 0.2]]
    ])).

extract_bbox_multilinestring_test() ->
    ?assertEqual([100.0, 0.0, 103.0, 3.0],
                 extract_bbox(<<"MultiLineString">>, [
        [[100.0, 0.0], [101.0, 1.0]],
        [[102.0, 2.0], [103.0, 3.0]]
    ])).

extract_bbox_multipolygon_test() ->
    ?assertEqual([100.0, 0.0, 103.0, 3.0],
                 extract_bbox(<<"MultiPolygon">>, [
        [[[102.0, 2.0], [103.0, 2.0], [103.0, 3.0], [102.0, 3.0], [102.0, 2.0]]],
        [[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]],
         [[100.2, 0.2], [100.8, 0.2], [100.8, 0.8], [100.2, 0.8], [100.2, 0.2]]]
    ])).

process_result_geometrycollection_test() ->
    Geojson = {[{<<"type">>,<<"GeometryCollection">>},
                {<<"geometries">>,
                 [{[{<<"type">>,<<"Point">>},
                    {<<"coordinates">>,[100.0,0.0]}]},
                  {[{<<"type">>,<<"LineString">>},
                    {<<"coordinates">>,[[101.0,0.0],[102.0,1.0]]}]}]}]},
    {Bbox, <<"somedoc">>} = process_result([[Geojson, <<"somedoc">>]]),
    ?assertEqual({100.0, 0.0, 102.0, 1.0}, Bbox).

process_result_geometrycollection_fail_test() ->
    % collection contains geometries with different dimensions
    Geojson = {[{<<"type">>,<<"GeometryCollection">>},
                {<<"geometries">>,
                 [{[{<<"type">>,<<"Point">>},
                    {<<"coordinates">>,[100.0,0.0,54.5]}]},
                  {[{<<"type">>,<<"LineString">>},
                    {<<"coordinates">>,[[101.0,0.0],[102.0,1.0]]}]}]}]},
    ?assertError(function_clause, process_result([[Geojson, <<"somedoc">>]])).
    
process_result_point_test() ->
    Geojson = {[{<<"type">>,<<"Point">>},
                {<<"coordinates">>,[100.0,0.0]}]},
    {Bbox, <<"somedoc">>} = process_result([[Geojson, <<"somedoc">>]]),
    ?assertEqual({100.0, 0.0, 100.0, 0.0}, Bbox).

process_result_point_bbox_test() ->
    Geojson = {[{<<"type">>,<<"Point">>},
                {<<"coordinates">>,[100.0,0.0]},
                {<<"bbox">>,[100.0,0.0,105.54,8.614]}]},
    {Bbox, <<"somedoc">>} = process_result([[Geojson, <<"somedoc">>]]),
    ?assertEqual({100.0, 0.0, 105.54, 8.614}, Bbox).

process_result_linestring_test() ->
    Geojson = {[{<<"type">>,<<"LineString">>},
                {<<"coordinates">>,[[101.0,0.0],[102.0,1.0]]}]},
    {Bbox, <<"somedoc">>} = process_result([[Geojson, <<"somedoc">>]]),
    ?assertEqual({101.0, 0.0, 102.0, 1.0}, Bbox).

process_result_linestring_toosmallbbox_test() ->
    Geojson = {[{<<"type">>,<<"LineString">>},
                {<<"coordinates">>,[[101.0,0.0],[102.0,1.0]]},
                {<<"bbox">>,[101.0,0.0,101.54,0.614]}]},
    {Bbox, <<"somedoc">>} = process_result([[Geojson, <<"somedoc">>]]),
    ?assertEqual({101.0, 0.0, 101.54, 0.614}, Bbox).
