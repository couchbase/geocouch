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

-export([start_update/3, process_doc/3, finish_update/1]).

% for output (couch_http_spatial, couch_http_spatial_list)
-export([geocouch_to_geojsongeom/1]).

% for polygon search
-export([extract_bbox/2, geojsongeom_to_geocouch/1]).

-include("couch_db.hrl").
-include("couch_spatial.hrl").
-include_lib("vtree/include/vtree.hrl").


start_update(Partial, State, NumChanges) ->
    QueueOpts = [{max_size, 100000}, {max_items, 500}],
    {ok, DocQueue} = couch_work_queue:new(QueueOpts),
    {ok, WriteQueue} = couch_work_queue:new(QueueOpts),

    #spatial_state{
        update_seq = UpdateSeq,
        db_name = DbName,
        idx_name = IdxName
    } = State,

    InitState = State#spatial_state{
        first_build = UpdateSeq == 0,
        partial_resp_pid = Partial,
        doc_acc = [],
        doc_queue = DocQueue,
        write_queue = WriteQueue
    },

    Self = self(),
    SpatialFun = fun() ->
        couch_task_status:add_task([
            {type, indexer},
            {database, DbName},
            {design_document, IdxName},
            {progress, 0},
            {changes_done, 0},
            {total_changes, NumChanges}
        ]),
        couch_task_status:set_update_frequency(500),
        map_docs(Self, InitState)
    end,
    WriteFun = fun() -> write_results(Self, InitState) end,

    spawn_link(SpatialFun),
    spawn_link(WriteFun),

    {ok, InitState}.


process_doc(Doc, Seq, #spatial_state{doc_acc=Acc}=State) when
        length(Acc) > 100 ->
    couch_work_queue:queue(State#spatial_state.doc_queue, lists:reverse(Acc)),
    process_doc(Doc, Seq, State#spatial_state{doc_acc=[]});
process_doc(nil, Seq, #spatial_state{doc_acc=Acc}=State) ->
    {ok, State#spatial_state{doc_acc=[{nil, Seq, nil} | Acc]}};
process_doc(#doc{id=Id, deleted=true}, Seq,
        #spatial_state{doc_acc=Acc}=State) ->
    {ok, State#spatial_state{doc_acc=[{Id, Seq, deleted} | Acc]}};
process_doc(#doc{id=Id}=Doc, Seq, #spatial_state{doc_acc=Acc}=State) ->
    {ok, State#spatial_state{doc_acc=[{Id, Seq, Doc} | Acc]}}.


finish_update(State) ->
    #spatial_state{
        doc_acc = Acc,
        doc_queue = DocQueue
    } = State,
    if Acc /= [] ->
        couch_work_queue:queue(DocQueue, Acc);
        true -> ok
    end,
    couch_work_queue:close(DocQueue),
    receive
        {new_state, NewState} ->
            {ok, NewState#spatial_state{
                first_build = undefined,
                partial_resp_pid = undefined,
                doc_acc = undefined,
                doc_queue = undefined,
                write_queue = undefined,
                query_server = nil
            }}
    end.


map_docs(Parent, State0) ->
    #spatial_state{
        doc_queue = DocQueue,
        write_queue = WriteQueue,
        query_server = QueryServer
    }= State0,
    case couch_work_queue:dequeue(DocQueue) of
        closed ->
            couch_query_servers:stop_doc_map(QueryServer),
            couch_work_queue:close(WriteQueue);
        {ok, Dequeued} ->
            State1 = case QueryServer of
                nil -> start_query_server(State0);
                _ -> State0
            end,
            {ok, MapResults} = compute_spatial_results(State1, Dequeued),
            couch_work_queue:queue(WriteQueue, MapResults),
            map_docs(Parent, State1)
    end.


compute_spatial_results(#spatial_state{query_server = Qs}, Dequeued) ->
    % Run all the non deleted docs through the view engine and
    % then pass the results on to the writer process.
    DocFun = fun
        ({nil, Seq, _}, {SeqAcc, AccDel, AccNotDel}) ->
            {erlang:max(Seq, SeqAcc), AccDel, AccNotDel};
        ({Id, Seq, deleted}, {SeqAcc, AccDel, AccNotDel}) ->
            {erlang:max(Seq, SeqAcc), [{Id, []} | AccDel], AccNotDel};
        ({_Id, Seq, Doc}, {SeqAcc, AccDel, AccNotDel}) ->
            {erlang:max(Seq, SeqAcc), AccDel, [Doc | AccNotDel]}
    end,
    FoldFun = fun(Docs, Acc) ->
        lists:foldl(DocFun, Acc, Docs)
    end,
    {MaxSeq, DeletedResults, Docs} =
        lists:foldl(FoldFun, {0, [], []}, Dequeued),
    {ok, MapResultList} = couch_query_servers:map_docs_raw(Qs, Docs),
    NotDeletedResults = lists:zipwith(
        fun(#doc{id = Id}, MapResults) -> {Id, MapResults} end,
        Docs,
        MapResultList),
    AllMapResults = DeletedResults ++ NotDeletedResults,
    update_task(length(AllMapResults)),
    {ok, {MaxSeq, AllMapResults}}.


write_results(Parent, State) ->
    #spatial_state{
        write_queue = WriteQueue,
        views = Views
    } = State,
    case couch_work_queue:dequeue(WriteQueue) of
        closed ->
            Parent ! {new_state, State};
        {ok, Info} ->
            EmptyKVs = [{View#spatial.id_num, []} || View <- Views],
            {Seq, ViewKVs, DocIdKeys} = merge_results(Info, 0, EmptyKVs, []),
            NewState = write_kvs(State, Seq, ViewKVs, DocIdKeys),
            send_partial(NewState#spatial_state.partial_resp_pid, NewState),
            write_results(Parent, NewState)
    end.


start_query_server(State) ->
    #spatial_state{
        language = Language,
        lib = Lib,
        views = Views
    } = State,
    Defs = [View#spatial.def || View <- Views],
    {ok, QServer} = couch_query_servers:start_doc_map(Language, Defs, Lib),
    State#spatial_state{query_server=QServer}.


% This is a verbatim copy from couch_mrview_updater
merge_results([], SeqAcc, ViewKVs, DocIdKeys) ->
    {SeqAcc, ViewKVs, DocIdKeys};
merge_results([{Seq, Results} | Rest], SeqAcc, ViewKVs, DocIdKeys) ->
    Fun = fun(RawResults, {VKV, DIK}) ->
        merge_results(RawResults, VKV, DIK)
    end,
    {ViewKVs1, DocIdKeys1} = lists:foldl(Fun, {ViewKVs, DocIdKeys}, Results),
    merge_results(Rest, erlang:max(Seq, SeqAcc), ViewKVs1, DocIdKeys1).


% The processing of the results is different for each indexer
merge_results({DocId, []}, ViewKVs, DocIdKeys) ->
    {ViewKVs, [{DocId, []} | DocIdKeys]};
merge_results({DocId, RawResults}, ViewKVs, DocIdKeys) ->
    JsonResults = couch_query_servers:raw_to_ejson(RawResults),
    Results = [[process_result(Res) || Res <- FunRs] || FunRs <- JsonResults],
    {ViewKVs1, ViewIdKeys} = insert_results(DocId, Results, ViewKVs, [], []),
    {ViewKVs1, [ViewIdKeys | DocIdKeys]}.


insert_results(DocId, [], [], ViewKVs, ViewIdKeys) ->
    {lists:reverse(ViewKVs), {DocId, ViewIdKeys}};
insert_results(DocId, [KVs | RKVs], [{Id, VKVs} | RVKVs], VKVAcc, VIdKeys) ->
    CombineDupesFun = fun
        ({Key, {Geom, Val}}, {[{Key, {dups, {Geom, Vals}}} | Rest], IdKeys}) ->
            {[{Key, {dups, {Geom, [Val | Vals]}}} | Rest], IdKeys};
        ({Key, {Geom, Val1}}, {[{Key, {Geom, Val2}} | Rest], IdKeys}) ->
            {[{Key, {dups, {Geom, [Val1, Val2]}}} | Rest], IdKeys};
        ({Key, _}=KV, {Rest, IdKeys}) ->
            {[KV | Rest], [{Id, Key} | IdKeys]}
    end,
    InitAcc = {[], VIdKeys},
    {Duped, VIdKeys0} = lists:foldl(CombineDupesFun, InitAcc, lists:sort(KVs)),

    FinalKVs = lists:map(fun
        ({Key, {dups, {Geom, Vals}}}) ->
            #kv_node{
                key = Key,
                docid = DocId,
                geometry = Geom,
                body = ?term_to_bin({dups, Vals})
            };
        ({Key, {Geom, Val}}) ->
            #kv_node{
                key = Key,
                docid = DocId,
                geometry = Geom,
                body = ?term_to_bin(Val)
            }
        end, Duped) ++ VKVs,
    insert_results(DocId, RKVs, RVKVs, [{Id, FinalKVs} | VKVAcc], VIdKeys0).


write_kvs(State, UpdateSeq, ViewKVs, DocIdKeys) ->
    #spatial_state{
        id_btree=IdBtree,
        first_build=FirstBuild,
        views = Views
    } = State,

    {ok, ToRemove, IdBtree2} = update_id_btree(IdBtree, DocIdKeys, FirstBuild),
    ToRemByView = collapse_rem_keys(ToRemove, dict:new()),

    UpdateView = fun(#spatial{id_num = ViewId} = View, {ViewId, KVs}) ->
        ToRem = couch_util:dict_find(ViewId, ToRemByView, []),
        Vtree = vtree_delete:delete(View#spatial.vtree, ToRem),
        Vtree2 = vtree_insert:insert(Vtree, KVs),
        NewUpdateSeq = case
                Vtree2#vtree.root =/= (View#spatial.vtree)#vtree.root of
            true -> UpdateSeq;
            false -> View#spatial.update_seq
        end,
        View#spatial{vtree=Vtree2, update_seq=NewUpdateSeq}
    end,

    State#spatial_state{
        views = lists:zipwith(UpdateView, Views, ViewKVs),
        update_seq = UpdateSeq,
        id_btree = IdBtree2
    }.


% This is a verbatim copy from couch_mrview_updater
update_id_btree(Btree, DocIdKeys, true) ->
    ToAdd = [{Id, DIKeys} || {Id, DIKeys} <- DocIdKeys, DIKeys /= []],
    couch_btree:query_modify(Btree, [], ToAdd, []);
update_id_btree(Btree, DocIdKeys, _) ->
    ToFind = [Id || {Id, _} <- DocIdKeys],
    ToAdd = [{Id, DIKeys} || {Id, DIKeys} <- DocIdKeys, DIKeys /= []],
    ToRem = [Id || {Id, DIKeys} <- DocIdKeys, DIKeys == []],
    couch_btree:query_modify(Btree, ToFind, ToAdd, ToRem).


% Use this step to convert the data from tuples to KV-nodes where only the
% `docid` and the `key` is set (that's enough for deleting them from the tree)
collapse_rem_keys([], Acc) ->
    Acc;
collapse_rem_keys([{ok, {DocId, ViewIdKeys}} | Rest], Acc) ->
    NewAcc = lists:foldl(fun({ViewId, Key}, Acc2) ->
        Node = #kv_node{
            docid = DocId,
            key = Key
        },
        dict:append(ViewId, Node, Acc2)
    end, Acc, ViewIdKeys),
    collapse_rem_keys(Rest, NewAcc);
collapse_rem_keys([{not_found, _} | Rest], Acc) ->
    collapse_rem_keys(Rest, Acc).


% This is a verbatim copy from couch_mrview_updater
send_partial(Pid, State) when is_pid(Pid) ->
    gen_server:cast(Pid, {new_state, State});
send_partial(_, _) ->
    ok.


% This is a verbatim copy from couch_mrview_updater
update_task(NumChanges) ->
    [Changes, Total] = couch_task_status:get([changes_done, total_changes]),
    Changes2 = Changes + NumChanges,
    Progress = case Total of
        0 ->
            % updater restart after compaction finishes
            0;
        _ ->
            (Changes2 * 100) div Total
    end,
    couch_task_status:update([{progress, Progress}, {changes_done, Changes2}]).


% The multidimensional case with a geometry
% XXX NOTE vmx 2012-11-29: Currently it is expected that the geometry
%     is the first value of the emit.
process_result([[{Geo}|Rest]|[Value]]) ->
    Tuples = process_range(Rest),
    {Bbox, Geom} = process_geometry(Geo),
    {Bbox ++ Tuples, {Geom, Value}};
% The multidimensional case without a geometry
process_result([MultiDim|[Value]]) when is_list(MultiDim) ->
    Tuples = process_range(MultiDim),
    {Tuples, {nil, Value}};
% There old case when only two dimensions were supported
process_result([{Geo}|[Value]]) ->
    {Bbox, Geom} = process_geometry(Geo),
    {Bbox, {Geom, Value}}.


% Transform the range from the query (which is JSON based) to a list of tuples
% that can be used for actual querying
process_range(Range) ->
    lists:map(
        fun([Min, Max]) ->
            {Min, Max};
        % A single value means that the mininum and the maximum are the same
        (Single) ->
            {Single, Single}
    end, Range).


% Returns an Erlang encoded geometry and the corresponding bounding box
process_geometry(Geo) ->
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
    {Bbox, Geom}.


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

bbox([], Range) ->
    Range;
bbox([[X, Y]|Rest], nil) ->
    bbox(Rest, [{X, X}, {Y, Y}]);
bbox([Coords|Rest], Range) ->
    Range2 = lists:zipwith(
        fun(Coord, {Min, Max}) ->
            {erlang:min(Coord, Min), erlang:max(Coord, Max)}
        end, Coords, Range),
    bbox(Rest, Range2).


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
