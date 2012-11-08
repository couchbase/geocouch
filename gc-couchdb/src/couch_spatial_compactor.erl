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

-module(couch_spatial_compactor).

-include ("couch_db.hrl").
-include ("couch_spatial.hrl").

-export([compact/3, swap_compacted/2]).

-record(acc, {
    btree = nil,
    kvs = [],
    kvs_size = 0,
    changes = 0,
    total_changes
}).

-record(spatial_acc, {
    treepos = nil,
    treeheight = 0,
    kvs = [],
    kvs_size = 0,
    changes = 0,
    total_changes
}).



compact(_Db, State, Opts) ->
    case lists:member(recompact, Opts) of
        false -> compact(State);
        true -> recompact(State)
    end.

compact(State) ->
    #spatial_state{
        db_name = DbName,
        idx_name = IdxName,
        sig = Sig,
        update_seq = UpdateSeq,
        id_btree = IdBtree,
        views = Views
    } = State,

    EmptyState = couch_util:with_db(DbName, fun(Db) ->
        CompactFName = couch_spatial_util:compaction_file(DbName, Sig),
        {ok, CompactFd} = couch_spatial_util:open_file(CompactFName),
        couch_spatial_util:reset_index(Db, CompactFd, State)
    end),

    #spatial_state{
        id_btree = EmptyIdBtree,
        views = EmptyViews,
        fd = EmptyFd
    } = EmptyState,

    % XXX vmx 2012-10-22: Not sure why the [] case happens
    Count = case couch_btree:full_reduce(IdBtree) of
        {ok, []} -> 0;
        {ok, Count2} -> Count2
    end,

    TotalChanges = lists:foldl(
        fun(View, Acc) ->
            % NOTE vmx 2012-10-18: This can be slow, as it traverses the
            %     whole tree.
            {ok, Num} = couch_spatial_util:get_row_count(View),
            Acc + Num
        end,
        Count, Views),
    % Use "view_compaction" for now, that it shows up in Futons active tasks
    % screen. Think about a more generic way for the future.
    couch_task_status:add_task([
        {type, view_compaction},
        {database, DbName},
        {design_document, IdxName},
        {progress, 0}
    ]),

    BufferSize0 = couch_config:get(
        "view_compaction", "keyvalue_buffer_size", "2097152"
    ),
    BufferSize = list_to_integer(BufferSize0),

    FoldFun = fun(Kv, Acc) ->
        #acc{
            btree = Bt,
            kvs = Kvs,
            kvs_size = KvsSize0
        } = Acc,
        KvsSize = KvsSize0 + ?term_size(Kv),
        case KvsSize >= BufferSize of
            true ->
                {ok, Bt2} = couch_btree:add(Bt, lists:reverse([Kv | Kvs])),
                Acc2 = update_task(Acc, 1 + length(Kvs)),
                {ok, Acc2#acc{btree = Bt2, kvs = [], kvs_size = 0}};
            _ ->
                {ok, Acc#acc{kvs = [Kv | Kvs], kvs_size = KvsSize}}
        end
    end,

    InitAcc = #acc{
        total_changes = TotalChanges,
        btree = EmptyIdBtree
    },
    {ok, _, FinalAcc} = couch_btree:foldl(IdBtree, FoldFun, InitAcc),
    #acc{
        btree = Bt3,
        kvs = Uncopied
    } = FinalAcc,
    {ok, NewIdBtree} = couch_btree:add(Bt3, lists:reverse(Uncopied)),
    FinalAcc2 = update_task(FinalAcc, length(Uncopied)),
    SpatialAcc = #spatial_acc{
        kvs = FinalAcc2#acc.kvs,
        kvs_size = FinalAcc2#acc.kvs_size,
        changes = FinalAcc2#acc.changes,
        total_changes = FinalAcc2#acc.total_changes
    },

    {NewViews, _} = lists:mapfoldl(fun({View, EmptyView}, Acc) ->
        case View#spatial.treepos of
            % Tree is empty, just grab the the FD
            nil -> {EmptyView#spatial{fd = EmptyFd}, Acc};
            _ -> compact_spatial(View, EmptyView, BufferSize, Acc)
        end
    end, SpatialAcc, lists:zip(Views, EmptyViews)),

    unlink(EmptyFd),
    {ok, EmptyState#spatial_state{
        id_btree = NewIdBtree,
        views = NewViews,
        update_seq = UpdateSeq
    }}.


recompact(State) ->
    #spatial_state{
        fd = Fd
    } = State,
    link(Fd),
    {Pid, Ref} = erlang:spawn_monitor(fun() ->
        couch_index_updater:update(couch_spatial_index, State)
    end),
    receive
        {'DOWN', Ref, _, _, {updated, Pid, State2}} ->
            unlink(Fd),
            {ok, State2}
    end.


compact_spatial(View, EmptyView, BufferSize, Acc0) ->
    #spatial{
        fd = OldFd,
        treepos = OrigTreepos
    } = View,
    #spatial{
        fd = NewFd,
        treepos = EmptyTreepos,
        treeheight = EmptyTreeheight
    } = EmptyView,

    Fun = fun(Kv, Acc) ->
        #spatial_acc{
            treepos = TreePos,
            treeheight = TreeHeight,
            kvs = Kvs,
            kvs_size = KvsSize0
        } = Acc,
        KvsSize = KvsSize0 + ?term_size(Kv),
        case KvsSize >= BufferSize of
        true ->
            {ok, TreePos2, TreeHeight2} = vtree_bulk:bulk_load(
                NewFd, TreePos, TreeHeight, [Kv|Kvs]),
            Acc2 = update_task(Acc, 1 + length(Kvs)),
            Acc2#spatial_acc{
                treepos = TreePos2,
                treeheight = TreeHeight2,
                kvs = [],
                kvs_size = 0
            };
        false ->
            Acc#spatial_acc{
                kvs = [Kv|Kvs],
                kvs_size = KvsSize
            }
        end
    end,

    InitAcc = Acc0#spatial_acc{
        kvs = [],
        kvs_size = 0,
        treepos = EmptyTreepos,
        treeheight = EmptyTreeheight
    },

    %{TreePos3, TreeHeight3, Uncopied, _Total} = vtree:foldl(
    FinalAcc = vtree:foldl(OldFd, OrigTreepos, Fun, InitAcc),
    #spatial_acc{
        treepos = TreePos3,
        treeheight = TreeHeight3,
        kvs = Uncopied
    } = FinalAcc,
    {ok, NewTreePos, NewTreeHeight} = vtree_bulk:bulk_load(
        NewFd, TreePos3, TreeHeight3, Uncopied),
    FinalAcc2 = update_task(FinalAcc, length(Uncopied)),
    NewView = EmptyView#spatial{
        treepos = NewTreePos,
        treeheight = NewTreeHeight,
        fd = NewFd
    },
    {NewView, FinalAcc2}.


update_task(#acc{}=Acc, ChangesInc) ->
    #acc{
        changes = Changes0,
        total_changes = Total
    } = Acc,
    Changes = Changes0 + ChangesInc,
    couch_task_status:update([{progress, (Changes * 100) div Total}]),
    Acc#acc{changes = Changes};
update_task(#spatial_acc{}=Acc, ChangesInc) ->
    #spatial_acc{
        changes = Changes0,
        total_changes = Total
    } = Acc,
    Changes = Changes0 + ChangesInc,
    couch_task_status:update([{progress, (Changes * 100) div Total}]),
    Acc#spatial_acc{changes = Changes}.


swap_compacted(OldState, NewState) ->
    #spatial_state{
        sig = Sig,
        db_name = DbName,
        fd = Fd
    } = NewState,

    link(Fd),

    RootDir = couch_index_util:root_dir(),
    IndexFName = couch_spatial_util:index_file(DbName, Sig),
    CompactFName = couch_spatial_util:compaction_file(DbName, Sig),
    ok = couch_file:delete(RootDir, IndexFName),
    ok = file:rename(CompactFName, IndexFName),

    unlink(OldState#spatial_state.fd),
    couch_ref_counter:drop(OldState#spatial_state.ref_counter),
    {ok, NewRefCounter} = couch_ref_counter:start([Fd]),

    {ok, NewState#spatial_state{ref_counter=NewRefCounter}}.
