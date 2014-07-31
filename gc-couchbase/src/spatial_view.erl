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

-module(spatial_view).

% For the updater
-export([write_kvs/3, finish_build/3, get_state/1,
         start_reduce_context/1, end_reduce_context/1, view_name/2,
         update_tmp_files/3, view_bitmap/1]).
-export([update_index/5]).
% For the group
-export([design_doc_to_set_view_group/2, view_group_data_size/2,
         reset_view/1, setup_views/5, set_state/2]).
% For the utils
-export([clean_views/5, view_info/1]).
% For the compactor
-export([compact_view/6, apply_log/3]).
% For the main module
-export([get_row_count/1, make_wrapper_fun/2, fold/4, index_extension/0,
         make_key_options/1, should_filter/1]).
-export([stats_ets/1, server_name/1, sig_to_pid_ets/1, name_to_sig_ets/1,
         pid_to_sig_ets/1]).
% For the couch_set_view like calls
-export([get_spatial_view/4]).


-include_lib("couch_spatial.hrl").
-include("couch_db.hrl").
%-include_lib("couch_set_view/include/couch_set_view.hrl").
% Only needed for #writer_acc{}
% XXX vmx 2013-08-02: #writer_acc{} should be moved to public header
-include_lib("couch_set_view/src/couch_set_view_updater.hrl").
-include_lib("vtree/include/vtree.hrl").

% Same as in couch_btree.erl
-define(POINTER_BITS,   48).
-define(TREE_SIZE_BITS, 48).
-define(KEY_BITS,       12).
-define(MAX_KEY_SIZE,   ((1 bsl ?KEY_BITS) - 1)).
-define(VALUE_BITS,     28).

% Same as the compactor uses for the ID-btree
-define(SORTED_CHUNK_SIZE, 1024 * 1024).

% View specific limits (same as in couch_set_view_updater).
-define(VIEW_SINGLE_VALUE_BITS,     24).
-define(VIEW_ALL_VALUES_BITS,       ?VALUE_BITS).
-define(MAX_VIEW_SINGLE_VALUE_SIZE, ((1 bsl ?VIEW_SINGLE_VALUE_BITS) - 1)).
-define(MAX_VIEW_ALL_VALUES_SIZE,   ((1 bsl ?VIEW_ALL_VALUES_BITS) - 1)).


-define(SPATIAL_VIEW_SERVER_NAME_PROD, spatial_view_server_name_prod).
-define(SPATIAL_VIEW_SERVER_NAME_DEV, spatial_view_server_name_dev).
-define(SPATIAL_VIEW_STATS_ETS_PROD, spatial_view_stats_prod).
-define(SPATIAL_VIEW_NAME_TO_SIG_ETS_PROD, spatial_view_name_to_sig_prod).
-define(SPATIAL_VIEW_SIG_TO_PID_ETS_PROD, spatial_view_sig_to_pid_prod).
-define(SPATIAL_VIEW_PID_TO_SIG_ETS_PROD, spatial_view_pid_to_sig_prod).
-define(SPATIAL_VIEW_STATS_ETS_DEV, spatial_view_stats_dev).
-define(SPATIAL_VIEW_NAME_TO_SIG_ETS_DEV, spatial_view_name_to_sig_dev).
-define(SPATIAL_VIEW_SIG_TO_PID_ETS_DEV, spatial_view_sig_to_pid_dev).
-define(SPATIAL_VIEW_PID_TO_SIG_ETS_DEV, spatial_view_pid_to_sig_dev).


write_kvs(Group, TmpFiles0, ViewKVs) ->
    lists:foldl(
        fun({#set_view{id_num = Id}, KvList0}, {AccCount, TmpFiles}) ->
            TmpFileInfo0 = dict:fetch(Id, TmpFiles),
            {KvList, Mbb} = calc_mbb(KvList0),
            TmpFileInfo = TmpFileInfo0#set_view_tmp_file_info{extra = Mbb},

            KvBins = convert_primary_index_kvs_to_binary(KvList, Group, []),
            ViewRecords = lists:foldr(
                fun({KeyBin, ValBin}, Acc) ->
                    KvBin = [<<(byte_size(KeyBin)):16>>, KeyBin, ValBin],
                    [[<<(iolist_size(KvBin)):32>>, KvBin] | Acc]
                end,
                [], KvBins),
            ok = file:write(
                TmpFileInfo#set_view_tmp_file_info.fd, ViewRecords),
            TmpFiles2 = dict:store(Id, TmpFileInfo, TmpFiles),
            {AccCount + length(KvBins), TmpFiles2}
        end,
        {0, TmpFiles0}, ViewKVs).


% Calculate the mbb and also returns the keys as flattened list of floats
% TODO vmx 2013-07-01: Add support for geometries. They will get transformed
%     in this function. Also support single item values that will be expanded
%     to an 2-element array with the same value.
calc_mbb(KvList) ->
    Less = fun(A, B) -> A < B end,
    lists:mapfoldl(fun
        ({{Key0, DocId}, {PartId, Body}}, nil) ->
            Key = ?JSON_DECODE(Key0),
            Acc = Key,
            Value = {PartId, Body},
            {{{lists:flatten(Key), DocId}, Value}, Acc};
        ({{Key0, DocId}, {PartId, Body}}, Mbb) ->
            Key = ?JSON_DECODE(Key0),
            Acc = lists:map(fun({[KeyMin, KeyMax], [MbbMin, MbbMax]}) ->
                [vtree_util:min({KeyMin, MbbMin}, Less),
                 vtree_util:max({KeyMax, MbbMax}, Less)]
            end, lists:zip(Key, Mbb)),
            Value = {PartId, Body},
            {{{lists:flatten(Key), DocId}, Value}, Acc}
    end, nil, KvList).


% Convert the key from a list of 2-tuples to a list of raw doubles
convert_primary_index_kvs_to_binary([], _Group, Acc) ->
    lists:reverse(Acc);
convert_primary_index_kvs_to_binary([H | Rest], Group, Acc)->
    {{Key, DocId}, {PartId, Body}} = H,
    KeyBin = encode_key_docid(Key, DocId),
    couch_set_view_util:check_primary_key_size(
        KeyBin, ?MAX_KEY_SIZE, Key, DocId, Group),
    V = case Body of
    % TODO vmx 2013-07-01: Support dups properly (they are encoded here
    %     but never really taken into account througout the code
    {dups, Values} ->
        ValueListBinary = lists:foldl(
            fun(V, Acc2) ->
                couch_set_view_util:check_primary_value_size(
                    V, ?MAX_VIEW_SINGLE_VALUE_SIZE, Key, DocId, Group),
                <<Acc2/binary, (byte_size(V)):24, V/binary>>
            end,
            <<>>, Values),
        <<PartId:16, ValueListBinary/binary>>;
    _ ->
        couch_set_view_util:check_primary_value_size(
            Body, ?MAX_VIEW_SINGLE_VALUE_SIZE, Key, DocId, Group),
        <<PartId:16, (byte_size(Body)):24, Body/binary>>
    end,
    couch_set_view_util:check_primary_value_size(
        V, ?MAX_VIEW_ALL_VALUES_SIZE, Key, DocId, Group),
    convert_primary_index_kvs_to_binary(Rest, Group, [{KeyBin, V} | Acc]).


-spec encode_key_docid([number()], binary()) -> binary().
encode_key_docid(Key, DocId) ->
    BinKey = list_to_binary([<<K:64/native-float>> || K <- Key]),
    <<(length(Key)):16, BinKey/binary, DocId/binary>>.


% Build the tree out of the sorted files
finish_build(SetView, GroupFd, TmpFiles) ->
    #set_view{
        id_num = Id,
        indexer = View
    } = SetView,

    #spatial_view{
        vtree = Vt
    } = View,

    #set_view_tmp_file_info{name = ViewFile} = dict:fetch(Id, TmpFiles),
    {ok, NewVtRoot} = vtree_copy:from_sorted_file(
        Vt, ViewFile, GroupFd,
        fun couch_set_view_updater:file_sorter_initial_build_format_fun/1),
    ok = file2:delete(ViewFile),
    SetView#set_view{
        indexer = View#spatial_view{
            vtree = Vt#vtree{root = NewVtRoot}
        }
    }.


% In order to build the spatial index bottom-up we need to have supply
% the enclosing bounding box
view_info(View) ->
    Mbb = ((View#spatial_view.vtree)#vtree.root)#kp_node.key,
    MbbEncoded = << <<Min:64/native-float, Max:64/native-float>> ||
        {Min, Max} <- Mbb>>,
    Dimension = integer_to_binary(length(Mbb)),
    [Dimension, $\n, MbbEncoded].

% Return the state of a view (which will be stored in the header)
get_state(View) ->
    get_vtree_state(View#spatial_view.vtree).

% XXX vmx 2013-01-02: Should perhaps be moved to the vtree itself (and then
%    renamed to `get_state`
get_vtree_state(Vt) ->
    case Vt#vtree.root of
    nil ->
        <<0:?POINTER_BITS, 0:?TREE_SIZE_BITS>>;
    #kp_node{} = Root ->
        #kp_node{
            key = Key,
            childpointer = Pointer,
            treesize = Size,
            reduce = Reduce,
            mbb_orig = MbbOrig
        } = Root,
        % TODO vmx 2013-06-27: Proper binary encoding would make sense,
        %     but this is good for now.
        Rest = term_to_binary({Key, Reduce, MbbOrig}),
        <<Pointer:?POINTER_BITS, Size:?TREE_SIZE_BITS, Rest/binary>>
    end.

set_state(View, State) ->
    Vt = set_vtree_state(View#spatial_view.vtree, State),
    View#spatial_view{vtree = Vt}.

% XXX vmx 2014-02-04: Should perhaps be moved to the vtree itself (and then
%    renamed to `set_state`
set_vtree_state(Vt, Root0) ->
    Root = case Root0 of
    nil ->
        nil;
    <<0:?POINTER_BITS, 0:?TREE_SIZE_BITS>> ->
        nil;
    <<Pointer:?POINTER_BITS, Size:?TREE_SIZE_BITS, Rest/binary>> ->
        <<NumMbb:16, BinMbb/binary>> = Rest,
        MbbOrig = [M || <<M:64/native-float>> <= BinMbb],
        Reduce = nil,
        Key = nil,
        % The root node pointer doesn't have a key
        Key = nil,
        #kp_node{
            key = Key,
            childpointer = Pointer,
            treesize = Size,
            reduce = Reduce,
            mbb_orig = MbbOrig
        }
    end,
    Vt#vtree{root = Root}.


% The spatial index doesn't store the bitmap for the full structure,
% it only stores the partition IDs in the leaf nodes. Those get
% filtered out on query time
view_bitmap(_View) ->
    0.


% There is not reduce context for spatial indexes, hence it's a no-op
start_reduce_context(_Group) ->
    ok.

% There is not reduce context for spatial indexes, hence it's a no-op
end_reduce_context(_Group) ->
    ok.


view_name(_SetViewGroup, _ViewPos) ->
    not_yet_implemented.


% In the mapreduce views, this updates the temporary files that are used
% by the C-based file sorter. The spatial view still use an Erlang code
% path for incremental updates. Hence this function is a bit of a misnomer,
% though it the future, once all the indexing is moved to C it won't be
% anymore.
% This is based on `write_changes/4 is based on an old revision of
% `couch_set_view_updater` (6bbe1cf89b2f6b5c9cf098b81c5ea60d339f8f0a), before
% parts of it were moved to C. `write_changes/4` got split into
% `write_to_tmp_batch_files/3` and `maybe_update_btrees/1` in commit
% e64bba5b58f5a4f555b06e6a1cb9ee8cdb6992f7
% XXX vmx 2013-08-09: Support for cleanup is still missing.
update_tmp_files(WriterAcc, ViewKeyValues, KeysToRemoveByView) ->
    #writer_acc{
       group = Group,
       stats = Stats
    } = WriterAcc,
    % XXX vmx 2013-08-02: Use a real value for IdBtreePurgedKeyCount
    IdBtreePurgedKeyCount = 0,
    ViewKeyValuesToAddKvNodes = lists:map(
        fun({View, AddKeyValues0}) ->
            AddKeyValues = lists:map(fun({{Key0, DocId}, {PartId, Body}}) ->
                % NOTE vmx 2013-08-05: The vtree code expects the key to
                % contain tuples. Think about changing the internal format
                % so less conversion is needed.
                Key = [{Min, Max} || [Min, Max] <- ?JSON_DECODE(Key0)],
                couch_set_view_util:check_primary_value_size(
                    Body, ?MAX_VIEW_SINGLE_VALUE_SIZE, Key, DocId, Group),
                Value = <<PartId:16, (byte_size(Body)):24, Body/binary>>,
                #kv_node{
                    key = Key,
                    docid = DocId,
                    body = Value
                }
            end,
            AddKeyValues0),
            {View, AddKeyValues}
        end,
        ViewKeyValues),
    ViewKeyValuesToRemoveKvNodes = dict:map(
        fun(View, RemoveKeyValues0) ->
            RemoveKeyValues = lists:map(fun(KeyDocId) ->
                % NOTE vmx 2013-08-07: Here we decode an just recently
                %    encoded value. This can be made more efficient.
                {Key0, DocId} = couch_set_view_util:decode_key_docid(KeyDocId),
                Key = [{Min, Max} || [Min, Max] <- Key0],
                #kv_node{
                    key = Key,
                    docid = DocId
                }
            end,
            RemoveKeyValues0),
            RemoveKeyValues
        end,
        KeysToRemoveByView),
    {SetViews, {CleanupKvCount, InsertedKvCount, DeletedKvCount}} =
        lists:mapfoldl(fun({SetView, {_SetView, AddKeyValues}}, {AccC, AccI, AccD}) ->
            #set_view{
                id_num = IdNum,
                indexer = View
            } = SetView,
            KeysToRemove = couch_util:dict_find(
                IdNum, ViewKeyValuesToRemoveKvNodes, []),
            case ?set_cbitmask(Group) of
            0 ->
                CleanupCount = 0,
                Vtree = vtree_delete:delete(View#spatial_view.vtree, KeysToRemove),
                Vtree2 = vtree_insert:insert(Vtree, AddKeyValues);
            _ ->
                % XXX vmx 2013-08-02: Cleanup currently isn't supported
                CleanupCount = 0,
                Vtree = vtree_delete:delete(View#spatial_view.vtree, KeysToRemove),
                Vtree2 = vtree_insert:insert(Vtree, AddKeyValues)
            end,
            NewSetView = SetView#set_view{
                indexer = View#spatial_view{
                    vtree = Vtree2
                }
            },
            {NewSetView,
                {AccC + CleanupCount, AccI + length(AddKeyValues),
                 AccD + length(KeysToRemove)}}
        end,
        {IdBtreePurgedKeyCount, 0, 0},
        lists:zip(Group#set_view_group.views, ViewKeyValuesToAddKvNodes)),

    % The sequence number are not updated, it will be done code that
    % deals with the id-btree.
    Header = Group#set_view_group.index_header,
    NewHeader = Header#set_view_index_header{
        view_states = [get_state(V#set_view.indexer) || V <- SetViews]
    },
    NewGroup = Group#set_view_group{
        views = SetViews,
        index_header = NewHeader
    },
    couch_file:flush(Group#set_view_group.fd),
    WriterAcc#writer_acc{
        group = NewGroup,
        stats = Stats#set_view_updater_stats{
            cleanup_kv_count =
                Stats#set_view_updater_stats.cleanup_kv_count + CleanupKvCount,
            inserted_kvs =
                Stats#set_view_updater_stats.inserted_kvs + InsertedKvCount,
            deleted_kvs =
                Stats#set_view_updater_stats.deleted_kvs + DeletedKvCount
        }
    }.


-spec update_index(#btree{},
                   string(),
                   non_neg_integer(),
                   set_view_btree_purge_fun() | 'nil',
                   term()) ->
                          {'ok', term(), #btree{},
                           non_neg_integer(), non_neg_integer()}.
update_index(_Bt, _FilePath, _BufferSize, _PurgeFun, _PurgeAcc) ->
    not_yet_implemented.


-spec design_doc_to_set_view_group(binary(), #doc{}) -> #set_view_group{}.
design_doc_to_set_view_group(SetName, #doc{id = Id, body = {Fields}}) ->
    {DesignOptions} = couch_util:get_value(<<"options">>, Fields, {[]}),
    {RawViews} = couch_util:get_value(<<"spatial">>, Fields, {[]}),
    % add the views to a dictionary object, with the map source as the key
    DictBySrc =
    lists:foldl(
        fun({Name, MapSrc}, DictBySrcAcc) ->
            View =
            case dict:find(MapSrc, DictBySrcAcc) of
                {ok, View0} -> View0;
                error -> #set_view{def = MapSrc, indexer = #spatial_view{}}
            end,
            MapNames = [Name | View#set_view.indexer#spatial_view.map_names],
            View2 = View#set_view{indexer = #spatial_view{map_names = MapNames}},
            dict:store(MapSrc, View2, DictBySrcAcc)
        end, dict:new(), RawViews),
    % number the views
    {SetViews, _N} = lists:mapfoldl(
        fun({_Src, SetView}, N) ->
            {SetView#set_view{id_num = N}, N + 1}
        end,
        0, lists:sort(dict:to_list(DictBySrc))),
    SetViewGroup = #set_view_group{
        set_name = SetName,
        name = Id,
        views = SetViews,
        design_options = DesignOptions,
        mod = ?MODULE,
        extension = index_extension()
    },
    couch_set_view_util:set_view_sig(SetViewGroup).


-spec index_extension() -> string().
index_extension() ->
    ".spatial".


-spec view_group_data_size(#btree{}, [#set_view{}]) -> non_neg_integer().
view_group_data_size(_IdBtree, _Views) ->
    not_yet_implemented.


reset_view(View) ->
    View#spatial_view{vtree = nil}.


setup_views(Fd, _BtreeOptions, _Group, ViewStates, Views) ->
    KvChunkThreshold = couch_config:get(
        "spatial_views", "vtree_kv_node_threshold", "2000"),
    KpChunkThreshold = couch_config:get(
        "spatial_views", "vtree_kp_node_threshold", "2000"),
    MinFillRate = couch_config:get(
        "spatial_views", "vtree_min_fill_rate", "0.4"),

    lists:zipwith(fun(VTState, SetView) ->
        Less = fun(A, B) -> A < B end,
        Root = case VTState of
        nil ->
            nil;
        <<Pointer:?POINTER_BITS, Size:?TREE_SIZE_BITS, Rest/binary>> ->
            {Key, Reduce, MbbOrig} = binary_to_term(Rest),
            #kp_node{
                key = Key,
                childpointer = Pointer,
                treesize = Size,
                reduce = Reduce,
                mbb_orig = MbbOrig
            }
        end,
        Vtree = #vtree{
            root = Root,
            min_fill_rate = list_to_float(MinFillRate),
            kp_chunk_threshold = list_to_integer(KpChunkThreshold),
            kv_chunk_threshold = list_to_integer(KvChunkThreshold),
            less = Less,
            fd = Fd
        },
        Indexer = SetView#set_view.indexer,
        SetView#set_view{indexer = Indexer#spatial_view{vtree = Vtree}}
    end,
    ViewStates, Views).


% The vtree currently doesn't support purges, hence this is a no-op
clean_views(_Instruction, _PurgeFun, SetViews, _Count, _Acc) ->
    {0, SetViews}.


compact_view(_Fd, _SetView, _EmptySetView, _FilterFun, _BeforeKVWriteFun, _Acc0) ->
    not_yet_implemented.


-spec get_row_count(#set_view{}) -> non_neg_integer().
get_row_count(_SetView) ->
    % XXX vmx 2013-07-04: Implement it properly with reduces. For now
    %     just always return 0 and hope it doesn't brak anything.
    0.


apply_log(_Group, _ViewLogFiles, _TmpDir) ->
    not_yet_implemented.


make_wrapper_fun(Fun, Filter) ->
    % TODO vmx 2013-07-22: Support de-duplication
    case Filter of
    false ->
        fun(Node, Acc) ->
            <<PartId:16, _BodySize:24, Body/binary>> = Node#kv_node.body,
            fold_fun(Fun, {Node#kv_node{body = Body}, PartId}, Acc)
        end;
    {true, _, IncludeBitmask} ->
        fun(Node, Acc) ->
            <<PartId:16, _BodySize:24, Body/binary>> = Node#kv_node.body,
            case (1 bsl PartId) band IncludeBitmask of
            0 ->
                {ok, Acc};
            _ ->
                fold_fun(Fun, {Node#kv_node{body = Body}, PartId}, Acc)
           end
        end
    end.


fold_fun(_Fun, nil, Acc) ->
    {ok, Acc};
fold_fun(Fun, {Node, PartId}, Acc) ->
    #kv_node{
        key = Key0,
        docid = DocId,
        body = Body
    } = Node,
    % NOTE vmx 2013-07-11: The key needs to be able to be encoded as JSON.
    %     Think about how to encode the MBB so less conversion is needed.
    Key = [[Min, Max] || {Min, Max} <- Key0],
    case Fun({{Key, DocId}, {PartId, Body}}, Acc) of
    {ok, Acc2} ->
        {ok, Acc2};
    {stop, Acc2} ->
        {stop, Acc2}
    end.


fold(View, WrapperFun, Acc, Options) ->
    Vt = View#spatial_view.vtree,
    Range = couch_util:get_value(range, Options, []),
    Result = case Range of
        [] ->
            vtree_search:all(Vt, WrapperFun, Acc);
        _ ->
            vtree_search:search(Vt, [Range], WrapperFun, Acc)
    end,
    {ok, nil, Result}.



% The following functions have their counterpart in couch_set_view

get_spatial_view(SetName, DDoc, ViewName, Req) ->
    #set_view_group_req{wanted_partitions = WantedPartitions} = Req,
    try
        {ok, Group0} = couch_set_view:get_group(
            spatial_view, SetName, DDoc, Req),
        {Group, Unindexed} = couch_set_view:modify_bitmasks(
            Group0, WantedPartitions),
        case get_spatial_view0(ViewName, Group#set_view_group.views) of
        {ok, View} ->
            {ok, View, Group, Unindexed};
        Else ->
            Else
        end
    catch
    throw:{error, empty_group} ->
        {not_found, missing_named_view}
    end.

get_spatial_view0(_Name, []) ->
    {not_found, missing_named_view};
get_spatial_view0(Name, [#set_view{} = View|Rest]) ->
    MapNames = (View#set_view.indexer)#spatial_view.map_names,
    case lists:member(Name, MapNames) of
        true -> {ok, View};
        false -> get_spatial_view0(Name, Rest)
    end.


% Processes the query paramters to extract the significant values when
% traversing the vtree
-spec make_key_options(#spatial_query_args{}) -> [{atom(), term()}].
make_key_options(ViewQueryArgs) ->
    [{range, ViewQueryArgs#spatial_query_args.range}].


-spec should_filter(#spatial_query_args{}) -> boolean().
should_filter(ViewQueryArgs) ->
    ViewQueryArgs#spatial_query_args.filter.


stats_ets(prod) ->
    ?SPATIAL_VIEW_STATS_ETS_PROD;
stats_ets(dev) ->
    ?SPATIAL_VIEW_STATS_ETS_DEV.

server_name(prod) ->
    ?SPATIAL_VIEW_SERVER_NAME_PROD;
server_name(dev) ->
    ?SPATIAL_VIEW_SERVER_NAME_DEV.

sig_to_pid_ets(prod) ->
    ?SPATIAL_VIEW_SIG_TO_PID_ETS_PROD;
sig_to_pid_ets(dev) ->
    ?SPATIAL_VIEW_SIG_TO_PID_ETS_DEV.

name_to_sig_ets(prod) ->
    ?SPATIAL_VIEW_NAME_TO_SIG_ETS_PROD;
name_to_sig_ets(dev) ->
    ?SPATIAL_VIEW_NAME_TO_SIG_ETS_DEV.

pid_to_sig_ets(prod) ->
    ?SPATIAL_VIEW_PID_TO_SIG_ETS_PROD;
pid_to_sig_ets(dev) ->
    ?SPATIAL_VIEW_PID_TO_SIG_ETS_DEV.
