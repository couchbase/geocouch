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
         reset_view/1, setup_views/5]).
% For the utils
-export([clean_views/5]).
% For the compactor
-export([compact_view/6, apply_log/3]).
% For the main module
-export([get_row_count/1, make_wrapper_fun/2, fold/4]).
% For the couch_set_view like calls
-export([get_spatial_view/4]).


-include("couch_db.hrl").
-include_lib("couch_set_view/include/couch_set_view.hrl").
-include_lib("vtree/include/vtree.hrl").

% Same as in couch_btree.erl
-define(POINTER_BITS,   48).
-define(TREE_SIZE_BITS, 48).
-define(KEY_BITS,       12).
-define(MAX_KEY_SIZE,   ((1 bsl ?KEY_BITS) - 1)).

% Same as the compactor uses for the ID-btree
-define(SORTED_CHUNK_SIZE, 1024 * 1024).


write_kvs(Group, TmpFiles0, ViewKVs) ->
    lists:foldl(
        fun({#set_view{id_num = Id}, KvList0}, {AccCount, TmpFiles}) ->
            TmpFileInfo0 = dict:fetch(Id, TmpFiles),
            {KvList, Mbb} = calc_mbb(KvList0),
            TmpFileInfo = TmpFileInfo0#tmp_file_info{extra = Mbb},

            KvBins = convert_primary_index_kvs_to_binary(KvList, Group, []),
            ViewRecords = lists:foldr(
                fun({KeyBin, ValBin}, Acc) ->
                    KvBin = [<<(byte_size(KeyBin)):16>>, KeyBin, ValBin],
                    [[<<(iolist_size(KvBin)):32>>, KvBin] | Acc]
                end,
                [], KvBins),
            ok = file:write(TmpFileInfo#tmp_file_info.fd, ViewRecords),
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
    V = case Body of
    % TODO vmx 2013-07-01: Support dups properly (they are encoded here
    %     but never really taken into account througout the code
    {dups, Values} ->
        ValueListBinary = lists:foldl(
            fun(V, Acc2) ->
                <<Acc2/binary, (byte_size(V)):24, V/binary>>
            end,
            <<>>, Values),
        <<PartId:16, ValueListBinary/binary>>;
    _ ->
        <<PartId:16, (byte_size(Body)):24, Body/binary>>
    end,
    KeyBin = encode_key_docid(Key, DocId),
    case byte_size(KeyBin) > ?MAX_KEY_SIZE of
    true ->
        #set_view_group{
            set_name = SetName,
            name = DDocId,
            type = Type
        } = Group,
        KeyPrefix = lists:sublist(unicode:characters_to_list(Key), 100),
        Error = iolist_to_binary(
            io_lib:format("key emitted for document `~s` is too long: ~s...",
                [DocId, KeyPrefix])),
        ?LOG_MAPREDUCE_ERROR("Bucket `~s`, ~s group `~s`, ~s",
            [SetName, Type, DDocId, Error]),
        throw({error, Error});
    false ->
        convert_primary_index_kvs_to_binary(Rest, Group, [{KeyBin, V} | Acc])
    end.

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

    #tmp_file_info{name = ViewFile} = dict:fetch(Id, TmpFiles),
    {ok, NewVtRoot} = vtree_copy:from_sorted_file(
        Vt, ViewFile, GroupFd,
        fun couch_set_view_updater:file_sorter_initial_build_format_fun/1),
    ok = file2:delete(ViewFile),
    SetView#set_view{
        indexer = View#spatial_view{
            vtree = Vt#vtree{root = NewVtRoot}
        }
    }.

% Return the state of a view (which will be stored in the header)
get_state(View) ->
    get_vtree_state(View#spatial_view.vtree).

% XXX vmx 2013-01-02: Should perhaps be moved to the vtree itself (and then
%    renamed to `get_state`
get_vtree_state(Vt) ->
    #vtree{
        root = #kp_node{
            key = Key,
            childpointer = Pointer,
            treesize = Size,
            reduce = Reduce,
            mbb_orig = MbbOrig
        }
    } = Vt,
    % TODO vmx 2013-06-27: Proper binary encoding would make sense,
    %     but this is good for now.
    Rest = term_to_binary({Key, Reduce, MbbOrig}),
    <<Pointer:?POINTER_BITS, Size:?TREE_SIZE_BITS, Rest/binary>>.


view_bitmap(_View) ->
    not_yet_implemented.


% There is not reduce context for spatial indexes, hence it's a no-op
start_reduce_context(_Group) ->
    ok.

% There is not reduce context for spatial indexes, hence it's a no-op
end_reduce_context(_Group) ->
    ok.


view_name(#set_view_group{views = SetViews}, ViewPos) ->
    View = (lists:nth(ViewPos, SetViews))#set_view.indexer,
    case View#mapreduce_view.map_names of
    [] ->
        [{Name, _} | _] = View#mapreduce_view.reduce_funs;
    [Name | _] ->
        ok
    end,
    Name.


% Update the temporary files with the key-values from the indexer. Return
% the updated writer accumulator.
update_tmp_files(_WriterAcc, _ViewKeyValues, _KeysToRemoveByView) ->
    not_yet_implemented.


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
        mod = ?MODULE
    },
    couch_set_view_util:set_view_sig(SetViewGroup).


-spec view_group_data_size(#btree{}, [#set_view{}]) -> non_neg_integer().
view_group_data_size(IdBtree, Views) ->
    lists:foldl(
        fun(SetView, Acc) ->
            Btree = (SetView#set_view.indexer)#mapreduce_view.btree,
            Acc + couch_btree:size(Btree)
        end,
        couch_btree:size(IdBtree),
        Views).


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


clean_views(_Instruction, _PurgeFun, _SetViews_, _Count, _Acc) ->
    not_yet_implemented.


compact_view(_Fd, _SetView, _EmptySetView, _FilterFun, _BeforeKVWriteFun, _Acc0) ->
    not_yet_implemented.


-spec get_row_count(#set_view{}) -> non_neg_integer().
get_row_count(_SetView) ->
    not_yet_implemented.


apply_log(_Group, _ViewLogFiles, _TmpDir) ->
    not_yet_implemented.


make_wrapper_fun(Fun, _Filter) ->
    % XXX vmx 2013-01-15: From now don't care about the partition filter or
    %     de-duplication
    fun(Node, Acc) ->
            <<_PartId:16, _BodySize:24, Body/binary>> = Node#kv_node.body,
            fold_fun(Fun, Node#kv_node{body = Body}, Acc)
    end.


fold_fun(_Fun, [], Acc) ->
    {ok, Acc};
fold_fun(Fun, Node, Acc) ->
    #kv_node{
        key = Key,
        docid = DocId,
        body = Body
    } = Node,
    case Fun({{Key, DocId}, Body}, Acc) of
    {ok, Acc2} ->
        {ok, Acc2};
    {stop, Acc2} ->
        {stop, Acc2}
    end.


fold(View, WrapperFun, Acc, _Options) ->
    % XXX vmx 2013-07-01: There's no option parsing yet, all results
    %     without any filtering will be returned
    Vt = View#spatial_view.vtree,
    Result = vtree_search:all(Vt, WrapperFun, Acc),
    {ok, nil, Result}.



% The following functions have their counterpart in couch_set_view

get_spatial_view(SetName, DDoc, ViewName, Req) ->
    #set_view_group_req{wanted_partitions = _WantedPartitions} = Req,
    try
        {ok, Group0} = couch_set_view:get_group(
            spatial_view, SetName, DDoc, Req),
        % XXX TODO vmx 2013-05-27: bitmask manipulation
        %{Group, Unindexed} = modify_bitmasks(Group0, WantedPartitions),
        {Group, Unindexed} = {Group0, []},
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
