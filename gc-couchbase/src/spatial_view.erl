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

% Needed for `dict()` type on Erlang >= 17.0
-compile(nowarn_deprecated_type).

% For the updater
-export([write_kvs/3, finish_build/3, get_state/1, view_name/2,
         update_spatial/3, view_bitmap/1]).
-export([encode_key_docid/2]).
-export([convert_primary_index_kvs_to_binary/3]).
-export([convert_back_index_kvs_to_binary/2]).
-export([view_insert_doc_query_results/6]).
% For the group
-export([design_doc_to_set_view_group/2, view_group_data_size/2,
         reset_view/1, setup_views/5, set_state/2]).
% For the utils
-export([clean_views/2, view_info/1]).
% For the main module
-export([get_row_count/1, make_wrapper_fun/2, fold/4, index_extension/0,
         make_key_options/1, should_filter/1, query_args_view_name/1]).
-export([stats_ets/1, server_name/1, sig_to_pid_ets/1, name_to_sig_ets/1,
         pid_to_sig_ets/1]).
% For the couch_set_view like calls
-export([get_spatial_view/4]).
% For couch_db
-export([validate_ddoc/1]).


-include_lib("couch_spatial.hrl").
-include("couch_db.hrl").
-include_lib("couch_set_view/include/couch_set_view.hrl").
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
-define(VIEW_GEOMETRY_BITS,         24).
-define(MAX_VIEW_SINGLE_VALUE_SIZE, ((1 bsl ?VIEW_SINGLE_VALUE_BITS) - 1)).
-define(MAX_VIEW_ALL_VALUES_SIZE,   ((1 bsl ?VIEW_ALL_VALUES_BITS) - 1)).
-define(MAX_VIEW_GEOMETRY_SIZE,     ((1 bsl ?VIEW_GEOMETRY_BITS) - 1)).


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
        fun({#set_view{id_num = Id}, KvList}, {AccCount, TmpFiles}) ->
            TmpFileInfo0 = dict:fetch(Id, TmpFiles),
            Mbb = calc_mbb(KvList),
            Mbb2 = merge_mbbs(Mbb, TmpFileInfo0#set_view_tmp_file_info.extra),
            TmpFileInfo = TmpFileInfo0#set_view_tmp_file_info{extra = Mbb2},

            KvBins = convert_primary_index_kvs_to_binary(KvList, Group, []),
            ViewRecords = lists:foldr(
                fun({KeyBin, ValBin}, Acc) ->
                    KvBin = [<<(byte_size(KeyBin)):16>>, KeyBin, ValBin],
                    [[<<(iolist_size(KvBin)):32/native>>, KvBin] | Acc]
                end,
                [], KvBins),
            ok = file:write(
                TmpFileInfo#set_view_tmp_file_info.fd, ViewRecords),
            TmpFiles2 = dict:store(Id, TmpFileInfo, TmpFiles),
            {AccCount + length(KvBins), TmpFiles2}
        end,
        {0, TmpFiles0}, ViewKVs).


% Calculates the enclosing multidimensional bounding box
-spec calc_mbb([{{[[number()]], binary()}, binary()}]) -> [[number()]] | nil.
calc_mbb(KvList) ->
    Less = fun(A, B) -> A < B end,
    lists:foldl(fun
        ({{Key, _DocId}, _PartIdValue}, nil) ->
            Key;
        ({{Key, _DocId}, _PartIdValue}, Mbb) ->
            lists:map(fun({[KeyMin, KeyMax], [MbbMin, MbbMax]}) ->
                [vtree_util:min({KeyMin, MbbMin}, Less),
                 vtree_util:max({KeyMax, MbbMax}, Less)]
            end, lists:zip(Key, Mbb))
    end, nil, KvList).


-spec merge_mbbs([[number()]] | nil, [[number()]] | nil) -> [[number()]] | nil.
merge_mbbs(nil, MbbB) ->
    MbbB;
merge_mbbs(MbbA, nil) ->
    MbbA;
merge_mbbs(MbbA, MbbB) ->
    Less = fun(A, B) -> A < B end,
    lists:map(fun({[MinA, MaxA], [MinB, MaxB]}) ->
        [vtree_util:min({MinA, MinB}, Less),
         vtree_util:max({MaxA, MaxB}, Less)]
    end, lists:zip(MbbA, MbbB)).


% Convert the key from a list of 2-tuples to a list of raw doubles
% NOTE vmx 2014-12-12: The first case could be more efficient, but for now
% it's good enough. Once the enclosing MBB for the initial index build is
% no longer needed, this case can go away as `calc_mbb` will be no longer
% needed in `write_kvs/3`.
-spec convert_primary_index_kvs_to_binary(
        [{{binary(), binary()},
          {partition_id(),
           {dups, [{binary(), binary()}]} | [{binary(), binary()}]}}],
        #set_view_group{}, [{binary(), binary()}]) -> [{binary(), binary()}].
convert_primary_index_kvs_to_binary([], _Group, Acc) ->
    lists:reverse(Acc);
convert_primary_index_kvs_to_binary([H | Rest], Group, Acc)->
    {{Key, DocId}, {PartId, Value}} = H,
    KeyBin = encode_key_docid(Key, DocId),
    couch_set_view_util:check_primary_key_size(
        KeyBin, ?MAX_KEY_SIZE, Key, DocId, Group),
    ValueBin = convert_values_to_binary(Value, Key, DocId, Group),
    ValueBin2 = <<PartId:16, ValueBin/binary>>,
    couch_set_view_util:check_primary_value_size(
        ValueBin2, ?MAX_VIEW_ALL_VALUES_SIZE, Key, DocId, Group),
    convert_primary_index_kvs_to_binary(
        Rest, Group, [{KeyBin, ValueBin2} | Acc]).


-spec convert_body_geometry_to_binary({binary(), binary()}, [[number()]], binary(),
                                      any()) -> binary().
convert_body_geometry_to_binary({Body, Geom}, Key, DocId, Group) ->
    couch_set_view_util:check_primary_value_size(
        Body, ?MAX_VIEW_SINGLE_VALUE_SIZE, Key, DocId, Group),
    check_primary_geometry_size(
        Geom, ?MAX_VIEW_GEOMETRY_SIZE, Key, DocId, Group),
    <<(byte_size(Body)):?VIEW_SINGLE_VALUE_BITS,
      (byte_size(Geom)):?VIEW_GEOMETRY_BITS,
      Body/binary, Geom/binary>>.


-spec convert_values_to_binary(
        {dups, [{binary(), binary()}]} | {binary(), binary()},
        [[number()]], binary(), #set_view_group{}) -> binary().
convert_values_to_binary({dups, Values}, Key, DocId, Group) ->
    lists:foldl(fun(V, Acc2) ->
        Bin = convert_body_geometry_to_binary(V, Key, DocId, Group),
        <<Acc2/binary, Bin/binary>>
    end, <<>>, Values);
convert_values_to_binary(Value, Key, DocId, Group) ->
    convert_body_geometry_to_binary(Value, Key, DocId, Group).



-spec encode_key_docid(binary() | [[number()]], binary()) -> binary().
encode_key_docid(BinMbb, DocId) when is_binary(BinMbb) ->
    % A double has 8 bytes
    NumDobules = byte_size(BinMbb) div 8,
    <<NumDobules:16, BinMbb/binary, DocId/binary>>;
encode_key_docid(Key, DocId) ->
    BinKey = encode_key(Key),
    % Prefix the key with the number of doubles
    <<(length(Key) * 2):16, BinKey/binary, DocId/binary>>.

-spec encode_key([[number()]]) -> binary().
encode_key(Key) ->
    << <<Min:64/native-float, Max:64/native-float>> || [Min, Max] <- Key>>.


-spec decode_key_docid(binary()) -> {[number()], binary()}.
decode_key_docid(<<NumDoubles:16, Rest/binary>>) ->
    % A double has 8 bytes
    KeySize = NumDoubles * 8,
    <<BinKey:KeySize/binary, DocId/binary>> = Rest,
    Key = vtree_io:decode_mbb(BinKey),
    {Key, DocId}.


% Build the tree out of the sorted files
-spec finish_build(#set_view_group{}, dict:dict(), string()) ->
                          {#set_view_group{}, pid()}.
finish_build(Group, TmpFiles, TmpDir) ->
    #set_view_group{
        sig = Sig,
        id_btree = IdBtree,
        views = SetViews
    } = Group,
    case os:find_executable("couch_view_index_builder") of
    false ->
        Cmd = nil,
        throw(<<"couch_view_index_builder command not found">>);
    Cmd ->
        ok
    end,
    Options = [exit_status, use_stdio, stderr_to_stdout, {line, 4096}, binary],
    Port = open_port({spawn_executable, Cmd}, Options),

    % The external process that builds up the tree needs to know the enclosing
    % bounding box of the data. The easiest way is to temporarily store it as
    % a vtree that will later on be replaced with the real data.
    SetViews2 = lists:map(fun(SetView) ->
        #set_view{
            id_num = Id,
            indexer = View
        } = SetView,
        #set_view_tmp_file_info{
            extra = Mbb
        } = dict:fetch(Id, TmpFiles),
        Key = case Mbb of
        nil ->
            [];
        _ ->
            [list_to_tuple(M) || M <- Mbb]
        end,
        SetView#set_view{
            indexer = View#spatial_view{
                vtree = #vtree{
                    root = #kp_node{
                        key = Key
                    }
                }
            }
        }
    end, SetViews),

    Group2 = Group#set_view_group{views = SetViews2},
    couch_set_view_util:send_group_info(Group2, Port),
    true = port_command(Port, [TmpDir, $\n]),
    #set_view_tmp_file_info{name = IdFile} = dict:fetch(ids_index, TmpFiles),
    DestPath = couch_set_view_util:new_sort_file_path(TmpDir, updater),
    true = port_command(Port, [DestPath, $\n, IdFile, $\n]),
    lists:foreach(
        fun(#set_view{id_num = Id}) ->
            #set_view_tmp_file_info{
                name = ViewFile
            } = dict:fetch(Id, TmpFiles),
            true = port_command(Port, [ViewFile, $\n])
        end,
        SetViews2),

    try
        couch_set_view_updater_helper:index_builder_wait_loop(Port, Group2, [])
    after
        catch port_close(Port)
    end,

    {ok, NewFd} = couch_file:open(DestPath),
    unlink(NewFd),
    {ok, HeaderBin, NewHeaderPos} = couch_file:read_header_bin(NewFd),
    HeaderSig = couch_set_view_util:header_bin_sig(HeaderBin),
    case HeaderSig == Sig of
    true ->
        ok;
    false ->
        couch_file:close(NewFd),
        ok = file2:delete(DestPath),
        throw({error, <<"Corrupted initial build destination file.\n">>})
    end,
    NewHeader = couch_set_view_util:header_bin_to_term(HeaderBin),
    #set_view_index_header{
        id_btree_state = NewIdBtreeRoot,
        view_states = NewViewRoots
    } = NewHeader,
    NewIdBtree = couch_btree:set_state(IdBtree#btree{fd = NewFd}, NewIdBtreeRoot),
    NewSetViews = lists:zipwith(
        % XXX vmx 2014-02-04: Refactor this function out into one called
        %     `update_states`
        fun(#set_view{indexer = View} = V, NewRoot) ->
            #spatial_view{vtree = Vt} = View,
            NewVt = set_vtree_state(Vt#vtree{fd = NewFd}, NewRoot),
            NewView = View#spatial_view{vtree = NewVt},
            V#set_view{indexer = NewView}
        end,
        SetViews2, NewViewRoots),

    NewGroup = Group#set_view_group{
        id_btree = NewIdBtree,
        views = NewSetViews,
        index_header = NewHeader,
        header_pos = NewHeaderPos
    },
    {NewGroup, NewFd}.


% In order to build the spatial index bottom-up we need to have supply
% the enclosing bounding box
view_info(#spatial_view{vtree = Vt}) ->
    case Vt#vtree.root of
    nil ->
        [<<"0">>, $\n];
    #kp_node{key = Mbb0, childpointer = Pointer} ->
        Mbb = case Mbb0 of
        nil ->
            Children = vtree_io:read_node(Vt#vtree.fd, Pointer),
            vtree_util:nodes_mbb(Children, Vt#vtree.less);
        _ ->
            Mbb0
       end,
       MbbEncoded = vtree_io:encode_mbb(Mbb),
       Dimension = list_to_binary(integer_to_list(length(Mbb))),
       [Dimension, $\n, MbbEncoded]
    end.

% Return the state of a view (which will be stored in the header)
get_state(View) ->
    get_vtree_state(View#spatial_view.vtree).

% XXX vmx 2013-01-02: Should perhaps be moved to the vtree itself (and then
%    renamed to `get_state`
get_vtree_state(Vt) ->
    case Vt#vtree.root of
    nil ->
        nil;
    #kp_node{} = Root ->
        #kp_node{
            childpointer = Pointer,
            treesize = Size,
            mbb_orig = MbbOrig
        } = Root,
        BinMbbOrig = vtree_io:encode_mbb(MbbOrig),
        % The length is the number of doubles, not the dimension, hence use
        % the length two times the length of MbbOrig
        NumMbbOrig = length(MbbOrig) * 2,
        Rest = <<NumMbbOrig:16, BinMbbOrig/binary>>,
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
    <<Pointer:?POINTER_BITS, Size:?TREE_SIZE_BITS, Rest/binary>> ->
        <<_NumMbb:16, BinMbb/binary>> = Rest,
        MbbOrig = vtree_io:decode_mbb(BinMbb),
        #kp_node{
            % The root node pointer doesn't have a key
            key = nil,
            childpointer = Pointer,
            treesize = Size,
            mbb_orig = MbbOrig
        }
    end,
    Vt#vtree{root = Root}.


% The spatial index doesn't store the bitmap for the full structure,
% it only stores the partition IDs in the leaf nodes. Those get
% filtered out on query time
view_bitmap(_View) ->
    0.

view_name(#set_view_group{views = SetViews}, ViewPos) ->
    View = (lists:nth(ViewPos, SetViews))#set_view.indexer,
    case View#spatial_view.map_names of
    [Name | _] ->
        Name
    end.


% This function does what the C-based native updater does for mapreduce views
-spec update_spatial([#set_view{}], [string()], non_neg_integer()) ->
                            [#set_view{}].
update_spatial(Views, [], _) ->
    Views;
update_spatial(Views, LogFiles, MaxBatchSize) ->
    lists:zipwith(fun(View, LogFile) ->
        {ok, Fd} = file2:open(LogFile, [read, raw, binary, read_ahead]),
        try
            process_log_file(View, Fd, MaxBatchSize, 0, [])
        after
            ok = file:close(Fd)
        end
    end, Views, LogFiles).


-spec process_log_file(#set_view{}, file:io_device(), non_neg_integer(),
                       non_neg_integer(),
                       [{insert, binary(), binary()} | {remove, binary()}]) ->
                              #set_view{}.
% Don't include the last operation as it made the batch size bigger than the
% maximum batch size
process_log_file(View0, LogFd, MaxBatchSize, BatchSize, [H | Ops])
        when BatchSize > MaxBatchSize ->
    View = flush_writes(View0, lists:reverse(Ops)),
    process_log_file(View, LogFd, MaxBatchSize, 0, [H]);
process_log_file(View, LogFd, MaxBatchSize, BatchSize, Ops) ->
    case file:read(LogFd, 4) of
    {ok, <<Size:32/native>>} ->
        {ok, <<OpCode:8, KeySize:16, Key:KeySize/binary, Value/binary>>} =
            file:read(LogFd, Size),
        OpAtom = couch_set_view_updater_helper:code_to_op(OpCode),
        Op = case OpAtom of
        insert ->
            {OpAtom, Key, Value};
        remove ->
            {OpAtom, Key}
        end,
        process_log_file(View, LogFd, MaxBatchSize, BatchSize + Size - 1 - 2, [Op | Ops]);
    eof ->
        flush_writes(View, lists:reverse(Ops));
    {error, Error} ->
        throw({file_read_error, Error})
    end.

% Insert the data into the view file
-spec flush_writes(#set_view{},
                   [{insert, binary(), binary()} | {remove, binary()}]) ->
                          #set_view{}.
flush_writes(SetView, Ops) ->
    #set_view{
        indexer = #spatial_view{
            vtree = Vt
        } = View
    } = SetView,
    {OpsToAdd, OpsToRemove} = lists:partition(
        fun({insert, _, _}) -> true;
           ({remove, _}) -> false
        end, Ops),
    KvNodesToRemove = lists:map(fun({remove, KeyDocId}) ->
        {Key, DocId} = decode_key_docid(KeyDocId),
        #kv_node{
            key = Key,
            docid = DocId
        }
    end, OpsToRemove),
    KvNodesToAdd = lists:map(fun({insert, KeyDocId, Value}) ->
        {Key, DocId} = decode_key_docid(KeyDocId),
        <<PartId:16, Values/binary>> = Value,
        BodyGeoms = vtree_io:decode_dups(Values),
        {Body, Geom} = case BodyGeoms of
        [{B, G}] ->
           {B, G};
        _ ->
           {BodyDups, GeomDups} = lists:unzip(BodyGeoms),
           {{dups, BodyDups}, {dups, GeomDups}}
        end,
        #kv_node{
            key = Key,
            docid = DocId,
            body = Body,
            partition = PartId,
            geometry = Geom
        }
    end, OpsToAdd),
    Vt2 = vtree_delete:delete(Vt, KvNodesToRemove),
    Vt3 = vtree_insert:insert(Vt2, KvNodesToAdd),
    SetView#set_view{
        indexer = View#spatial_view{
            vtree = Vt3
        }
    }.


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
    set_view_sig(SetViewGroup).


% Make sure the signature changes whenever the definitions of the spatial
% views change. This way a view gets re-generated. Re-sorting the view
% definitions within the JSON object won't change the signature.
-spec set_view_for_sig(#set_view{}) -> [binary()].
set_view_for_sig(SetView) ->
    #set_view{
        def = Def,
        indexer = #spatial_view{
            map_names = MapNames
        }
    } = SetView,
    [MapNames, Def].


-spec set_view_sig(#set_view_group{}) -> #set_view_group{}.
set_view_sig(#set_view_group{views = SetViews} = Group) ->
    % With using an iolist for the signature, it's easier possible to replace
    % it with a C-based implementation in the future. It's also easier to add
    % additional optional things without changing the signature of exisisting
    % views.
    SetViews2 = [set_view_for_sig(SetView) || SetView <- SetViews],
    Sig = couch_util:md5(iolist_to_binary(SetViews2)),
    Group#set_view_group{sig = Sig}.


-spec index_extension() -> string().
index_extension() ->
    ".spatial".


-spec view_group_data_size(#btree{}, [#set_view{}]) -> non_neg_integer().
view_group_data_size(IdBtree, Views) ->
    lists:foldl(
        fun(SetView, Acc) ->
            Root = ((SetView#set_view.indexer)#spatial_view.vtree)#vtree.root,
            Acc + vtree_io:treesize(Root)
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
            <<_NumMbb:16, BinMbb/binary>> = Rest,
            MbbOrig = vtree_io:decode_mbb(BinMbb),
            #kp_node{
                % The root node pointer doesn't have a key
                key = nil,
                childpointer = Pointer,
                treesize = Size,
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
% XXX vmx 2014-07-30: This is needed to have correct results after a rebalance
clean_views(SetViews0, CleanupParts) ->
    SetViews = lists:map(fun(SetView) ->
        View = SetView#set_view.indexer,
        Vt0 = View#spatial_view.vtree,
        % NOTE vmx 2014-08-05: currently the tree manipulation code expects
        % KV-nodes as input
        CleanupNodes = [#kv_node{partition = PartId} ||
            PartId <- CleanupParts],
        Vt = vtree_cleanup:cleanup(Vt0, CleanupNodes),
        SetView#set_view{
            indexer = View#spatial_view{
                vtree = Vt
            }
        }
    end, SetViews0),
    {0, SetViews}.


-spec get_row_count(#set_view{}) -> non_neg_integer().
get_row_count(_SetView) ->
    % XXX vmx 2013-07-04: Implement it properly with reduces. For now
    %     just always return 0 and hope it doesn't brak anything.
    0.


make_wrapper_fun(Fun, Filter) ->
    % TODO vmx 2013-07-22: Support de-duplication
    case Filter of
    false ->
        fun(Node, Acc) ->
            ExpandedNode = expand_dups(Node),
            fold_fun(Fun, ExpandedNode, Acc)
        end;
    {true, _, IncludeBitmask} ->
        fun(Node, Acc) ->
            case (1 bsl Node#kv_node.partition) band IncludeBitmask of
            0 ->
                {ok, Acc};
            _ ->
                ExpandedNode = expand_dups(Node),
                fold_fun(Fun, ExpandedNode, Acc)
           end
        end
    end.


-spec fold_fun(
        fun(({{[[number()]], binary()}, {partition_id(), binary(), json()}},
             any()) -> {ok | stop, any()}),
        [#kv_node{}], any()) -> {ok | stop, any()}.
fold_fun(_Fun, [], Acc) ->
    {ok, Acc};
fold_fun(Fun, [Node | Rest], Acc) ->
    #kv_node{
        key = Key0,
        docid = DocId,
        body = Body,
        geometry = Geom,
        partition = PartId
    } = Node,
    case Geom of
    <<>> ->
        JsonGeom = nil;
    _ ->
        {ok, JsonGeom} = wkb_reader:wkb_to_geojson(Geom)
    end,
    % NOTE vmx 2013-07-11: The key needs to be able to be encoded as JSON.
    %     Think about how to encode the MBB so less conversion is needed.
    Key = [[Min, Max] || {Min, Max} <- Key0],
    case Fun({{Key, DocId}, {PartId, Body, JsonGeom}}, Acc) of
    {ok, Acc2} ->
        fold_fun(Fun, Rest, Acc2);
    {stop, Acc2} ->
        {stop, Acc2}
    end.


fold(View, WrapperFun, Acc, Options) ->
    #spatial_view{
       vtree = #vtree{
           root = Root
       } = Vt
    } = View,
    case Root of
    nil ->
        {ok, nil, Acc};
     % Use the original MBB for comparison as the key is not necessarily
     % set. The original MBB is good enough for this check as it will have
     % the same dimesionality.
    #kp_node{mbb_orig = MbbOrig} ->
        Range = couch_util:get_value(range, Options, []),
        Result = case Range of
        [] ->
            vtree_search:all(Vt, WrapperFun, Acc);
        _ when length(Range) =/= length(MbbOrig) ->
            throw(list_to_binary(io_lib:format(
                "The query range must have the same dimensionality as "
                "the index. Your range was `~10000p`, but the index has a "
                "dimensionality of `~p`.", [Range, length(MbbOrig)])));
        _ ->
            vtree_search:search(Vt, [Range], WrapperFun, Acc)
        end,
        {ok, nil, Result}
    end.


-spec expand_dups(#kv_node{}) -> [#kv_node{}].
expand_dups(#kv_node{body = {dups, BodyDups}, geometry = {dups, GeomDups}}
        = Node) ->
    [Node#kv_node{body = B, geometry = G} || {B, G}
        <- lists:zip(BodyDups, GeomDups)];
expand_dups(Node) ->
    [Node].


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


% The key might contain a geometry or points instead of ranges for certain
% dimensions.
% In case of a geometry, calculate the bounding box and return it.
% In case of a point instead of a range, create a collapsed range
-spec maybe_process_key(binary()) -> {[[number()]], geom() | nil}.
maybe_process_key(Key0) ->
    Key = ?JSON_DECODE(Key0),
    case Key of
    % Legacy API when only a geometry is given
    {Geom} ->
        process_geometry(Geom);
    [_ | _] ->
        case Key of
        [{Geom} | T] ->
            {Bbox, Geom2} = process_geometry(Geom),
            Key2 = Bbox ++ T;
        _ ->
            Key2 = Key,
            Geom2 = <<>>
        end,
        Key3 = lists:map(
            fun([]) ->
                throw({emit_key, <<"A range cannot be an empty array.">>});
            ([_SingleElementList]) ->
                throw({emit_key, <<"A range cannot be single element "
                                   "array.">>});
            ([Min, Max]) when not (is_number(Min) andalso is_number(Max)) ->
                throw({emit_key, <<"Ranges must be numbers.">>});
            ([Min, Max]) when Min > Max ->
                throw({emit_key, <<"The minimum of a range must be smaller "
                                   "than the maximum.">>});
            ([Min, Max]) ->
                [Min, Max];
            (SingleValue) when is_tuple(SingleValue)->
                throw({emit_key, <<"A geometry is only allowed as the first "
                                   "element in the array.">>});
            (SingleValue) when not is_number(SingleValue)->
                throw({emit_key, <<"The values of the key must be numbers or "
                                   "a GeoJSON geometry.">>});
            (SingleValue) ->
                [SingleValue, SingleValue]
            end,
        Key2),
        {Key3, Geom2};
    _ ->
        throw({emit_key, <<"The key must be an array of numbers which might"
                           "have a GeoJSON geometry as first element.">>})
    end.


% Returns an Erlang encoded geometry and the corresponding bounding box
process_geometry(Geo) ->
    Bbox = try
        Type = binary_to_atom(proplists:get_value(<<"type">>, Geo), utf8),
        case Type of
        'GeometryCollection' ->
            Geometries = proplists:get_value(<<"geometries">>, Geo),
            lists:foldl(fun({Geometry}, CurBbox) ->
                Type2 = binary_to_atom(
                    proplists:get_value(<<"type">>, Geometry), utf8),
                Coords = proplists:get_value(<<"coordinates">>, Geometry),
                case proplists:get_value(<<"bbox">>, Geo) of
                undefined ->
                    extract_bbox(Type2, Coords, CurBbox);
                [W, S, E, N] ->
                    [[W, E], [S, N]]
                end
            end, nil, Geometries);
        _ ->
            Coords = proplists:get_value(<<"coordinates">>, Geo),
            case proplists:get_value(<<"bbox">>, Geo) of
            undefined ->
                extract_bbox(Type, Coords);
            [W, S, E, N] ->
                [[W, E], [S, N]]
            end
        end
    catch _:badarg ->
        throw({emit_key, <<"The supplied geometry must be valid GeoJSON.">>})
    end,
    {ok, Geom} = wkb_writer:geojson_to_wkb({Geo}),
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
        end, InitBbox, Coords);
    InvalidType ->
        throw({emit_key,
            <<"The supplied geometry type `",
              (atom_to_binary(InvalidType, latin1))/binary,
              "` is not a valid GeoJSON. "
              "Valid geometry types are (case sensitive): "
              "Point, LineString, Polygon, MultiPoint, MultiLineString, "
              "MultiLineString">>})
    end.

bbox([], Range) ->
    Range;
bbox([[X, Y]|Rest], nil) ->
    bbox(Rest, [[X, X], [Y, Y]]);
bbox([Coords|Rest], Range) ->
    Range2 = lists:zipwith(
        fun(Coord, [Min, Max]) ->
            [erlang:min(Coord, Min), erlang:max(Coord, Max)]
        end, Coords, Range),
    bbox(Rest, Range2).


-spec check_primary_geometry_size(binary(), pos_integer(), [number()],
        binary(), #set_view_group{}) -> ok.
check_primary_geometry_size(Bin, Max, Key, DocId, Group) when byte_size(Bin) > Max ->
    #set_view_group{set_name = SetName, name = DDocId, type = Type} = Group,
    Error = iolist_to_binary(
        io_lib:format("geometry emitted for key `~s`, document `~s`, is too big"
                      " (~p bytes)", [Key, DocId, byte_size(Bin)])),
    ?LOG_MAPREDUCE_ERROR("Bucket `~s`, ~s group `~s`, ~s",
                         [?LOG_USERDATA(SetName), Type, ?LOG_USERDATA(DDocId), ?LOG_USERDATA(Error)]),
    throw({error, Error});
check_primary_geometry_size(_Bin, _Max, _Key, _DocId, _Group) ->
    ok.


-spec convert_back_index_kvs_to_binary(
        [{binary(), {partition_id(), {binary(), {partition_id(), binary()}}}}],
        [{binary(), binary()}]) -> [{binary(), binary()}].
convert_back_index_kvs_to_binary([], Acc)->
    lists:reverse(Acc);
convert_back_index_kvs_to_binary(
        [{DocId, {PartId, ViewIdKeys}} | Rest], Acc) ->
    ViewIdKeysBinary = lists:foldl(
        fun({ViewId, Keys}, Acc2) ->
            KeyListBinary = lists:foldl(
                fun(Key, AccKeys) ->
                    Key2 = encode_key(Key),
                    <<AccKeys/binary, (byte_size(Key2)):16, Key2/binary>>
                end,
                <<>>, Keys),
            NumKeys = length(Keys),
            case NumKeys >= (1 bsl 16) of
            true ->
                ErrorMsg = io_lib:format(
                    "Too many (~p) keys emitted for "
                    "document `~s` (maximum allowed is ~p",
                    [NumKeys, DocId, (1 bsl 16) - 1]),
                throw({error, iolist_to_binary(ErrorMsg)});
            false ->
                ok
            end,
            <<Acc2/binary, ViewId:8, NumKeys:16, KeyListBinary/binary>>
        end,
        <<>>, ViewIdKeys),
    KvBin = {<<PartId:16, DocId/binary>>,
        <<PartId:16, ViewIdKeysBinary/binary>>},
    convert_back_index_kvs_to_binary(Rest, [KvBin | Acc]).


-spec view_insert_doc_query_results(
        binary(), partition_id(), [set_view_key_value()],
        [set_view_key_value()], [set_view_key_value()], [set_view_key()]) ->
                                           {[set_view_key_value()],
                                            [set_view_key()]}.
view_insert_doc_query_results(_DocId, _PartitionId, [], [], ViewKVsAcc,
        ViewIdKeysAcc) ->
    {lists:reverse(ViewKVsAcc), lists:reverse(ViewIdKeysAcc)};
view_insert_doc_query_results(DocId, PartitionId, [ResultKVs | RestResults],
        [{View, KVs} | RestViewKVs], ViewKVsAcc, ViewIdKeysAcc) ->
    ResultKvGeoms = [{maybe_process_key(Key), Val} || {Key, Val} <- ResultKVs],
    % Take any identical keys and combine the values
    {NewKVs, NewViewIdKeysAcc} = lists:foldl(
        fun({{Key, Geom}, Val}, {[{{Key, PrevDocId} = Kd, PrevVal} | AccRest],
                AccVid}) when PrevDocId =:= DocId ->
            Dups = case PrevVal of
            {PartitionId, {dups, Dups0}} ->
                [{Val, Geom} | Dups0];
            {PartitionId, UserPrevVal} ->
                [{Val, Geom}, UserPrevVal]
            end,
            {[{Kd, {PartitionId, {dups, Dups}}} | AccRest], AccVid};
        ({{Key, Geom}, Val}, {AccKv, AccVid}) ->
            {[{{Key, DocId}, {PartitionId, {Val, Geom}}} | AccKv],
                [Key | AccVid]}
        end,
        {KVs, []}, lists:sort(ResultKvGeoms)),
    NewViewKVsAcc = [{View, NewKVs} | ViewKVsAcc],
    case NewViewIdKeysAcc of
    [] ->
        NewViewIdKeysAcc2 = ViewIdKeysAcc;
    _ ->
        NewViewIdKeysAcc2 = [
            {View#set_view.id_num, NewViewIdKeysAcc} | ViewIdKeysAcc]
    end,
    view_insert_doc_query_results(
        DocId, PartitionId, RestResults, RestViewKVs, NewViewKVsAcc,
        NewViewIdKeysAcc2).


-spec query_args_view_name(#spatial_query_args{}) -> binary().
query_args_view_name(#spatial_query_args{view_name = ViewName}) ->
    ViewName.


-spec validate_ddoc(#doc{}) -> ok.
validate_ddoc(#doc{body = {Body}}) ->
    Spatial = couch_util:get_value(<<"spatial">>, Body, {[]}),
    case Spatial of
    {Views} when is_list(Views) ->
        ok;
    _ ->
        Views = [],
        throw({error, <<"The field `spatial' is not a json object.">>})
    end,
    HasMapreduce = case couch_util:get_value(<<"views">>, Body, {[]}) of
    {[]} ->
        false;
    _ ->
        true
    end,
    case length(Views) > 0 andalso HasMapreduce of
    true ->
        throw({error, <<"A design document may only contain mapreduce *or* "
            "spatial views">>});
    false ->
        ok
    end,
    lists:foreach(
        fun({SpatialName, Value}) ->
            validate_spatial_definition(SpatialName, Value)
        end,
        Views).


-spec validate_spatial_definition(binary(), binary()) -> ok.
validate_spatial_definition(<<"">>, _) ->
    throw({error, <<"Spatial view name cannot be an empty string">>});
validate_spatial_definition(SpatialName, SpatialDef) when
        is_binary(SpatialDef) ->
    validate_spatial_name(SpatialName, iolist_to_binary(io_lib:format(
        "Spatial view name `~s` cannot have leading or trailing whitespace",
        [SpatialName]))),
    validate_spatial_function(SpatialName, SpatialDef);
validate_spatial_definition(SpatialName, _) ->
    ErrorMsg = io_lib:format("Value for spatial view `~s' is not "
                             "a string.", [SpatialName]),
    throw({error, iolist_to_binary(ErrorMsg)}).


% Make sure the view name doesn't contain leading or trailing whitespace
% (space, tab, newline or carriage return)
-spec validate_spatial_name(binary(), binary()) -> ok.
validate_spatial_name(<<" ", _Rest/binary>>, ErrorMsg) ->
    throw({error, ErrorMsg});
validate_spatial_name(<<"\t", _Rest/binary>>, ErrorMsg) ->
    throw({error, ErrorMsg});
validate_spatial_name(<<"\n", _Rest/binary>>, ErrorMsg) ->
    throw({error, ErrorMsg});
validate_spatial_name(<<"\r", _Rest/binary>>, ErrorMsg) ->
    throw({error, ErrorMsg});
validate_spatial_name(Bin, ErrorMsg) when size(Bin) > 1 ->
    Size = size(Bin) - 1 ,
    <<_:Size/binary, Trailing/bits>> = Bin,
    % Check for trailing whitespace
    validate_spatial_name(Trailing, ErrorMsg);
validate_spatial_name(_, _) ->
    ok.


-spec validate_spatial_function(binary(), binary()) -> ok.
validate_spatial_function(SpatialName, SpatialDef) ->
    case mapreduce:start_map_context(spatial_view, [SpatialDef]) of
    {ok, _Ctx} ->
        ok;
    {error, Reason} ->
        ErrorMsg = io_lib:format("Syntax error in the spatial function of"
                                 " the spatial view `~s': ~s",
                                 [SpatialName, Reason]),
        throw({error, iolist_to_binary(ErrorMsg)})
    end.
