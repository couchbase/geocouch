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

% This module is a copy from couch_btree_copy adapted to the vtree
-module(vtree_copy).

-export([from_sorted_file/4]).


-include_lib("vtree/include/vtree.hrl").

% XXX vmx 2013-01-14: from couch_btree_copy! Don't duplicate it, but
%    move it to a common header file or refactor things properly
-define(FLUSH_PAGE_CACHE_AFTER, 102400).
-record(acc, {
    vtree,
    fd,
    before_kv_write = nil,
    user_acc = [],
    filter = fun(_) -> true end,
    kv_chunk_threshold,
    kp_chunk_threshold,
    nodes = array:new(),
    cur_level = 1,
    max_level = 1,
    % only used while at bottom level (1)
    values = [],
    leaf_size = 0
}).


from_sorted_file(EmptyVtree, SortedFileName, DestFd, BinToKvFun) ->
    Acc = #acc{
        vtree = EmptyVtree,
        fd = DestFd,
        kv_chunk_threshold = EmptyVtree#vtree.kv_chunk_threshold,
        kp_chunk_threshold = EmptyVtree#vtree.kp_chunk_threshold
    },
    {ok, SourceFd} = file2:open(SortedFileName, [read, raw, binary, read_ahead]),
    {ok, Acc2} = try
        sorted_file_fold(SourceFd, SortedFileName, BinToKvFun, 0, 0, Acc)
    after
        ok = file:close(SourceFd)
    end,
    {ok, CopyRootState, _FinalAcc} = finish_copy(Acc2),
    {ok, CopyRootState}.


sorted_file_fold(Fd, FileName, BinToKvFun, AdviseOffset, BytesRead, Acc) ->
    case file:read(Fd, 4) of
    {ok, <<Len:32>>} ->
        case file:read(Fd, Len) of
        {ok, KvBin} ->
            BytesRead2 = BytesRead + 4 + Len,
            case (BytesRead2 - AdviseOffset) >= ?FLUSH_PAGE_CACHE_AFTER of
            true ->
                AdviseOffset2 = BytesRead2,
                (catch file:advise(Fd, AdviseOffset, BytesRead2, dont_need));
            false ->
                AdviseOffset2 = AdviseOffset
            end,
            % XXX 2013-01-14: This BinToKvFun/1 call can probably be hard-coded
            %     to that function (even in couch_btree_copy).
            Kv = BinToKvFun(KvBin),
            {ok, Acc2} = fold_copy(Kv, Len, Acc),
            sorted_file_fold(Fd, FileName, BinToKvFun, AdviseOffset2, BytesRead2, Acc2);
        eof ->
            throw({unexpected_eof, FileName});
        {error, Error} ->
            throw({file_read_error, FileName, Error})
        end;
    eof ->
        (catch file:advise(Fd, AdviseOffset, BytesRead, dont_need)),
        {ok, Acc};
    {error, Error} ->
        throw({file_read_error, FileName, Error})
    end.


% fold_copy/3 is the real central piece of code. Here the tree is built from
% the sorted file
fold_copy(Item, ItemSize, #acc{cur_level = 1} = Acc) ->
    #acc{
        values = Values,
        leaf_size = LeafSize
    } = Acc,
    Kv = extract(Acc, Item),
    LeafSize2 = LeafSize + ItemSize,
    Values2 = [Kv | Values],
    NextAcc = case LeafSize2 >= Acc#acc.kv_chunk_threshold of
    true ->
        % XXX vmx 2013-01-14: flush_leaf does the reduces and also updates
        %     the task status (via before_leaf_write/2). I'll leave that
        %     out for now and instead call write_leaf directly.
        %{LeafState, Acc2} = flush_leaf(Values2, Acc),
        {ok, KpNode} = write_leaf(Acc, Values2),
        bubble_up(KpNode, Acc);
    false ->
        Acc#acc{values = Values2, leaf_size = LeafSize2}
    end,
    {ok, NextAcc}.


-spec split_key_docid(binary()) -> {binary(), binary()}.
split_key_docid(<<NumDoubles:16, Rest/binary>>) ->
    KeySize = NumDoubles*8,
    <<Mbb:KeySize/binary, DocId/binary>> = Rest,
    {Mbb, DocId}.

extract(_Acc, {KeyDocId, Value}) ->
    {Key0, DocId} = split_key_docid(KeyDocId),
    Key = bin_to_mbb(Key0),
    #kv_node{
       key = Key,
       docid = DocId,
       % TODO vmx 2013-07-01: include geometry
       geometry = nil,
       body = Value
    }.


% Converts a list of doubles to an MBB
bin_to_mbb(Bin) ->
    bin_to_mbb(Bin, []).
bin_to_mbb(<<>>, Acc) ->
    lists:reverse(Acc);
bin_to_mbb(<<Min:64/native-float, Max:64/native-float, Rest/binary>>, Acc0) ->
    Acc = [{Min, Max}|Acc0],
    bin_to_mbb(Rest, Acc).


write_leaf(#acc{fd = Fd, vtree = Vt}, NodeList0) ->
    NodeList = vtree_io:write_kvnode_external(Fd, NodeList0),
    {ok, KpNode} = vtree_io:write_node(Fd, NodeList, Vt#vtree.less),
    {ok, KpNode#kp_node{mbb_orig = KpNode#kp_node.key}}.


bubble_up(KpNode, #acc{cur_level = Level} = Acc) ->
    bubble_up(KpNode, Level, Acc).

bubble_up(KpNode, Level, Acc) ->
    #acc{max_level = MaxLevel, nodes = Nodes, fd = Fd, vtree = Vt} = Acc,
    Acc2 = case Level of
    1 ->
        Acc#acc{values = [], leaf_size = 0};
    _ ->
        Acc#acc{nodes = array:set(Level, {0, []}, Nodes)}
    end,
    % XXX vmx 2013-01-14: Not sure if external_size is good enough here.
    KpSize = erlang:external_size(KpNode),
    case Level of
    MaxLevel ->
        Acc2#acc{
            nodes = array:set(Level + 1, {KpSize, [KpNode]}, Acc2#acc.nodes),
            max_level = Level + 1
        };
    _ when Level < MaxLevel ->
        {Size, NextLevelNodes} = array:get(Level + 1, Acc2#acc.nodes),
        NextLevelNodes2 = [KpNode | NextLevelNodes],
        Size2 = Size + KpSize,
        case Size2 >= Acc#acc.kp_chunk_threshold of
        true ->
            {ok, NewKpNode0} = vtree_io:write_node(
                Fd, lists:reverse(NextLevelNodes2), Vt#vtree.less),
            NewKpNode = NewKpNode0#kp_node{mbb_orig = NewKpNode0#kp_node.key},
            bubble_up(NewKpNode, Level + 1, Acc2);
        false ->
            Acc2#acc{
                nodes = array:set(Level + 1, {Size2, NextLevelNodes2}, Acc2#acc.nodes)
            }
        end
    end.


finish_copy(#acc{nodes = Nodes, values = LeafValues, leaf_size = LeafSize} = Acc) ->
    Acc2 = Acc#acc{
        nodes = array:set(1, {LeafSize, LeafValues}, Nodes)
    },
    finish_copy_loop(Acc2).


finish_copy_loop(#acc{cur_level = 1, max_level = 1, values = LeafValues} = Acc) ->
    case LeafValues of
    [] ->
        {ok, nil, Acc};
    [#kv_node{} | _] = KvList ->
        {ok, KpNode} = write_leaf(Acc, KvList),
        {ok, KpNode, Acc}
    end;

finish_copy_loop(#acc{cur_level = Level, max_level = Level} = Acc) ->
    #acc{
        nodes = Nodes,
        fd = Fd,
        vtree = Vt
    } = Acc,
    case array:get(Level, Nodes) of
    {_Size, [{_Key, {Pos, Red, Size}}]} ->
        {ok, {Pos, Red, Size}, Acc};
    {_Size, NodeList} ->
        {ok, NewKpNode} = vtree_io:write_node(
            Fd, lists:reverse(NodeList), Vt#vtree.less),
        {ok, NewKpNode#kp_node{mbb_orig = NewKpNode#kp_node.key}, Acc}
    end;

finish_copy_loop(Acc) ->
    #acc{
        cur_level = Level,
        nodes = Nodes,
        fd = Fd,
        vtree = Vt
    } = Acc,
    case array:get(Level, Nodes) of
    {0, []} ->
        Acc2 = Acc#acc{cur_level = Level + 1},
        finish_copy_loop(Acc2);
    {_Size, NodeList} ->
        {Kp, Acc2} = case Level of
        1 ->
            {ok, KpNode} = write_leaf(Acc, NodeList),
            {KpNode, Acc};
        _ when Level > 1 ->
            {ok, KpNode} = vtree_io:write_node(
                Fd, lists:reverse(NodeList), Vt#vtree.less),
            {KpNode#kp_node{mbb_orig = KpNode#kp_node.key}, Acc}
        end,
        {ParentSize, ParentNode} = array:get(Level + 1, Nodes),
        ParentSize2 = ParentSize + erlang:external_size(Kp),
        Acc3 = Acc2#acc{
            nodes = array:set(Level + 1, {ParentSize2, [Kp | ParentNode]}, Nodes),
            cur_level = Level + 1
        },
        finish_copy_loop(Acc3)
    end.
