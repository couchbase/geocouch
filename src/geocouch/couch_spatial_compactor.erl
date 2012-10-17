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

-export([start_compact/2]).

%% @spec start_compact(
%%     DbName::binary()|{DbName::binary(), GroupDbName::binary()},
%%     GroupId::binary()) -> ok
%% @doc Compacts the spatial indexes.  GroupId must not include the _design/
%% prefix
% For foreign Design Document
start_compact({DbName, GroupDbName}, GroupId) ->
    Pid = couch_spatial:get_group_server({DbName, GroupDbName}, GroupId),
    CompactFun = fun(Group, EmptyGroup, _) ->
        compact_group(Group, EmptyGroup, {DbName, GroupDbName})
    end,
    gen_server:cast(Pid, {start_compact, CompactFun});
start_compact(DbName, GroupId) ->
    Pid = couch_spatial:get_group_server(
        DbName, <<"_design/",GroupId/binary>>),
    gen_server:cast(Pid, {start_compact, fun compact_group/3}).

%%=============================================================================
%% internal functions
%%=============================================================================

% For foreign Design Documents (stored in a different DB)
docs_db_name({DocsDbName, _DDocDbName}) ->
    DocsDbName;
docs_db_name(DbName) when is_binary(DbName) ->
    DbName.

%% @spec compact_group(Group, NewGroup) -> ok
compact_group(Group, EmptyGroup, DbName) ->
    #spatial_group{
        current_seq = Seq,
        id_btree = IdBtree,
        name = GroupId,
        indexes = Indexes,
        fd = Fd
    } = Group,

    #spatial_group{
        id_btree = EmptyIdBtree,
        indexes = EmptyIndexes,
        fd = EmptyFd
    } = EmptyGroup,

    DbName1 = docs_db_name(DbName),
    {ok, Db} = couch_db:open_int(DbName1, []),
    {ok, <<Count:40, _DelCount:40, _Size:48>>} =
        couch_btree:full_reduce(Db#db.docinfo_by_id_btree),
    couch_db:close(Db),

    % Use "view_compaction" for now, that it shows up in Futons active tasks
    % screen. Think about a more generic way for the future.
    couch_task_status:add_task([
        {type, view_compaction},
        {database, DbName1},
        {design_document, GroupId},
        {progress, 0}
    ]),
    % Create a new version of the lookup tree (the ID B-tree)
    Fun = fun({DocId, _IndexIdKeys} = KV, {Bt, Acc, TotalCopied, _LastId}) ->
        % NOTE vmx (2011-01-18): use the same value as for view compaction,
        %     though wondering why a value of 10000 is hard-coded
        if TotalCopied rem 10000 =:= 0 ->
            couch_task_status:update([
                {progress, (TotalCopied*100) div Count}]),
            {ok, Bt2} = couch_btree:add(Bt, lists:reverse([KV|Acc])),
            {ok, {Bt2, [], TotalCopied+1, DocId}};
        true ->
            {ok, {Bt, [KV|Acc], TotalCopied+1, DocId}}
        end
    end,
    {ok, _, {Bt3, Uncopied, _Total, _LastId}} = couch_btree:foldl(IdBtree, Fun,
        {EmptyIdBtree, [], 0, nil}),
    couch_file:flush(Bt3#btree.fd),
    {ok, NewIdBtree} = couch_btree:add(Bt3, lists:reverse(Uncopied)),
    couch_file:flush(NewIdBtree#btree.fd),

    NewIndexes = lists:map(fun({Index, EmptyIndex}) ->
        case Index#spatial.treepos of
            % Tree is empty, just grab the the FD
            nil -> EmptyIndex#spatial{fd = EmptyFd};
            _ -> compact_spatial(Fd, EmptyFd, Index, EmptyIndex)
        end
    end, lists:zip(Indexes, EmptyIndexes)),

    NewGroup = EmptyGroup#spatial_group{
        id_btree=NewIdBtree,
        indexes=NewIndexes,
        current_seq=Seq
    },

    Pid = couch_spatial:get_group_server(DbName, GroupId),
    gen_server:cast(Pid, {compact_done, NewGroup}).

%% @spec compact_spatial(Index, EmptyIndex) -> CompactView
compact_spatial(OldFd, NewFd, Index, EmptyIndex) ->
    {ok, Count} = couch_spatial:get_item_count(OldFd, Index#spatial.treepos),

    Fun = fun(Node, {TreePos, TreeHeight, Acc, TotalCopied}) ->
        if TotalCopied rem 10000 =:= 0 ->
            couch_task_status:update([
                {progress, (TotalCopied*100) div Count}]),
            {ok, TreePos2, TreeHeight2} = vtree_bulk:bulk_load(
                NewFd, TreePos, TreeHeight, [Node|Acc]),
            {TreePos2, TreeHeight2, [], TotalCopied + 1};
        true ->
            {TreePos, TreeHeight, [Node|Acc], TotalCopied + 1}
        end
    end,

    {TreePos3, TreeHeight3, Uncopied, _Total} = vtree:foldl(
        OldFd, Index#spatial.treepos, Fun,
        {EmptyIndex#spatial.treepos, EmptyIndex#spatial.treeheight, [], 0}),
    {ok, NewTreePos, NewTreeHeight} = vtree_bulk:bulk_load(
        NewFd, TreePos3, TreeHeight3, Uncopied),
    EmptyIndex#spatial{
        treepos = NewTreePos,
        treeheight = NewTreeHeight,
        fd = NewFd
    }.
