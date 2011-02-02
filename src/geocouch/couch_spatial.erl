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

-module(couch_spatial).
-behaviour(gen_server).

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3]).
-export([fold/6]).
% For List functions
-export([get_spatial_index/4]).
% For compactor
-export([get_group_server/2, get_item_count/2]).
% For _spatialinfo
-export([get_group_info/2]).
% For _spatial_cleanup
-export([cleanup_index_files/1]).


-include("couch_db.hrl").
-include("couch_spatial.hrl").


start_link() ->
    ?LOG_DEBUG("Spatial daemon: starting link.", []),
    gen_server:start_link({local, couch_spatial}, couch_spatial, [], []).

init([]) ->
    RootDir = couch_config:get("couchdb", "view_index_dir"),
    ets:new(couch_spatial_groups_by_db, [bag, private, named_table]),
    ets:new(spatial_group_servers_by_sig, [set, protected, named_table]),
    ets:new(couch_spatial_groups_by_updater, [set, private, named_table]),
    process_flag(trap_exit, true),
    {ok, #spatial{root_dir=RootDir}}.

add_to_ets(Pid, DbName, Sig) ->
    true = ets:insert(couch_spatial_groups_by_updater, {Pid, {DbName, Sig}}),
    true = ets:insert(spatial_group_servers_by_sig, {{DbName, Sig}, Pid}),
    true = ets:insert(couch_spatial_groups_by_db, {DbName, Sig}).


delete_from_ets(Pid, DbName, Sig) ->
    true = ets:delete(couch_spatial_groups_by_updater, Pid),
    true = ets:delete(spatial_group_servers_by_sig, {DbName, Sig}),
    true = ets:delete_object(couch_spatial_groups_by_db, {DbName, Sig}).


get_group_server(DbName, DDocName) ->
    % get signature for group
    case couch_spatial_group:open_db_group(DbName, DDocName) of
    % do we need to close this db?
    {ok, _Db, Group} ->
?LOG_DEBUG("get_group_server: ~p", [DDocName]),
        case gen_server:call(couch_spatial, {get_group_server, DbName, Group}) of
        {ok, Pid} ->
            Pid;
        Error ->
            throw(Error)
        end;
    Error ->
        throw(Error)
    end.

get_group(Db, GroupId, Stale) ->
    MinUpdateSeq = case Stale of
    ok -> 0;
    update_after -> 0;
    _Else -> couch_db:get_update_seq(Db)
    end,
?LOG_DEBUG("get_group: MinUpdateSeq: ~p (stale? ~p)", [MinUpdateSeq, Stale]),
    GroupPid = get_group_server(couch_db:name(Db), GroupId),
    Result = couch_spatial_group:request_group(GroupPid, MinUpdateSeq),
    case Stale of
    update_after ->
        % best effort, process might die
        spawn(fun() ->
            LastSeq = couch_db:get_update_seq(Db),
            couch_spatial_group:request_group(GroupPid, LastSeq)
        end);
    _ ->
        ok
    end,
    Result.

get_group_info(Db, GroupId) ->
    couch_view_group:request_group_info(
        get_group_server(couch_db:name(Db), GroupId)).

% The only reason why couch_view:cleanup_index_files can't be used is the
% call to get_group_info
cleanup_index_files(Db) ->
    % load all ddocs
    {ok, DesignDocs} = couch_db:get_design_docs(Db),

    % make unique list of group sigs
    Sigs = lists:map(fun(#doc{id = GroupId}) ->
        {ok, Info} = get_group_info(Db, GroupId),
        ?b2l(couch_util:get_value(signature, Info))
    end, [DD||DD <- DesignDocs, DD#doc.deleted == false]),

    % It's ok to use a clone of couch_view:list_index_files, as
    % spatial indexes and view indexes are in the same
    % directory (view_index_dir setting)
    FileList = geocouch_duplicates:list_index_files(Db),

    % regex that matches all ddocs
    RegExp = "("++ string:join(Sigs, "|") ++")",

    % filter out the ones in use
    DeleteFiles = [FilePath
           || FilePath <- FileList,
              re:run(FilePath, RegExp, [{capture, none}]) =:= nomatch],
    % delete unused files
    ?LOG_DEBUG("deleting unused view index files: ~p",[DeleteFiles]),
    RootDir = couch_config:get("couchdb", "view_index_dir"),
    [couch_file:delete(RootDir,File,false)||File <- DeleteFiles],
    ok.

delete_index_dir(RootDir, DbName) ->
    couch_view:nuke_dir(RootDir ++ "/." ++ ?b2l(DbName) ++ "_design").


% XXX NOTE vmx: I don't know when this case happens
do_reset_indexes(DbName, Root) ->
    % shutdown all the updaters and clear the files, the db got changed
    Names = ets:lookup(couch_spatial_groups_by_db, DbName),
    lists:foreach(
        fun({_DbName, Sig}) ->
            ?LOG_DEBUG("Killing update process for spatial group ~s. in database ~s.", [Sig, DbName]),
            [{_, Pid}] = ets:lookup(spatial_group_servers_by_sig, {DbName, Sig}),
            exit(Pid, kill),
            receive {'EXIT', Pid, _} ->
                delete_from_ets(Pid, DbName, Sig)
            end
        end, Names),
    delete_index_dir(Root, DbName),
    file:delete(Root ++ "/." ++ ?b2l(DbName) ++ "_temp").

% counterpart in couch_view is get_map_view/4
get_spatial_index(Db, GroupId, Name, Stale) ->
?LOG_DEBUG("get_spatial_index: ~p", [Name]),
    case get_group(Db, GroupId, Stale) of
    {ok, #spatial_group{indexes=Indexes}=Group} ->
        case get_spatial_index0(Name, Indexes) of
        {ok, Index} ->
            {ok, Index, Group};
        Else ->
            Else
        end;
    Error ->
        Error
    end.

get_spatial_index0(_Name, []) ->
    {not_found, missing_named_view};
get_spatial_index0(Name, [#spatial{index_names=IndexNames}=Index|Rest]) ->
?LOG_DEBUG("Name: ~p, IndexNames: ~p", [Name, IndexNames]),
    % NOTE vmx: I don't understand why need lists:member and recursion
    case lists:member(Name, IndexNames) of
        true -> {ok, Index};
        false -> get_spatial_index0(Name, Rest)
    end.


terminate(_Reason, _Srv) ->
    ok.

handle_call({get_group_server, DbName,
    #spatial_group{name=GroupId,sig=Sig}=Group}, _From,
            #spatial{root_dir=Root}=Server) ->
    case ets:lookup(spatial_group_servers_by_sig, {DbName, Sig}) of
    [] ->
        ?LOG_DEBUG("Spawning new group server for spatial group ~s in database ~s.",
            [GroupId, DbName]),
        case (catch couch_spatial_group:start_link({Root, DbName, Group})) of
        {ok, NewPid} ->
            add_to_ets(NewPid, DbName, Sig),
           {reply, {ok, NewPid}, Server};
        {error, invalid_view_seq} ->
            do_reset_indexes(DbName, Root),
            case (catch couch_spatial_group:start_link({Root, DbName, Group})) of
            {ok, NewPid} ->
                add_to_ets(NewPid, DbName, Sig),
                {reply, {ok, NewPid}, Server};
            Error ->
                {reply, Error, Server}
            end;
        Error ->
            {reply, Error, Server}
        end;
    [{_, ExistingPid}] ->
        {reply, {ok, ExistingPid}, Server}
    end.


handle_cast(foo,State) ->
    {noreply, State}.

% Cleanup on exit, e.g. resetting the group information stored in ETS tables 
handle_info({'EXIT', FromPid, Reason}, Server) ->
    case ets:lookup(couch_spatial_groups_by_updater, FromPid) of
    [] ->
        if Reason /= normal ->
            % non-updater linked process died, we propagate the error
            ?LOG_ERROR("Exit on non-updater process: ~p", [Reason]),
            exit(Reason);
        true -> ok
        end;
    [{_, {DbName, GroupId}}] ->
        delete_from_ets(FromPid, DbName, GroupId)
    end,
    {noreply, Server};

handle_info(_Msg, Server) ->
    {noreply, Server}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

% counterpart in couch_view is fold/4
fold(Group, Index, FoldFun, InitAcc, Bbox, Bounds) ->
    WrapperFun = fun(Node, Acc) ->
        Expanded = couch_view:expand_dups([Node], []),
        lists:foldl(fun(E, {ok, Acc2}) ->
            FoldFun(E, Acc2)
        end, {ok, Acc}, Expanded)
    end,
    {_State, Acc} = vtree:lookup(
        Group#spatial_group.fd, Index#spatial.treepos, Bbox,
        {WrapperFun, InitAcc}, Bounds),
    {ok, Acc}.

% counterpart in couch_view is get_row_count/1
get_item_count(Fd, TreePos) ->
    Count = vtree:count_total(Fd, TreePos),
    {ok, Count}.
