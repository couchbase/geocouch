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
-export([fold/5]).
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
    ets:new(couch_spatial_groups_by_db, [bag, protected, named_table]),
    ets:new(spatial_group_servers_by_sig, [set, protected, named_table]),
    ets:new(couch_spatial_groups_by_updater, [set, private, named_table]),

    couch_db_update_notifier:start_link(
        fun({deleted, DbName}) ->
            gen_server:cast(couch_spatial, {reset_indexes, DbName});
        ({created, DbName}) ->
            gen_server:cast(couch_spatial, {reset_indexes, DbName});
        ({ddoc_updated, {DbName, #doc{id = DDocId} = DDoc}}) ->
            % In Coucbase, the ddoc_updated event is only triggered on the the
            % master database. This means that we need to keep track of all
            % the spatial indexes on the vBucket databases. Hence the
            % `couch_spatial_groups_by_db` ets table keeps track not only of
            % the databases the spatial groups belong to, but also of the
            % master database which contains the design document
            % (`ForeignDbName`).
            % In case there is no foreign database (non Couchbase spatial
            % groups), the database the group belongs to, is used (as it
            % contains the design document).
            % When the ddoc_updated event is triggered, we loop through the
            % `couch_spatial_groups_by_db` ets table matching the
            % *databases that contain the design document* to get all the
            % groups that belong to that one (which is one for every vBucket
            % in Couchbase).
            case ets:match_object(couch_spatial_groups_by_db,
                    {{'_', DbName}, {DDocId, '$1'}}) of
            [] ->
                ok;
            Groups ->
                lists:foreach(fun({{DbName1, _}, {_, Sig}}) ->
                    update_group(DbName1, DDoc, Sig)
                end, Groups)
            end;
        (_Else) ->
            ok
        end),
    process_flag(trap_exit, true),
    ok = couch_file:init_delete_dir(RootDir),
    {ok, #spatial{root_dir=RootDir}}.

update_group(DbName, DDoc, Sig) ->
    case ets:lookup(spatial_group_servers_by_sig, {DbName, Sig}) of
    [{_, GroupPid}] ->
        NewSig = case DDoc#doc.deleted of
        true ->
            <<>>;
        false ->
            DDoc2 = couch_doc:with_ejson_body(DDoc),
            list_to_binary(couch_spatial_group:get_signature(DDoc2))
        end,
        (catch gen_server:cast(GroupPid, {ddoc_updated, NewSig}));
    [] ->
        ok
    end.

add_to_ets(Pid, DbName, ForeignDbName, DDocId, Sig) ->
    true = ets:insert(couch_spatial_groups_by_updater, {Pid, {DbName, Sig}}),
    true = ets:insert(spatial_group_servers_by_sig, {{DbName, Sig}, Pid}),
    true = ets:insert(
        couch_spatial_groups_by_db, {{DbName, ForeignDbName}, {DDocId, Sig}}).


delete_from_ets(Pid, DbName, ForeignDbName, DDocId, Sig) ->
    true = ets:delete(couch_spatial_groups_by_updater, Pid),
    true = ets:delete(spatial_group_servers_by_sig, {DbName, Sig}),
    true = ets:delete_object(
        couch_spatial_groups_by_db, {{DbName, ForeignDbName}, {DDocId, Sig}}).

% For foreign Design Documents (stored in a different DB)
get_group_server({DbName, GroupDbName}, GroupId) when is_binary(GroupId) ->
    DbGroup = case GroupId of
    <<?DESIGN_DOC_PREFIX, _/binary>> ->
        open_db_group(GroupDbName, GroupId);
    _ ->
        open_db_group(GroupDbName, <<?DESIGN_DOC_PREFIX, GroupId/binary>>)
    end,
    get_group_server(DbName, DbGroup);
get_group_server(DbName, GroupId) when is_binary(GroupId) ->
    Group = open_db_group(DbName, GroupId),
    get_group_server(DbName, Group);
get_group_server(DbName, Group) ->
    case gen_server:call(couch_spatial, {get_group_server, DbName, Group}, infinity) of
    {ok, Pid} ->
        Pid;
    Error ->
        throw(Error)
    end.

open_db_group(DbName, GroupId) ->
    case couch_spatial_group:open_db_group(DbName, GroupId) of
    {ok, Group} ->
        Group;
    Error ->
        throw(Error)
    end.

get_group(Db, {GroupDb, GroupId}, Stale) ->
    DbGroup = open_db_group(couch_db:name(GroupDb), GroupId),
    do_get_group(Db, DbGroup, Stale);
get_group(Db, GroupId, Stale) ->
    DbGroup = open_db_group(couch_db:name(Db), GroupId),
    do_get_group(Db, DbGroup, Stale).


do_get_group(Db, GroupId, Stale) ->
    MinUpdateSeq = case Stale of
    ok -> 0;
    update_after -> 0;
    _Else -> couch_db:get_update_seq(Db)
    end,
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

get_group_info({DbName, GroupDbName}, GroupId) ->
    GroupPid = get_group_server({DbName, GroupDbName}, GroupId),
    couch_view_group:request_group_info(GroupPid);
get_group_info(#db{name = DbName}, GroupId) ->
    get_group_info(DbName, GroupId);
get_group_info(DbName, GroupId) ->
    couch_view_group:request_group_info(get_group_server(DbName, GroupId)).


cleanup_index_files(Db) ->
    % load all ddocs
    {ok, DesignDocs} = couch_db:get_design_docs(Db, no_deletes),
    % make unique list of group sigs
    Sigs = lists:map(fun couch_spatial_group:get_signature/1, DesignDocs),
    FileList = list_index_files(Db),

    % regex that matches all ddocs
    RegExp = "("++ string:join(Sigs, "|") ++")",

    % filter out the ones in use
    DeleteFiles = case Sigs of
    [] ->
        FileList;
    _ ->
        [FilePath || FilePath <- FileList,
            re:run(FilePath, RegExp, [{capture, none}]) =:= nomatch]
    end,
    % delete unused files
    case DeleteFiles of
    [] ->
        ok;
    _ ->
        ?LOG_INFO("Deleting unused (old) spatial index files:~n~n~s",
            [string:join(DeleteFiles, "\n")])
    end,
    RootDir = couch_config:get("couchdb", "view_index_dir"),
    lists:foreach(
        fun(File) -> couch_file:delete(RootDir, File, false) end,
        DeleteFiles).

delete_index_dir(RootDir, DbName) ->
    geocouch_duplicates:nuke_dir(
        RootDir, RootDir ++ "/." ++ ?b2l(DbName) ++ "_design").

list_index_files(Db) ->
    % call server to fetch the index files
    RootDir = couch_config:get("couchdb", "view_index_dir"),
    filelib:wildcard(RootDir ++ "/." ++ ?b2l(couch_db:name(Db)) ++
        "_design"++"/*.spatial").

do_reset_indexes(DbName, Root) ->
    % shutdown all the updaters and clear the files, the db got changed
    Names = ets:lookup(couch_spatial_groups_by_db, {DbName, '_'}),
    lists:foreach(
        fun({{_DbName, ForeignDbName}, {DDocId, Sig}}) ->
            ?LOG_DEBUG("Killing update process for spatial group ~s. in database ~s.", [Sig, DbName]),
            [{_, Pid}] = ets:lookup(spatial_group_servers_by_sig, {DbName, Sig}),
            couch_util:shutdown_sync(Pid),
            delete_from_ets(Pid, DbName, ForeignDbName, DDocId, Sig)
        end, Names),
    delete_index_dir(Root, DbName),
    RootDelDir = couch_config:get("couchdb", "view_index_dir"),
    couch_file:delete(RootDelDir, Root ++ "/." ++ ?b2l(DbName) ++ "_temp").

% counterpart in couch_view is get_map_view/4
get_spatial_index(Db, GroupId, Name, Stale) ->
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
    {not_found, missing_named_index};
get_spatial_index0(Name, [#spatial{index_names=IndexNames}=Index|Rest]) ->
    % NOTE vmx: I don't understand why need lists:member and recursion
    case lists:member(Name, IndexNames) of
        true -> {ok, Index};
        false -> get_spatial_index0(Name, Rest)
    end.


terminate(_Reason, _Srv) ->
    ok.

handle_call({get_group_server, DbName, #spatial_group{sig=Sig} = Group}, From,
            #spatial{root_dir=Root} = Server) ->
    case ets:lookup(spatial_group_servers_by_sig, {DbName, Sig}) of
    [] ->
        spawn_monitor(fun() -> new_group(Root, DbName, Group) end),
        ets:insert(spatial_group_servers_by_sig, {{DbName, Sig}, [From]}),
        {noreply, Server};
    [{_, WaitList}] when is_list(WaitList) ->
        ets:insert(spatial_group_servers_by_sig, {{DbName, Sig}, [From | WaitList]}),
        {noreply, Server};
    [{_, ExistingPid}] ->
        {reply, {ok, ExistingPid}, Server}
    end;

handle_call({reset_indexes, DbName}, _From, #spatial{root_dir=Root}=Server) ->
    do_reset_indexes(DbName, Root),
    {reply, ok, Server}.

handle_cast({reset_indexes, DbName}, #spatial{root_dir=Root}=Server) ->
    do_reset_indexes(DbName, Root),
    {noreply, Server}.

new_group(Root, DbName, Group) ->
    #spatial_group{
        name = GroupId,
        sig = Sig,
        dbname = ForeignDbName
    } = Group,
    ?LOG_DEBUG("Spawning new group server for spatial group ~s in database ~s.",
        [GroupId, DbName]),
    case (catch couch_spatial_group:start_link({Root, DbName, Group})) of
    {ok, NewPid} ->
        unlink(NewPid),
        exit({DbName, ForeignDbName, GroupId, Sig, {ok, NewPid}});
    {error, invalid_view_seq} ->
        ok = gen_server:call(couch_spatial, {reset_indexes, DbName}),
        new_group(Root, DbName, Group);
    Error ->
        exit({DbName, ForeignDbName, GroupId, Sig, Error})
    end.


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
    [{_, {DbName, Sig}}] ->
        [{{DbName, ForeignDbName}, {DDocId, Sig}}] = ets:match_object(
            couch_spatial_groups_by_db, {{DbName, '_'}, {'$1', Sig}}),
        delete_from_ets(FromPid, DbName, ForeignDbName, DDocId, Sig)
    end,
    {noreply, Server};

handle_info({'DOWN', _, _, _, {DbName, ForeignDbName, DDocId, Sig, Reply}}, Server) ->
    [{_, WaitList}] = ets:lookup(spatial_group_servers_by_sig, {DbName, Sig}),
    [gen_server:reply(From, Reply) || From <- WaitList],
    case Reply of {ok, NewPid} ->
        link(NewPid),
        add_to_ets(NewPid, DbName, ForeignDbName, DDocId, Sig);
     _ -> ok end,
    {noreply, Server}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

% counterpart in couch_view is fold/4
fold(Index, FoldFun, InitAcc, Bbox, Bounds) ->
    WrapperFun = fun(Node, Acc) ->
        Expanded = couch_view:expand_dups([Node], []),
        lists:foldl(fun(E, {ok, Acc2}) ->
            FoldFun(E, Acc2)
        end, {ok, Acc}, Expanded)
    end,
    {_State, Acc} = vtree:lookup(
        Index#spatial.fd, Index#spatial.treepos, Bbox,
        {WrapperFun, InitAcc}, Bounds),
    {ok, Acc}.

% counterpart in couch_view is get_row_count/1
get_item_count(Fd, TreePos) ->
    Count = vtree:count_total(Fd, TreePos),
    {ok, Count}.
