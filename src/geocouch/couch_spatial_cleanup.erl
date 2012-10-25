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

-module(couch_spatial_cleanup).

-export([run/1]).

-include("couch_db.hrl").
-include("couch_spatial.hrl").


run(Db) ->
    RootDir = couch_index_util:root_dir(),
    DbName = couch_db:name(Db),

    DesignDocs = couch_db:get_design_docs(Db),
    SigFiles = lists:foldl(fun(DDocInfo, SigFilesAcc) ->
        {ok, DDoc} = couch_db:open_doc_int(Db, DDocInfo, [ejson_body]),
        {ok, InitState} = couch_spatial_util:ddoc_to_spatial_state(
            DbName, DDoc),
        Sig = InitState#spatial_state.sig,
        IndexFilename = couch_spatial_util:index_file(DbName, Sig),
        CompactFilename = couch_spatial_util:compaction_file(DbName, Sig),
        [IndexFilename, CompactFilename | SigFilesAcc]
    end, [], [DD || DD <- DesignDocs, DD#full_doc_info.deleted == false]),

    IdxDir = couch_index_util:index_dir(spatial, DbName),
    DiskFiles = filelib:wildcard(filename:join(IdxDir, "*")),

    % We need to delete files that have no ddoc.
    ToDelete = DiskFiles -- SigFiles,

    lists:foreach(fun(FN) ->
        ?LOG_DEBUG("Deleting stale spatial view file: ~s", [FN]),
        couch_file:delete(RootDir, FN, false)
    end, ToDelete),

    ok.
