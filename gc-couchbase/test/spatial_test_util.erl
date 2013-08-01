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

-module(spatial_test_util).

-export([start_server/1, stop_server/0]).


start_server() ->
    couch_server_sup:start_link(test_util:config_files()),
    put(addr, couch_config:get("httpd", "bind_address", "127.0.0.1")),
    put(port, integer_to_list(mochiweb_socket_server:get(couch_httpd, port))).


start_server(SetName) ->
    GeoCouchConfig = filename:join(root_dir() ++ [config_file()]),
    ConfigFiles = test_util:config_files() ++ [GeoCouchConfig],
    couch_config:start_link(ConfigFiles),
    DbDir = couch_config:get("couchdb", "database_dir"),
    IndexDir = couch_config:get("couchdb", "view_index_dir"),
    NewDbDir = filename:join([DbDir, binary_to_list(SetName)]),
    NewIndexDir = filename:join([IndexDir, binary_to_list(SetName)]),
    case file:make_dir(NewDbDir) of
    ok ->
        ok;
    {error, eexist} ->
        ok;
    Error ->
        throw(Error)
    end,
    case file:make_dir(NewIndexDir) of
    ok ->
        ok;
    {error, eexist} ->
        ok;
    Error2 ->
        throw(Error2)
    end,
    ok = couch_config:set("couchdb", "database_dir", NewDbDir, false),
    ok = couch_config:set("couchdb", "view_index_dir", NewIndexDir, false),
    start_server(),
    ok.


stop_server() ->
    ok = timer:sleep(1000),
    couch_server_sup:stop().


% @doc The location of the GeoCouch config file relative to the root directory
-spec config_file() -> string().
config_file() ->
    "etc/couchdb/default.d/geocouch.ini".


% @doc Returns the root directory of GeoCouch as a list. It makes the
% assumptions that the currently running test is in <rootdir>/test/thetest.t
-spec root_dir() -> [file:filename()].
root_dir() ->
    EscriptName = filename:split(filename:absname(escript:script_name())),
    lists:sublist(EscriptName, length(EscriptName)-2).
