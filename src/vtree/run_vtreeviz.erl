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

-module(run_vtreeviz).
-export([run/0]).

-define(FILENAME, "/tmp/vtree_huge.bin").
%-define(FILENAME, "/tmp/couchdb_vtree.bin").
-define(NODE_NUM, 5000).

-record(node, {
    % type = inner | leaf
    type=leaf}).

run() ->
%    case couch_file:open(?FILENAME) of
%    {ok, Fd} ->
%        %io:format("Tree: ~p~n", [TreePos]),
%        vtreeviz:visualize(Fd, 1318),
%        ok;
%    {error, Reason} ->
%       io:format("ERROR (~s): Couldn't open file (~s) for tree storage~n",
%                  [Reason, ?FILENAME])
%    end.
    case couch_file:open(?FILENAME, [create, overwrite]) of
    {ok, Fd} ->
        TreePos = build_tree(Fd, ?NODE_NUM),
        %io:format("Tree: ~p~n", [TreePos]),
        vtreeviz:visualize(Fd, TreePos),
        ok;
    {error, Reason} ->
        io:format("ERROR (~s): Couldn't open file (~s) for tree storage~n",
                  [Reason, ?FILENAME])
    end.

build_tree(Fd, Size) ->
    Max = 1000,
    lists:foldl(fun(Count, CurTreePos) ->
        RandomMbr = {random:uniform(Max), random:uniform(Max),
                     random:uniform(Max), random:uniform(Max)},
        %io:format("~p~n", [RandomMbr]),
        {ok, _, NewRootPos} = vtree:insert(
            Fd, CurTreePos,
            {RandomMbr, #node{type=leaf},
             list_to_binary("Node" ++ integer_to_list(Count))}),
        %io:format("test_insertion: ~p~n", [NewRootPos]),
        NewRootPos
    end, -1, lists:seq(1,Size)).
