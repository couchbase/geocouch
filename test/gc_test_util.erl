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

-module(gc_test_util).

-export([init_code_path/0, random_node/0, random_node/1, build_random_tree/2,
    lookup/3, root_dir/0, gc_config_file/0]).

-record(node, {
    % type = inner | leaf
    type = inner
}).

% @doc The location of the GeoCouch config file relative to the root directory
-spec gc_config_file() -> string().
gc_config_file() ->
    "etc/couchdb/default.d/geocouch.ini".

init_code_path() ->
    BuildDir = filename:join(root_dir() ++ ["build"]),
    code:add_pathz(BuildDir).

% @doc Returns the root directory of GeoCouch as a list. It makes the
% assumptions that the currently running test is in <rootdir>/test/thetest.t
-spec root_dir() -> [file:filename()].
root_dir() ->
    EscriptName = filename:split(filename:absname(escript:script_name())),
    lists:sublist(EscriptName, length(EscriptName)-2).

% @doc Create a random node. Return the ID of the node and the node itself.
-spec random_node() -> {string(), tuple()}.
random_node() ->
    random_node({654, 642, 698}).
-spec random_node(Seed::{integer(), integer(), integer()}) -> {string(), tuple()}.
random_node(Seed) ->
    random:seed(Seed),
    Max = 1000,
    {W, X, Y, Z} = {random:uniform(Max), random:uniform(Max),
                    random:uniform(Max), random:uniform(Max)},
    RandomMbr = {erlang:min(W, X), erlang:min(Y, Z),
                 erlang:max(W, X), erlang:max(Y, Z)},
    RandomLineString = {linestring, [[erlang:min(W, X), erlang:min(Y, Z)],
        [erlang:max(W, X), erlang:max(Y, Z)]]},
    {list_to_binary("Node" ++ integer_to_list(Y) ++ integer_to_list(Z)),
     {RandomMbr, #node{type=leaf}, RandomLineString,
      list_to_binary("Value" ++ integer_to_list(Y) ++ integer_to_list(Z))}}.

-spec build_random_tree(Filename::string(), Num::integer()) ->
        {ok, {file:io_device(), {integer(), integer()}}} | {error, string()}.
build_random_tree(Filename, Num) ->
    % The random seed generator changed in R15 (erts 5.9)
    case erlang:system_info(version) >= "5.9" of
        true -> build_random_tree(Filename, Num, {86880, 81598, 91188});
        false -> build_random_tree(Filename, Num, {654, 642, 698})
    end.
-spec build_random_tree(Filename::string(), Num::integer(),
        Seed::{integer(), integer(), integer()}) ->
        {ok, {file:io_device(), {integer(), integer()}}} | {error, string()}.
build_random_tree(Filename, Num, Seed) ->
    random:seed(Seed),
    case couch_file:open(Filename, [create, overwrite]) of
    {ok, Fd} ->
        Max = 1000,
        {Tree, TreeHeight} = lists:foldl(
            fun(Count, {CurTreePos, _CurTreeHeight}) ->
                {W, X, Y, Z} = {random:uniform(Max), random:uniform(Max),
                                random:uniform(Max), random:uniform(Max)},
                RandomMbr = {erlang:min(W, X), erlang:min(Y, Z),
                             erlang:max(W, X), erlang:max(Y, Z)},
                RandomLineString = {linestring,
                    [[erlang:min(W, X), erlang:min(Y, Z)],
                    [erlang:max(W, X), erlang:max(Y, Z)]]},
                %io:format("~p~n", [RandomMbr]),
                {ok, _, NewRootPos, NewTreeHeight} = vtree:insert(
                    Fd, CurTreePos,
                    list_to_binary("Node" ++ integer_to_list(Count)),
                    {RandomMbr, #node{type=leaf}, RandomLineString,
                     list_to_binary("Node" ++ integer_to_list(Count))}),
                %io:format("test_insertion: ~p~n", [NewRootPos]),
                {NewRootPos, NewTreeHeight}
            end, {nil, 0}, lists:seq(1,Num)),
        %io:format("Tree: ~p~n", [Tree]),
        {ok, {Fd, {Tree, TreeHeight}}};
    {error, _Reason} ->
        io:format("ERROR: Couldn't open file (~s) for tree storage~n",
                  [Filename])
    end.


lookup(Fd, Pos, Bbox) ->
    % default function returns a list of 2-tuple with
    %  - 2-tuple with MBR and document ID
    %  - 2-tuple with the geometry and the actual value
    vtree:lookup(Fd, Pos, Bbox, {fun({{Bbox2, DocId}, {Geom, Value}}, Acc) ->
         % NOTE vmx (2011-02-09) This should perhaps also be changed from
         %     {Bbox2, DocId, Geom, Value} to {{Bbox2, DocId}, {Geom, Value}}
         Acc2 = [{Bbox2, DocId, Geom, Value}|Acc],
         {ok, Acc2}
    end, []}, nil).
