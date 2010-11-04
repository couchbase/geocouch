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

-module(vtreeviz).

-export([visualize/2]).


visualize(Fd, ParentPos) ->
    io:format("digraph G~n{~n    node [shape = record];~n", []),
    print_children(Fd, ParentPos),
    io:format("}~n", []),
    ok.

get_children(Fd, Pos) ->
    {ok, {_RootMbr, _RootMeta, Children}} = couch_file:pread_term(Fd, Pos),
    Children.

print_children(Fd, ParentPos) ->
    ChildrenPos = get_children(Fd, ParentPos),
    ChildrenLabels = if is_integer(hd(ChildrenPos)) ->
        ChildrenMbr = lists:map(fun(ChildPos) ->
            %io:format("ChildPos: ~p~n", [ChildPos]),
            {ok, {Mbr, _Meta, _Children}} = couch_file:pread_term(Fd, ChildPos),
            Mbr
        end, ChildrenPos),
        node_labels(ChildrenPos, ChildrenMbr);
    true ->
        node_labels(ChildrenPos)
    end,
    io:format("node~w [label=\"{~s}\"];~n", [ParentPos, ChildrenLabels]),
    print_edges(Fd, ParentPos, ChildrenPos).

% leaf nodes
node_labels(Children) ->
    string_join("|", Children, fun({Mbr, _, {Id, _Val}}) ->
        io_lib:format("~s ~w", [Id, tuple_to_list(Mbr)])
    end).

% inner nodes
node_labels(ChildrenPos, ChildrenMbr) ->
    Children = lists:zip(ChildrenPos, ChildrenMbr),
    ChildrenLabels = lists:map(fun({ChildPos, ChildMbr}) ->
        io_lib:format("<f~w>~w ~w", [ChildPos, ChildPos, tuple_to_list(ChildMbr)])
    end, Children),
    string_join("|", ChildrenLabels).

print_edges(Fd, ParentPos, Children) ->  
    lists:foreach(fun(ChildPos) ->
        if is_integer(ChildPos) ->
            io:format("node~w:f~w -> node~w~n", [ParentPos, ChildPos, ChildPos]),
            print_children(Fd, ChildPos);
        true ->
            ok
        end
    end, Children).


% From http://www.trapexit.org/String_join_with (2010-03-12)
string_join(Join, L) ->
    string_join(Join, L, fun(E) -> E end).

string_join(_Join, L=[], _Conv) ->
    L;
string_join(Join, [H|Q], Conv) ->
    lists:flatten(lists:concat(
        [Conv(H)|lists:map(fun(E) -> [Join, Conv(E)] end, Q)]
    )).
