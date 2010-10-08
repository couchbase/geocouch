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
    Children = get_children(Fd, ParentPos),
    %io:format("Children: ~p~n", [Children]),
    print_nodes(ParentPos, Children),
    print_edges(Fd, ParentPos, Children).

print_nodes(ParentPos, Children) ->
    ChildrenIds = if is_integer(hd(Children)) ->
        ChildrenLabels = lists:map(fun(ChildPos) ->
            io_lib:format("<f~w>~w foo", [ChildPos, ChildPos])
        end, Children),
        string_join("|", ChildrenLabels);
    true ->
        {_, _, ChildrenLabels} = lists:unzip3(Children),
        string_join("|", ChildrenLabels,
                    fun({Id, Val}) -> io_lib:format("~s", [Id]) end)
    end,
    io:format("node~w [label=\"{~s}\"];~n", [ParentPos, ChildrenIds]).

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
