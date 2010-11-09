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

-module(vtreestats).

-export([print/2]).

-record(stats, {
    % number of children in inner nodes
    numinner = [],
    % number of children in leaf nodes
    numleafs = [],
    % depth of the leaf nodes
    depth = []
}).


print(Fd, ParentPos) ->
    Stats = seedtree_init(Fd, ParentPos),
    Inner = Stats#stats.numinner,
    Leafs = Stats#stats.numleafs,
    Depth = Stats#stats.depth,
    io:format("Result: ~w~n", [Stats]),
    io:format("innernodes (~w)~n", [length(Inner)]),
    io:format("  avg (min, max): ~.1f (~w, ~w)~n",
        [lists:sum(Inner)/length(Inner), lists:min(Inner), lists:max(Inner)]),

    io:format("leafs (~w)~n", [length(Leafs)]),
    io:format("  avgnum (min, max): ~.1f (~w, ~w)~n",
        [lists:sum(Leafs)/length(Leafs), lists:min(Leafs), lists:max(Leafs)]),
    io:format("  avgdepth (min, max): ~.1f (~w, ~w)~n",
        [lists:sum(Depth)/length(Depth), lists:min(Depth), lists:max(Depth)]).

seedtree_init(Fd, RootPos) ->
    Stats = seedtree_init(Fd, RootPos, 0, #stats{}),
    Stats#stats{numinner=lists:reverse(Stats#stats.numinner),
            numleafs=lists:reverse(Stats#stats.numleafs),
            depth=lists:reverse(Stats#stats.depth)}.

seedtree_init(Fd, RootPos, Depth, Stats) ->
    {ok, Parent} = couch_file:pread_term(Fd, RootPos),
    {ParentMbr, ParentMeta, EntriesPos} = Parent,

    if
    % leaf node
    is_tuple(hd(EntriesPos)) ->
        Stats#stats{numleafs=[length(EntriesPos)|Stats#stats.numleafs],
                depth=[Depth|Stats#stats.depth]};
    % inner node
    true ->
        Stats4 = lists:foldl(fun(EntryPos, Stats2) ->
            Stats3 = seedtree_init(Fd, EntryPos, Depth+1, Stats2)
        end, Stats, EntriesPos),
        Stats4#stats{numinner=[length(EntriesPos)|Stats4#stats.numinner]}
    end.
