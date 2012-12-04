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

-module(vtree_util).

-include("vtree.hrl").

-export([calc_mbb/2, nodes_mbb/2, min/2, max/2, calc_perimeter/1,
         calc_volume/1, intersect_mbb/3, find_min_value/2, within_mbb/3]).


-spec min(Tuple::{any(), any()} | [any()], Less::fun()) -> Min::any().
min({A, B}, Less) ->
    case Less(A, B) of
        true -> A;
        false -> B
    end;
min([H|T], Less) ->
    min(T, Less, H).
-spec min(List::[any()], Less::fun(), Min::any()) -> Min::any().
min([], _Less, Min) ->
    Min;
min([H|T], Less, Min) ->
    Min2 = case Less(H, Min) of
               true -> H;
               false -> Min
           end,
    min(T, Less, Min2).

-spec max(Tuple::{any(), any()} | [any()], Less::fun()) -> Max::any().
max({A, B}, Less) ->
    case Less(A, B) of
        true -> B;
        false -> A
    end;
max([H|T], Less) ->
    max(T, Less, H).
-spec max(List::[any()], Less::fun(), Max::any()) -> Max::any().
max([], _Less, Max) ->
    Max;
max([H|T], Less, Max) ->
    Max2 = case Less(H, Max) of
               true -> Max;
               false -> H
           end,
    max(T, Less, Max2).


% Calculate the perimeter of list of 2-tuples that contain a min and a max
% value
-spec calc_perimeter(mbb()) -> number().
calc_perimeter(Values) ->
    lists:foldl(fun({Min, Max}, Acc) ->
                        Acc + (Max-Min)
                end, 0, Values).


% Calculate the volume of list of 2-tuples that contain a min and a max
% value
-spec calc_volume(mbb()) -> number().
calc_volume(Values) ->
    lists:foldl(fun({Min, Max}, Acc) ->
                        Acc * (Max-Min)
                end, 1, Values).


% Calculate the enclosing bounding box from a list of bounding boxes
-spec calc_mbb(List::[[{any(), any()}]], Less::fun()) -> [{any(), any()}].
calc_mbb([H|T], Less) ->
    calc_mbb(T, Less, H).
-spec calc_mbb(List::[[{any(), any()}]], Less::fun(),  Mbb::[{any(), any()}])
              -> Mbb::[{any(), any()}].
calc_mbb([], _Less, Mbb) ->
    Mbb;
calc_mbb([H|T], Less, Mbb) ->
    Mbb2 = lists:map(
             fun({{Min, Max}, {MinMbb, MaxMbb}}) ->
                     {?MODULE:min({Min, MinMbb}, Less),
                      ?MODULE:max({Max, MaxMbb}, Less)}
             end, lists:zip(H, Mbb)),
    calc_mbb(T, Less, Mbb2).


% Calculate the enclosing MBB from a list of nodes
-spec nodes_mbb(Nodes :: [#kv_node{} | #kp_node{} | split_node()],
                Less :: lessfun()) -> mbb().
nodes_mbb([#kv_node{}|_]=Nodes, Less) ->
    Mbbs = [Node#kv_node.key || Node <- Nodes],
    vtree_util:calc_mbb(Mbbs, Less);
nodes_mbb([#kp_node{}|_]=Nodes, Less) ->
    Mbbs = [Node#kp_node.key || Node <- Nodes],
    vtree_util:calc_mbb(Mbbs, Less);
nodes_mbb(Nodes, Less) ->
    {Mbbs, _} = lists:unzip(Nodes),
    vtree_util:calc_mbb(Mbbs, Less).


% Returns the intersection of two MBBs. Touching also counts as intersection.
-spec intersect_mbb(A :: mbb(), B :: mbb(), Less :: lessfun()) ->
                           mbb() | overlapfree.
intersect_mbb(A, B, Less) ->
    intersect_mbb0(lists:zip(A, B), Less, []).
-spec intersect_mbb0([{{keyval(), keyval()}, {keyval(), keyval()}}],
                     Less :: lessfun(), Acc :: mbb()) -> mbb() | overlapfree.
intersect_mbb0([], _Less, Acc) ->
    lists:reverse(Acc);
% If both values are `nil`, it is like a wildcard, it covers the other
% range completely
intersect_mbb0([{{nil, nil}, MinMax}|T], Less, Acc) ->
    intersect_mbb0(T, Less, [MinMax|Acc]);
intersect_mbb0([{MinMax, {nil, nil}}|T], Less, Acc) ->
    intersect_mbb0(T, Less, [MinMax|Acc]);
% If one end of the range is `nil` it's an open range and the intersection
% will be the one of the other given range. The guards are needed to prevent
% endless loops.
intersect_mbb0([{{nil, MaxA}, {MinB, MaxB}}|T], Less, Acc) when MinB =/= nil ->
    intersect_mbb0([{{MinB, MaxA}, {MinB, MaxB}}|T], Less, Acc);
intersect_mbb0([{{MinA, nil}, {MinB, MaxB}}|T], Less, Acc) when MaxB =/= nil ->
    intersect_mbb0([{{MinA, MaxB}, {MinB, MaxB}}|T], Less, Acc);
intersect_mbb0([{{MinA, MaxA}, {nil, MaxB}}|T], Less, Acc) when MinA =/= nil ->
    intersect_mbb0([{{MinA, MaxA}, {MinA, MaxB}}|T], Less, Acc);
intersect_mbb0([{{MinA, MaxA}, {MinB, nil}}|T], Less, Acc) when MaxA =/= nil ->
    intersect_mbb0([{{MinA, MaxA}, {MinB, MaxA}}|T], Less, Acc);
% All `nil` cases are resolved, do the actual work
intersect_mbb0([{{MinA, MaxA}, {MinB, MaxB}}|T], Less, Acc) ->
    Min = vtree_util:max({MinA, MinB}, Less),
    Max = vtree_util:min({MaxA, MaxB}, Less),

    case Less(Max, Min) of
        true -> overlapfree;
        false -> intersect_mbb0(T, Less, [{Min, Max}|Acc])
    end.


% Find the minumum value from a list of things. One item from the
% list will be passed into the the suppplied `MinFun`, which returns
% some numner. The lowest number together with the corresponding item
% will be returned.
-spec find_min_value(MinFun :: fun(), Data :: [any()]) -> {number(), any()}.
find_min_value(MinFun, [_|_]=Data) ->
    lists:foldl(
      fun(Item, {MinVal, _}=Acc) ->
              Val = MinFun(Item),
              case (Val < MinVal) orelse (MinVal =:= nil) of
                  true -> {Val, Item};
                  false -> Acc
              end
      end,
      {nil, []}, Data).


% Returns true if MBB `A` is completely within `B`, i.E. merging `A` with `B`
% wouldn't expand `B` and just return `B`.
-spec within_mbb(A :: mbb(), B :: mbb(), Less :: lessfun()) -> true | false.
within_mbb(A, B, Less) ->
    within_mbb0(lists:zip(A, B), Less).
-spec within_mbb0([{{keyval(), keyval()}, {keyval(), keyval()}}],
                  Less :: lessfun()) -> true | false.
within_mbb0([], _Less) ->
    true;
within_mbb0([{{MinA, MaxA}, {MinB, MaxB}}|T], Less) ->
    case Less(MinA, MinB) orelse Less(MaxB, MaxA) of
        true ->
            false;
        _ ->
            within_mbb0(T, Less)
    end.
