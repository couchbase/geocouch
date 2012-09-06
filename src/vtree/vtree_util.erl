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
         calc_volume/1, intersect_mbb/3, find_min_value/2]).


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


% Returns the intersection of two MBBs
-spec intersect_mbb(A :: mbb(), B :: mbb(), Less :: lessfun()) ->
                           mbb() | overlapfree.
intersect_mbb(A, B, Less) ->
    intersect_mbb0(lists:zip(A, B), Less, []).
-spec intersect_mbb0([{{keyval(), keyval()}, {keyval(), keyval()}}],
                     Less :: lessfun(), Acc :: mbb()) -> mbb() | overlapfree.
intersect_mbb0([], _Less, Acc) ->
    lists:reverse(Acc);
intersect_mbb0([{{MinA, MaxA}, {MinB, MaxB}}|T], Less, Acc) ->
    Min = vtree_util:max({MinA, MinB}, Less),
    Max = vtree_util:min({MaxA, MaxB}, Less),

    case {Min, Max} of
        {Min, Max} when Min > Max -> overlapfree;
        {Min, Max} when Min < Max -> intersect_mbb0(T, Less, [{Min, Max}|Acc]);
        % The MBBs either touch eachother, or one has zero length
        {Min, Max} when Min == Max ->
            case (MinA /= MaxB) and (MaxA /= MinB) of
                % The MBBs don't touch eachother
                true -> intersect_mbb0(T, Less, [{Min, Max}|Acc]);
                % The Mbbs touch eachother
                false -> overlapfree
            end
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
