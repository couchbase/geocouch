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

% This module implements the querying of the vtree. It follows the rules of
% the standard R-tree. The only difference is that you can query with multiple
% bounding boxes at the same time. This is useful if you have queries over
% the dateline and want to decompose it into two separate bounding boxes

-module(vtree_search).

-include("vtree.hrl").

-export([search/4, all/3, count_search/2, count_all/1]).

-ifdef(makecheck).
-compile(export_all).
-endif.

-type foldfun() :: fun((#kv_node{} | #kp_node{}, any()) -> {ok | stop, any()}).


-spec search(Vt :: #vtree{}, Boxes :: [mbb()], FoldFun :: foldfun(),
             InitAcc :: any()) -> any().
search(#vtree{root=nil}, _Boxes, _FoldFun, InitAcc) ->
    InitAcc;
search(Vt, Boxes, FoldFun, InitAcc) ->
    {_, Acc} = traverse(Vt, [Vt#vtree.root], Boxes, FoldFun, {ok, InitAcc}),
    Acc.


% No bounding box given, return everything
-spec all(Vt :: #vtree{}, FoldFun :: foldfun(), InitAcc :: any()) -> any().
all(#vtree{root=nil}, _FoldFun, InitAcc) ->
    InitAcc;
all(Vt, FoldFun, InitAcc) ->
    {_, Acc} = traverse_all(Vt, [Vt#vtree.root], FoldFun, {ok, InitAcc}),
    Acc.


% Returns only the number of matching geometries (and not the geometries
% themselves)
-spec count_search(Vt :: #vtree{}, Boxes :: [mbb()]) -> non_neg_integer().
count_search(Vt, Boxes) ->
    search(Vt, Boxes, fun(_Node, Acc) -> {ok, Acc+1} end, 0).


% Returns the number all geometries (and not the geometries themselves)
-spec count_all(Vt :: #vtree{}) -> non_neg_integer().
count_all(Vt) ->
    all(Vt, fun(_Node, Acc) -> {ok, Acc+1} end, 0).


% The accumulator is always a 2-tuple with eith 'ok' or 'stop' and the actual
% value.
-spec traverse(Vt :: #vtree{}, Nodes :: [#kv_node{} | #kp_node{}],
               Boxes :: [mbb()], FoldFun :: foldfun(),
               InitAcc :: {ok |stop, any()}) -> {ok | stop, any()}.
traverse(_Vt, _Nodes, _Boxes, _FoldFun, {stop, Acc}) ->
    {stop, Acc};
traverse(_Vt, [], _Boxes, _FoldFun, OkAcc) ->
    OkAcc;
traverse(Vt, [#kv_node{}|_]=Nodes, Boxes, FoldFun, OkAcc) ->
    traverse_kv(Vt#vtree.less, Nodes, Boxes, FoldFun, OkAcc);
traverse(Vt, [#kp_node{}=Node|Rest], Boxes, FoldFun, OkAcc) ->
    #vtree{
          less = Less,
          fd = Fd
         } = Vt,
    Result = case boxes_intersect_mbb(Boxes, Node#kp_node.key, Less) of
                 [] ->
                    % No box intersects, stop moving deeper
                    OkAcc;
                 IntersectingBoxes ->
                     Children = vtree_io:read_node(
                                  Fd, Node#kp_node.childpointer),
                     % Move deeper
                     traverse(Vt, Children, IntersectingBoxes, FoldFun, OkAcc)
             end,
    % Move sideways
    traverse(Vt, Rest, Boxes, FoldFun, Result).

-spec traverse_kv(Less :: lessfun(), Nodes :: [#kv_node{}], Boxes :: [mbb()],
                  FoldFun :: foldfun(), InitAcc :: {ok |stop, any()}) ->
                         {ok | stop, any()}.
traverse_kv(_Less, _Nodes, _Boxes, _FoldFun, {stop, Acc}) ->
    {stop, Acc};
traverse_kv(_Less, [], _Boxes, _FoldFun, OkAcc) ->
    OkAcc;
traverse_kv(Less, [Node|Rest], Boxes, FoldFun, {ok, Acc}) ->
    Result = case any_box_intersects_mbb(
                    Boxes, Node#kv_node.key, Less) of
                 true ->
                     FoldFun(Node, Acc);
                 false ->
                     {ok, Acc}
             end,
    traverse_kv(Less, Rest, Boxes, FoldFun, Result).


% Traverse the full tree without any bounding box
-spec traverse_all(Vt :: #vtree{}, Nodes :: [#kv_node{} | #kp_node{}],
                   FoldFun :: foldfun(), InitAcc :: {ok |stop, any()}) ->
                          {ok | stop, any()}.
traverse_all(_Vt, _Nodes, _FoldFun, {stop, Acc}) ->
    {stop, Acc};
traverse_all(_Vt, [], _FoldFun, OkAcc) ->
    OkAcc;
traverse_all(_Vt, [#kv_node{}|_]=Nodes, FoldFun, OkAcc) ->
    traverse_all_kv(Nodes, FoldFun, OkAcc);
traverse_all(Vt, [#kp_node{}=Node|Rest], FoldFun, OkAcc) ->
    Children = vtree_io:read_node(Vt#vtree.fd, Node#kp_node.childpointer),
    % Move deeper
    Result = traverse_all(Vt, Children, FoldFun, OkAcc),
    % Move sideways
    traverse_all(Vt, Rest, FoldFun, Result).

-spec traverse_all_kv(Nodes :: [#kv_node{}], FoldFun :: foldfun(),
                      InitAcc :: {ok |stop, any()}) ->
                             {ok | stop, any()}.
traverse_all_kv(_Nodes, _FoldFun, {stop, Acc}) ->
    {stop, Acc};
traverse_all_kv([], _FoldFun, OkAcc) ->
    OkAcc;
traverse_all_kv([#kv_node{}=Node|Rest], FoldFun, {ok, Acc}) ->
    Result = FoldFun(Node, Acc),
    traverse_all_kv(Rest, FoldFun, Result).


% Returns true if any of the boxes intersects the MBB
-spec any_box_intersects_mbb(Boxes :: [mbb()], Mbb :: mbb(),
                             Less :: lessfun()) -> boolean().
any_box_intersects_mbb([], _Mbb, _Less) ->
    false;
any_box_intersects_mbb([Box|Boxes], Mbb, Less) ->
    case vtree_util:intersect_mbb(Box, Mbb, Less) of
        overlapfree ->
            any_box_intersects_mbb(Boxes, Mbb, Less);
        _ -> true
    end.


% Returns all boxes that intersect a given MBB
-spec boxes_intersect_mbb(Boxes :: [mbb()], Mbb :: mbb() | nil,
                          Less :: lessfun()) -> [mbb()].
boxes_intersect_mbb(Boxes, nil, _Less) ->
    Boxes;
boxes_intersect_mbb(Boxes, Mbb, Less) ->
    lists:filter(fun(Box) ->
                         case vtree_util:intersect_mbb(Box, Mbb, Less) of
                             overlapfree -> false;
                             _ -> true
                         end
                 end, Boxes).
