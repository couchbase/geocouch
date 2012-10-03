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

-export([search/2]).

-ifdef(makecheck).
-compile(export_all).
-endif.


-spec search(Vt :: #vtree{}, Boxes :: [mbb()]) -> [#kv_node{}].
search(#vtree{root=nil}, _Boxes) ->
    [];
search(Vt, Boxes) ->
    traverse(Vt, [Vt#vtree.root], Boxes, []).


-spec traverse(Vt :: #vtree{}, Nodes :: [#kv_node{} | #kp_node{}],
               Boxes :: [mbb()], Acc :: [#kv_node{}]) -> [#kv_node{}].
traverse(_Vt, [], _Boxes, Acc) ->
    Acc;
traverse(Vt, [#kv_node{}|_]=Nodes, Boxes, _Acc) ->
    lists:filter(fun(Node) ->
                         any_box_intersects_mbb(
                           Boxes, Node#kv_node.key, Vt#vtree.less)
                 end, Nodes);
traverse(Vt, [#kp_node{}=Node|Rest], Boxes, Acc) ->
    #vtree{
          less = Less,
          fd = Fd
         } = Vt,
    IntersectingBoxes = boxes_intersect_mbb(Boxes, Node#kp_node.key, Less),
    ChildResults = case IntersectingBoxes of
                       % No box intersects, stop moving deeper
                       [] -> [];
                       IntersectingBoxes ->
                           Children = vtree_io:read_node(
                                        Fd, Node#kp_node.childpointer),
                           % Move deeper
                           traverse(Vt, Children, IntersectingBoxes, Acc)
                   end,
    % Move sideways
    Result = traverse(Vt, Rest, Boxes, Acc),
    ChildResults ++ Result.


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
-spec boxes_intersect_mbb(Boxes :: [mbb()], Mbb :: mbb(),
                          Less :: lessfun()) -> [mbb()].
boxes_intersect_mbb(Boxes, Mbb, Less) ->
    lists:filter(fun(Box) ->
                         case vtree_util:intersect_mbb(Box, Mbb, Less) of
                             overlapfree -> false;
                             _ -> true
                         end
                 end, Boxes).
