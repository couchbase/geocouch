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

% This module implements the chooose subtree algorithm for the vtree. It is an
% implementation of the choose subtree algorithm described in:
% A Revised R * -tree in Comparison with Related Index Structures
% by Norbert Beckmann, Bernhard Seeger

-module(vtree_choose).

-include("vtree.hrl").

-export([choose_subtree/3]).

-ifdef(makecheck).
-compile(export_all).
-endif.


-spec choose_subtree(Nodes :: [split_node()], NewMbb :: mbb(),
                     Less :: lessfun()) -> split_node().
choose_subtree(Nodes, NewMbb, Less) ->
    % Return all nodes that completely enclose the node that will be inserted,
    % hence don't need any expansion.
    Cov = lists:filter(fun({Mbb, _}) ->
                               vtree_util:within_mbb(NewMbb, Mbb, Less)
                       end, Nodes),
    case Cov of
        % Any node needs to be expanded to include the new one
        [] ->
            Sorted =
                lists:sort(
                  fun({MbbA, _}, {MbbB, _}) ->
                          DeltaA = calc_delta_perimeter(MbbA, NewMbb, Less),
                          DeltaB = calc_delta_perimeter(MbbB, NewMbb, Less),
                          DeltaA =< DeltaB
                  end, Nodes),
            case limit_nodes(Sorted, NewMbb, Less) of
                [] ->
                    hd(Sorted);
                Limited ->
                    process_limited(Limited, NewMbb, Less)
            end;
        % There are nodes that don't need any expension to include the
        % newly added node
        Cov ->
            min_size(Cov)
    end.


-spec limit_nodes(Nodes :: [split_node()], NewMbb :: mbb(), Less :: lessfun())
                 -> [split_node()].
limit_nodes([{FirstMbb, _}|Nodes], NewMbb, Less) ->
    % All perimetric overlaps with first entry as entry the new
    % node will be assigned to.
    OverlapFirst = [{calc_delta_common_overlap_perimeter(
                       FirstMbb, J, NewMbb, Less), Node}
                     || {J, _}=Node <- Nodes],

    % Extract the first p nodes where the last node is the one whose
    % perimetric overlap would increase from the assignment of the new
    % node to the first node. I.e that all nodes from the end of the
    % list will be dropped that don't increase the perimetric overlap.
    Limited = lists:reverse(
                lists:dropwhile(fun({Overlap, _}) ->
                                        Overlap == 0
                                end, lists:reverse(OverlapFirst))),

    % Strip off the overlap and return a plain list of nodes
    [Node || {_, Node} <- Limited].


% Go on with finding an optimum candidate with a limited subset of the nodes
-spec process_limited(Limited :: [split_node()], NewMbb :: mbb(),
                      Less :: lessfun()) -> split_node().
process_limited(Limited, NewMbb, Less) ->
    CheckComp = case any_zero_volume(Limited, NewMbb, Less) of
                    true -> check_comp(perimeter, Limited, NewMbb, Less);
                    false -> check_comp(volume, Limited, NewMbb, Less)
                end,
    case CheckComp of
        {success, FinalNode} ->
            FinalNode;
        % All nodes lead to additional overlap with the other
        % nodes, when the new node is assigned to one of them,
        % hence use the one that leads to the minimum overlap
        {overlap, Candidates} ->
            {_, {_Overlap, FinalNode}} = vtree_util:find_min_value(
                                           fun({Overlap, _}) ->
                                                   Overlap
                                           end, Candidates),
            FinalNode
    end.


% It loops through the `Nodes` to find an ideal candidate, the `NewMbb` can
% be assigned to. Take the node where the assignment of the new MBB leads
% to minimal overlap with the other nodes.
% The whole algorithm gernally follows a depth first approach but adds
% a lot of complexity due to optimizations in case there is no node that
% doesn't lead to increased intersection with other nodes.
% For more details see the corresponding function CheckComp, in Section 3
% of the RR*-tree paper.
-spec check_comp(Aggregate :: perimeter | volume, Nodes :: [split_node()],
                 NewMbb :: mbb(), Less :: lessfun()) ->
                        {overlap, [{number(), split_node()}]} |
                        {success, split_node()}.
check_comp(Aggregate, Nodes, NewMbb, Less) ->
    case check_comp(Aggregate, 1, NewMbb, [], [], Less, Nodes) of
        {overlap, OverlapAcc, _} ->
            {overlap, OverlapAcc};
        {success, _} = Success ->
            Success
    end.
-spec check_comp(Aggregate :: perimeter | volume, Index :: pos_integer(),
                 NewMbb :: mbb(), OverlapAcc :: [{number(), split_node()}],
                 CandAcc :: [pos_integer()], Less :: lessfun(),
                 OrigNodes :: [split_node()]) ->
                        {overlap, [{number(), split_node()}], [pos_integer()]}
                            | {success, split_node()}.
check_comp(Aggregate, Index, NewMbb, OverlapAcc, CandAcc, Less, OrigNodes) ->
    CandAcc2 = [Index|CandAcc],
    T = lists:nth(Index, OrigNodes),
    Result = check_comp0(Aggregate, OrigNodes, 0, 1, Index, NewMbb, OverlapAcc,
                         CandAcc2, Less, T, OrigNodes),
    case Result of
        {overlap, Overlap, OverlapAcc2, CandAcc3} ->
            OverlapAcc3 = [{Overlap, T}|OverlapAcc2],
            {overlap, OverlapAcc3, CandAcc3};
        {success, _} = Success ->
            Success
    end.


% `check_comp0` corresponds to the for-loop in CheckComp, Section 3.

% Loop complete, the total overlap is still 0, hence use this node
-spec check_comp0(Aggregate :: perimeter | volume, Nodes :: [split_node()],
                  Overlap :: number(), Counter :: pos_integer(),
                  Index :: pos_integer(), NewMbb :: mbb(),
                  OverlapAcc :: [{number(), split_node()}],
                  CandAcc :: [pos_integer()], Less :: lessfun(),
                  T :: split_node(), OrigNodes :: [split_node()]) ->
                         {overlap, number(), [{number(), split_node()}],
                          [pos_integer()]} |
                         {success, split_node()}.
check_comp0(_Aggregate, [], Overlap, _Counter, _Index, _NewMbb, _OverlapAcc,
            _CandAcc, _Less, T, _OrigNodes) when Overlap == 0 ->
    {success, T};
% Loop complete, there is some overlap, hence report it back
check_comp0(_Aggregate, [], Overlap, _Counter, _Index, _NewMbb, OverlapAcc,
            CandAcc, _Less, _T, _OrigNodes) ->
    {overlap, Overlap, OverlapAcc, CandAcc};
% Skip the case when the current item would be the one we are currently
% comparing to
check_comp0(Aggregate, [_J|Nodes], Overlap, Counter, Index, NewMbb, OverlapAcc,
            CandAcc, Less, T, OrigNodes) when Index == Counter ->
    check_comp0(Aggregate, Nodes, Overlap, Counter+1, Index, NewMbb, OverlapAcc,
                CandAcc, Less, T, OrigNodes);
% The normal case
check_comp0(Aggregate, [J|Nodes], Overlap, Counter, Index, NewMbb, OverlapAcc,
            CandAcc, Less, T, OrigNodes) ->
    {TMbb, _} = T,
    {JMbb, _} = J,
    NewOverlap = case Aggregate of
                     perimeter -> calc_delta_common_overlap_perimeter(
                                    TMbb, JMbb, NewMbb, Less);
                     volume -> calc_delta_common_overlap_volume(
                                 TMbb, JMbb, NewMbb, Less)
                 end,
    Overlap2 = Overlap + NewOverlap,
    Result = case NewOverlap /= 0 andalso not lists:member(Counter, CandAcc) of
                 true ->
                     check_comp(Aggregate, Counter, NewMbb, OverlapAcc,
                                CandAcc, Less, OrigNodes);
                 false ->
                     % Just keep on looping with the current index
                     {overlap, OverlapAcc, CandAcc}
             end,
    case Result of
        {success, _} = Success ->
            Success;
        {overlap, OverlapAcc2, CandAcc2} ->
            check_comp0(Aggregate, Nodes, Overlap2, Counter+1, Index, NewMbb,
                        OverlapAcc2, CandAcc2, Less, T, OrigNodes)
    end.


% Return true if the assignment of the `NewMbb` to the supplied nodes would
% still lead to at least one zero volume node
-spec any_zero_volume(Nodes :: [split_node()], NewMbb :: mbb(),
                      Less :: lessfun()) -> boolean().
any_zero_volume([], _NewMbb, _Less) ->
    false;
any_zero_volume([{Mbb,_}|T], NewMbb, Less) ->
    Merged = vtree_util:calc_mbb([Mbb, NewMbb], Less),
    Volume = vtree_util:calc_volume(Merged),
    case Volume == 0 of
        true ->
            true;
        _ ->
            any_zero_volume(T, NewMbb, Less)
    end.


% Calculates the delta perimeter of two MBBs, i.e. expension of the perimeter
% when you merge the `NewMbb` with the the ``OriginalMbb`.
-spec calc_delta_perimeter(OriginalMbb :: mbb(), NewMbb :: mbb(),
                           Less :: lessfun()) -> number().
calc_delta_perimeter(OriginalMbb, NewMbb, Less) ->
    Merged = vtree_util:calc_mbb([OriginalMbb, NewMbb], Less),
    vtree_util:calc_perimeter(Merged) - vtree_util:calc_perimeter(OriginalMbb).


% It's the common overlap (by perimeter) of MBB `T` with MBB `J` with the
% newly added MBB `NewMbb`, i.e. the difference between the intersection of
% `J` merged with `NewMbb` and T and the intersection between `J` and `T`.
% See section 3, Definition 1.
-spec calc_delta_common_overlap_perimeter(T :: mbb(), J :: mbb(),
                                          NewMbb :: mbb(), Less :: lessfun())
                                         -> number().
calc_delta_common_overlap_perimeter(T, J, NewMbb, Less) ->
    Merged = vtree_util:calc_mbb([T, NewMbb], Less),
    MergedPerimeter = case vtree_util:intersect_mbb(Merged, J, Less) of
                          overlapfree -> 0;
                          MergedMbb -> vtree_util:calc_perimeter(MergedMbb)
                      end,
    OldPerimeter = case vtree_util:intersect_mbb(T, J, Less) of
                       overlapfree -> 0;
                       OldMbb -> vtree_util:calc_perimeter(OldMbb)
                   end,
    MergedPerimeter - OldPerimeter.


% It's the same as `calc_delta_common_overlap_perimeter`, but with using the
% volume instead of the perimeter.
% See section 3, Definition 1.
-spec calc_delta_common_overlap_volume(T :: mbb(), J :: mbb(),
                                       NewMbb :: mbb(), Less :: lessfun()) ->
                                              number().
calc_delta_common_overlap_volume(T, J, NewMbb, Less) ->
    Merged = vtree_util:calc_mbb([T, NewMbb], Less),
    MergedPerimeter = case vtree_util:intersect_mbb(Merged, J, Less) of
                          overlapfree -> 0;
                          MergedMbb -> vtree_util:calc_volume(MergedMbb)
                      end,
    OldPerimeter = case vtree_util:intersect_mbb(T, J, Less) of
                       overlapfree -> 0;
                       OldMbb -> vtree_util:calc_volume(OldMbb)
                   end,
    MergedPerimeter - OldPerimeter.


% Returns the node (from a list of nodes) that has the least volume, except
% one node has zero volume, then the one with the least perimeter is returned
-spec min_size(Nodes :: [split_node()]) -> split_node().
min_size(Nodes) ->
    {MinVolume, MinNode} = vtree_util:find_min_value(
                             fun({Mbb, _}) ->
                                     vtree_util:calc_volume(Mbb)
                             end, Nodes),
    case MinVolume == 0 of
        % There's at least one node with zero volume, hence use the
        % perimeter instead
        true ->
            {_MinPerim, MinNode2} = vtree_util:find_min_value(
                                      fun({Mbb, _}) ->
                                              vtree_util:calc_perimeter(Mbb)
                                      end, Nodes),
            MinNode2;
        false ->
            MinNode
    end.
