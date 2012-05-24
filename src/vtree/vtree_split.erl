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

% This module implements a split algorithm for the vtree. It is an
% implementation of the split algorithm described in:
% A Revised R * -tree in Comparison with Related Index Structures
% by Norbert Beckmann, Bernhard Seeger

-module(vtree_split).

-include("vtree.hrl").

-ifdef(makecheck).
-compile(export_all).
-endif.


% Some infos for this module
% ==========================
%
% Nodes
% -----
%
% Nodes are a 2-tuple, containing the MBB and n case of a:
%  1. KV node: the pointer to the node in the file
%  2. KP node: a list of pointers to its children
%
% Candidates
% ----------
%
% A candidate is a Node that is splitted into two partitions. Each partition
% contains a at least some given minimum number and at most some given maximum
% number of Nodes
% So it's: {[some Nodes], [some Nodes]}
%
% Split axis
% ----------
%
% The split axis is a list of candidates. It is the list of candidates, that
% have the overall smallest perimeter. For the calculations on how to get
% there see `split_axis/4`.




% Calculate the split axis. Returns the split candidates with the overall
% minimal perimeter.
% This corresponds to step 1 of 4.1 in the RR*-tree paper
-spec split_axis(Nodes :: [split_node()], FillMin :: integer(),
                 FillMax :: integer(), Less :: lessfun()) -> [candidate()].
split_axis(Nodes, FillMin, FillMax, Less) ->
    NumDims = length(element(1, hd(Nodes))),

    {_, Candidates} =
        lists:foldl(
          fun(Dim, CurMin) ->
                  SortedMin = sort_dim_min(Nodes, Dim, Less),
                  SortedMax = sort_dim_max(Nodes, Dim, Less),
                  Min = candidates_perimeter(SortedMin, FillMin, FillMax,
                                             Less),
                  Max = candidates_perimeter(SortedMax, FillMin, FillMax,
                                             Less),

                  case CurMin of
                      nil -> min_perim([Min, Max]);
                      CurMin -> min_perim([CurMin, Min, Max])
                  end
          end,
          nil, lists:seq(1, NumDims)),
    Candidates.


% chooose_candidate returns the candidate with the minimal value as calculated
% by the goal function. It's the second step of the split algorithm as
% described in section 4.2.4.
% `MbbN` is the bounding box around the nodes that should be split including
% the newly added one.
-spec choose_candidate(Candidates :: [candidate()], Dim :: integer(),
                       MbbO :: mbb(), MbbN :: mbb(), FillMin :: integer(),
                       FillMax :: integer(), Less :: lessfun()) -> candidate().
choose_candidate(Candidates, Dim, MbbO, MbbN, FillMin, FillMax, Less) ->
    PerimMax = perim_max(MbbN),
    Asym = asym(Dim, MbbO, MbbN),
    Wf = make_weighting_fun(Asym, FillMin, FillMax),

    {_, Candidate} = vtree_util:find_min_value(
                       fun(Candidate) ->
                               goal_fun(Candidate, PerimMax, Wf, Less)
                       end, Candidates),
    Candidate.


% This is the goal function "w" as described in section 4.2.4.
% It takes a Candidate, the maximum perimeter of the MBB that also includes
% the to be added node and a less function.
-spec goal_fun(Candidate :: candidate(), PerimMax :: number(), Wf :: fun(),
               Less :: lessfun()) -> number().
goal_fun({F, S}=Candidate, PerimMax, Wf, Less) ->
    MbbF = nodes_mbb(F, Less),
    MbbS = nodes_mbb(S, Less),
    % The index is where the candidates were split
    Index = length(F),

    case vtree_util:intersect_mbb(MbbF, MbbS, Less) of
        overlapfree ->
            wg_overlapfree(Candidate, PerimMax, Less) * Wf(Index);
        _ ->
            wg(Candidate, Less) / Wf(Index)
    end.


% It's the original weighting function "wg" that returns a value for a
% candidate.
% It corresponds to step 2 of 4.1 in the RR*-tree paper, extended by 4.2.4.
-spec wg(Candidate :: candidate(), Less :: lessfun()) -> number().
wg({F, S}, Less) ->
    MbbF = nodes_mbb(F, Less),
    MbbS = nodes_mbb(S, Less),
    OverlapMbb = vtree_util:intersect_mbb(MbbF, MbbS, Less),

    % Check if one of the nodes has no volume (at least one
    % dimension is collapsed to a single point).
    case (vtree_util:calc_volume(MbbF) /= 0) andalso
        (vtree_util:calc_volume(MbbS) /= 0) of
        true ->
            vtree_util:calc_volume(OverlapMbb);
        false ->
            vtree_util:calc_perimeter(OverlapMbb)
    end.


% It's the original weighting function "wg" for the overlap-free case
% It corresponds to step 2 of 4.1 in the RR*-tree paper, extended by 4.2.4.
-spec wg_overlapfree(Candidate :: candidate(), PerimMax :: number(),
                     Less :: lessfun()) -> number().
wg_overlapfree({F, S}, PerimMax, Less) ->
    nodes_perimeter(F, Less) + nodes_perimeter(S, Less) - PerimMax.


-spec make_weighting_fun(Asym :: float(), FillMin :: integer(),
                         FillMax :: integer()) -> fun().
make_weighting_fun(Asym, FillMin, FillMax) ->
    % In thee RR*-tree paper they conclude that the best average performance
    % is achieved with a "S" set to 0.5. Hence it's hard-coded here.
    S = 0.5,
    Mu = (1 - (2*FillMin)/(FillMax+1)) * Asym,
    Sigma = S * (1 + erlang:abs(Mu)),
    Y1 = math:exp(-1/math:pow(S, 2)),
    Ys = 1 / (1-Y1),
    fun(Index) ->
            Xi = ((2*Index) / (FillMax+1)) - 1,
            Exp = math:exp(-math:pow((Xi-Mu)/Sigma, 2)),
            Ys * (Exp - Y1)
    end.


% Sorts the nodes by a certain dimension by the lower value (For example
% the lower value of the y coordinate)
-spec sort_dim_min(Nodes :: [split_node()], Dim :: integer(),
                   Less :: lessfun()) -> [split_node()].
sort_dim_min(Nodes, Dim, Less) ->
    lists:sort(
      fun({MbbA, _}, {MbbB, _}) ->
              {MinA, _} = lists:nth(Dim, MbbA),
              {MinB, _} = lists:nth(Dim, MbbB),
              Less(MinA, MinB) orelse (MinA == MinB)
      end,
      Nodes).

% Sorts the nodes by a certain dimension by the higher value (For example
% the higher value of the y coordinate)
-spec sort_dim_max(Nodes :: [split_node()], Dim :: integer(),
                   Less :: lessfun()) -> [split_node()].
sort_dim_max(Nodes, Dim, Less) ->
    lists:sort(
      fun({MbbA, _}, {MbbB, _}) ->
              {_, MaxA} = lists:nth(Dim, MbbA),
              {_, MaxB} = lists:nth(Dim, MbbB),
              Less(MaxA, MaxB) orelse (MaxA == MaxB)
      end,
      Nodes).


% The maximum perimeter (for definition see proof of Lemma 1, section 4.2.4)
-spec perim_max(Mbb :: mbb()) -> number().
perim_max(Mbb) ->
    MinPerim = lists:min([Max-Min || {Min, Max} <- Mbb]),
    2 * vtree_util:calc_perimeter(Mbb) - MinPerim.


% Create all possible split candidates from a list of nodes
-spec create_split_candidates(Nodes :: [split_node()], FillMin :: integer(),
                              FillMax :: integer()) -> [candidate()].
create_split_candidates(Nodes, FillMin, FillMax) ->
    % FillMax might violate the minimum fill rate condition
    FillMax2 = erlang:min(length(Nodes)-FillMin, FillMax),
    [lists:split(SplitPos, Nodes) || SplitPos <- lists:seq(FillMin, FillMax2)].


% Calculate the enclosing MBB from a list of nodes
-spec nodes_mbb(Nodes :: [split_node()], Less :: lessfun()) -> [mbb()].
nodes_mbb(Nodes, Less) ->
    {Mbbs, _} = lists:unzip(Nodes),
    vtree_util:calc_mbb(Mbbs, Less).


% Calculate the perimeter of the enclosing MBB of some nodes
-spec nodes_perimeter(Nodes :: [split_node()], Less :: lessfun()) -> number().
nodes_perimeter(Nodes, Less) ->
    Mbb = nodes_mbb(Nodes, Less),
    vtree_util:calc_perimeter(Mbb).


% Get the perimeters of all split candidates. Returns a 2-tuple with the
% perimeter and the split candidates
-spec candidates_perimeter(Nodes :: [split_node()], FillMin :: integer(),
                           FillMax :: integer(), Less :: lessfun()) ->
                                  {number(), [candidate()]}.
candidates_perimeter(Nodes, FillMin, FillMax, Less) ->
    Candidates = create_split_candidates(Nodes, FillMin, FillMax),
    Perims = [nodes_perimeter(F, Less) + nodes_perimeter(S, Less) ||
                 {F, S} <- Candidates],
    Perim = lists:sum(Perims),
    {Perim, Candidates}.


% Input is a list of 2-tuples that contain the perimeter as first element.
% Return the 2-tuple that contains the minimum perimeter.
-spec min_perim([{number(), any()}]) -> {number(), any()}.
min_perim([H|T]) ->
    min_perim(T, H).
-spec min_perim([{number(), any()}], {number(), any()}) -> {number(), any()}.
min_perim([], Min) ->
    Min;
min_perim([{Perim, _}=H|T], {MinPerim, _}) when Perim < MinPerim ->
    min_perim(T, H);
min_perim([_|T], Min) ->
    min_perim(T, Min).


% Returns the asym for a certain dimension
-spec asym(Dim :: integer(), MbbO :: mbb(), MbbN :: mbb()) -> number().
asym(Dim, MbbO, MbbN) ->
    LengthN = mbb_dim_length(Dim, MbbN),
    CenterN = mbb_dim_center(Dim, MbbN),
    CenterO = mbb_dim_center(Dim, MbbO),
    ((2*(CenterN - CenterO)) / LengthN).


% Returns the length of a certain dimension of an MBB
-spec mbb_dim_length(Dim :: integer(), Mbb :: mbb()) -> number().
mbb_dim_length(Dim, Mbb) ->
    {Min, Max} = lists:nth(Dim, Mbb),
    Max - Min.


% Returns the center of a certain dimension of an MBB
-spec mbb_dim_center(Dim :: integer(), Mbb :: mbb()) -> number().
mbb_dim_center(Dim, Mbb) ->
    {Min, Max} = lists:nth(Dim, Mbb),
    Min + ((Max - Min)/2).
