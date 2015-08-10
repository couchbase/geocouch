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
% couch_db.hrl is only included to have log messages
-include("couch_db.hrl").

-export([split_inner/5, split_leaf/5]).

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




% The full split algorithm for an inner node. All dimensions are
% taken into account. The best split candidate is returned.
-spec split_inner(Nodes :: [split_node()], Mbb0 :: mbb(),
                  FillMin :: number(), FillMax :: number(),
                  Less :: lessfun()) -> candidate().
split_inner(Nodes, MbbO, FillMin, FillMax, Less) ->
    NumDims = length(element(1, hd(Nodes))),
    MbbN = vtree_util:nodes_mbb(Nodes, Less),

    {_, Candidate} =
        lists:foldl(
          % Loop through every dimension to find the split with the
          % minimal cost
          fun(Dim, {MinVal, _}=Acc) ->
                  SortedMin = sort_dim_min(Nodes, Dim, Less),
                  SortedMax = sort_dim_max(Nodes, Dim, Less),
                  CandidatesMin = create_split_candidates(SortedMin, FillMin,
                                                          FillMax),
                  CandidatesMax = create_split_candidates(SortedMax, FillMin,
                                                          FillMax),

                  {Val, Candidate} = choose_candidate(
                                       CandidatesMin ++ CandidatesMax, Dim,
                                       MbbO, MbbN, FillMin, Less),
                  case Val < MinVal of
                      true -> {Val, Candidate};
                      false -> Acc
                  end
          end,
          {nil, nil}, lists:seq(1, NumDims)),
    Candidate.


% The full split algorithm for a leaf node. Only the dimension with the
% minumum perimeter is taken into account. The best split candidate is
% returned.
-spec split_leaf(Nodes :: [split_node()], Mbb0 :: mbb(),
                 FillMin :: number(), FillMax :: number(),
                 Less :: lessfun()) -> candidate().
split_leaf(Nodes, MbbO, FillMin, FillMax, Less) ->
    {Dim, Candidates} = split_axis(Nodes, FillMin, FillMax, Less),
    MbbN = vtree_util:nodes_mbb(Nodes, Less),
    {_, Candidate} = choose_candidate(Candidates, Dim, MbbO, MbbN, FillMin,
                                      Less),
    Candidate.


% Calculate the split axis. Returns the dimension of the split candidates
% with the overall minimal perimeter and the candidates themselves.
% This corresponds to step 1 of 4.1 in the RR*-tree paper
-spec split_axis(Nodes :: [split_node()], FillMin :: pos_integer(),
                 FillMax :: pos_integer(), Less :: lessfun()) ->
                        {integer(), [candidate()]}.
split_axis(Nodes, FillMin, FillMax, Less) ->
    NumDims = length(element(1, hd(Nodes))),

    {Dim, {_MinPerim, Candidates}} =
        lists:foldl(
          fun(Dim, {_, CurMin}) ->
                  SortedMin = sort_dim_min(Nodes, Dim, Less),
                  SortedMax = sort_dim_max(Nodes, Dim, Less),
                  Min = candidates_perimeter(SortedMin, FillMin, FillMax,
                                             Less),
                  Max = candidates_perimeter(SortedMax, FillMin, FillMax,
                                             Less),

                  NewMin = case CurMin of
                      nil -> min_perim([Min, Max]);
                      CurMin -> min_perim([CurMin, Min, Max])
                  end,
                  {Dim, NewMin}
          end,
          {1, nil}, lists:seq(1, NumDims)),
    {Dim, Candidates}.


% chooose_candidate returns the candidate with the minimal value as calculated
% by the goal function. It's the second step of the split algorithm as
% described in section 4.2.4.
% `MbbN` is the bounding box around the nodes that should be split including
% the newly added one.
-spec choose_candidate(Candidates :: [candidate()], Dim :: integer(),
                       MbbO :: mbb(), MbbN :: mbb(), FillMin :: number(),
                       Less :: lessfun()) ->
                              {number(), candidate()}.
choose_candidate([{F, S}|_]=Candidates, Dim, MbbO, MbbN, FillMin, Less) ->
    CandidateSize = ?ext_size([Node || {_, Node} <- F]) +
        ?ext_size([Node || {_, Node} <- S]),

    PerimMax = perim_max(MbbN),
    Asym = asym(Dim, MbbO, MbbN),
    Wf = make_weighting_fun(Asym, FillMin, CandidateSize),

    vtree_util:find_min_value(
      fun(Candidate) ->
              goal_fun(Candidate, PerimMax, Wf, Less)
      end, Candidates).


% This is the goal function "w" as described in section 4.2.4.
% It takes a Candidate, the maximum perimeter of the MBB that also includes
% the to be added node and a less function.
-spec goal_fun(Candidate :: candidate(), PerimMax :: number(), Wf :: fun(),
               Less :: lessfun()) -> number().
goal_fun({F, S}=Candidate, PerimMax, Wf, Less) ->
    MbbF = vtree_util:nodes_mbb(F, Less),
    MbbS = vtree_util:nodes_mbb(S, Less),
    % The `Offset` is bytes offset where the candidates were split
    Offset = ?ext_size([Node || {_, Node} <- F]),

    case vtree_util:intersect_mbb(MbbF, MbbS, Less) of
        overlapfree ->
            wg_overlapfree(Candidate, PerimMax, Less) * Wf(Offset);
        _ ->
            wg(Candidate, Less) / Wf(Offset)
    end.


% It's the original weighting function "wg" that returns a value for a
% candidate.
% It corresponds to step 2 of 4.1 in the RR*-tree paper, extended by 4.2.4.
-spec wg(Candidate :: candidate(), Less :: lessfun()) -> number().
wg({F, S}, Less) ->
    MbbF = vtree_util:nodes_mbb(F, Less),
    MbbS = vtree_util:nodes_mbb(S, Less),
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


-spec make_weighting_fun(Asym :: float(), FillMin :: number(),
                         MaxSize :: pos_integer()) -> fun().
make_weighting_fun(Asym, FillMin, MaxSize) ->
    % In thee RR*-tree paper they conclude that the best average performance
    % is achieved with a "S" set to 0.5. Hence it's hard-coded here.
    S = 0.5,
    % In the RR*-tree paper Mu is calcluated with the maximum fill size + 1
    % (which corresponds to an overflowing node). As we don't use the number
    % of nodes, but the byte size as thresholds to determine how many children
    % a node should/can hold, we use the total size of a single split
    % candidate instead.
    Mu = (1 - (2*FillMin)/MaxSize) * Asym,
    Sigma = S * (1 + erlang:abs(Mu)),
    Y1 = math:exp(-1/math:pow(S, 2)),
    Ys = 1 / (1-Y1),
    % In the RR*-tree paper the position of the node within the node list
    % is used. As we use the byte size of the nodes as thresholds, use the
    % byte offset of the split location instead.
    fun(Offset) ->
            % In the RR*-tree paper Xi is calculate with the maximum
            % fill size + 1, use again the maximum byte size (see the
            % comments above for the reasoning.
            Xi = ((2*Offset) / MaxSize) - 1,
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
-spec create_split_candidates(Nodes :: [split_node()],
                              FillMin :: number(),
                              FillMax :: number()) -> [candidate()].
create_split_candidates([H|T], FillMin, FillMax) ->
    create_split_candidates([H], T, FillMin, FillMax, []).

-spec create_split_candidates(A :: [split_node()], B :: [split_node()],
                              FillMin :: number(), FillMax :: number(),
                              [candidate()]) -> [candidate()].
% The minimum fill rate was already relaxed (see below) and there's still
% no split candidate. The reason is probably that any of the nodes is
% bigger in size (bytes) than the maximum chunk threshold.
% Instead of making it a fatal failure, create a single split candidate,
% where the first partition contains as many nodes as possible until the
% maximum threshold is overcome. This means the the maximum threshold
% guarantee will be violated, but that's better than a fatal error.
create_split_candidates(A, [], 0, FillMax, []) ->
    ?LOG_ERROR("Warning: there is a node that is bigger than the maximum "
               "chunk threshold (~p). Increase the chunk threshold.",
               [FillMax]),
    case vtree_modify:get_overflowing_subset(FillMax, A) of
        % The very last node lead to the overflow, hence the second partition
        % is empty, but split candidates must be divided into two partitions
        % that contain at least one item each.
        {A, []} ->
            [lists:split(length(A) - 1, A)];
        Else ->
            [Else]
    end;
% No valid split candidates were found. Instead of returning an error, we
% relax the minimum filled condition to zero. This case should rarely happen
% (only in very extreme cases). For example if you have two nodes, one with a
% very large byte size, the other one very small. There the minimum fill rate
% can't be satisfied easily and we would end up without a candidate at all.
create_split_candidates(A, [], _, FillMax, []) ->
    ?LOG_INFO("Relax the minimum fill rate condition in order to find a "
              "split candidate", []),
    create_split_candidates(A, 0, FillMax);
create_split_candidates(_, [], _, _, Candidates) ->
    lists:reverse(Candidates);
create_split_candidates(A, [HeadB|RestB]=B, FillMin, FillMax, Candidates0) ->
    % Use the sizes of the actual nodes
    SizeA = ?ext_size([Node || {_, Node} <- A]),
    SizeB = ?ext_size([Node || {_, Node} <- B]),
    Candidates =
        case (SizeA >= FillMin andalso SizeA =< FillMax) andalso
            (SizeB >= FillMin andalso SizeB =< FillMax) of
            true ->
                [{A, B}|Candidates0];
            false ->
                Candidates0
        end,
    create_split_candidates(A ++ [HeadB], RestB, FillMin, FillMax, Candidates).


% Calculate the perimeter of the enclosing MBB of some nodes
-spec nodes_perimeter(Nodes :: [split_node()], Less :: lessfun()) -> number().
nodes_perimeter(Nodes, Less) ->
    Mbb = vtree_util:nodes_mbb(Nodes, Less),
    vtree_util:calc_perimeter(Mbb).


% Get the perimeters of all split candidates. Returns a 2-tuple with the
% perimeter and the split candidates
-spec candidates_perimeter(Nodes :: [split_node()], FillMin :: number(),
                           FillMax :: number(), Less :: lessfun()) ->
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
    case LengthN == 0 of
        true ->
            0;
        false ->
            CenterN = mbb_dim_center(Dim, MbbN),
            CenterO = mbb_dim_center(Dim, MbbO),
            ((2*(CenterN - CenterO)) / LengthN)
    end.


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
