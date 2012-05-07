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

-ifdef(makecheck).
-compile(export_all).
-endif.

% The value a key can have. For the vtree that is either a number or
% (UTF-8) string
-type keyval() :: number() | string().

% The multidimensional bounding box
-type mbb() :: [{Min :: keyval(), Max :: keyval()}] | [].
% The node format for the splits. It contains the MBB and in case of a:
%  1. KV node: the pointer to the node in the file
%  2. KP node: a list of pointers to its children
-type split_node() :: {Mbb :: mbb(),
                       KvPosOrChildren :: integer() | [integer()]}.

-type candidate() :: {[split_node()], [split_node()]}.

% The less function compares two values and returns true if the former is
% less than the latter
-type lessfun() :: fun((keyval(), keyval()) -> boolean()).

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
% there see `split_axis/3`.




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


% This corresponds to step 2 of 4.1 in the RR*-tree paper
-spec choose_candidate(Candidates::[candidate()], Less :: lessfun()) ->
                              candidate().
choose_candidate(Candidates, Less) ->
    case overlapfree_candidates(Candidates, Less) of
        [] ->
            % Check if one of the nodes has no volume (at least one dimension
            % is collapsed to a single point).
            {F, S} = hd(Candidates),
            MbbF = nodes_mbb(F, Less),
            MbbS = nodes_mbb(S, Less),
            case (calc_volume(MbbF) =/= 0) and (calc_volume(MbbS) =/= 0) of
                true ->
                    min_volume_overlap_candidate(Candidates, Less);
                false ->
                    min_perimeter_overlap_candidate(Candidates, Less)
            end;
        OfCandidates ->
            min_perimeter_candidate(OfCandidates, Less)
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
              Less(MinA, MinB) orelse (MinA =:= MinB)
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
              Less(MaxA, MaxB) orelse (MaxA =:= MaxB)
      end,
      Nodes).

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
    calc_perimeter(Mbb).


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
% Return the 2-tuple that contains the minumum perimeter.
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
        {Min, Max} when Min =:= Max ->
            case (MinA =/= MaxB) and (MaxA =/= MinB) of
                % The MBBs don't touch eachother
                true -> intersect_mbb0(T, Less, [{Min, Max}|Acc]);
                % The Mbbs touch eachother
                false -> overlapfree
            end
    end.


% Returns all overlap-free candidates. Overlap-free means the the MBBs of
% the two partitions of the candidate don't intersect eachother.
-spec overlapfree_candidates(Candidates :: [candidate()], Less :: lessfun()) ->
                                    [candidate()].
overlapfree_candidates(Candidates, Less) ->
    lists:filter(fun({F, S}) ->
                         MbbF = nodes_mbb(F, Less),
                         MbbS = nodes_mbb(S, Less),
                         case intersect_mbb(MbbF, MbbS, Less) of
                             overlapfree -> true;
                             _ -> false
                         end
                 end, Candidates).


% `find_min_candidate` returns the split candidate with the minumum value.
% The `MinFun` is a function that gets a candidate and returns the
% value that should be minimized.
-spec find_min_candidate(MinFun :: fun(), Candidates :: [candidate()]) ->
                                candidate().
find_min_candidate(MinFun, Candidates) ->
    {_, Candidate} = lists:foldl(
                       fun(Candidate, {MinVal, _MinCandidate}=Acc) ->
                               Val = MinFun(Candidate),
                               case (Val < MinVal) orelse (MinVal =:= nil) of
                                   true -> {Val, Candidate};
                                   false -> Acc
                               end
                       end,
                       {nil, []}, Candidates),
    Candidate.


% Returns the candidate with the minimum perimeter.
% The minimum perimeter of a candidate is the perimeter of sum of the
% perimeters of the enclosing mbb of each partition.
-spec min_perimeter_candidate(Candidates :: [candidate()],
                              Less :: lessfun()) -> candidate().
min_perimeter_candidate(Candidates, Less) ->
    find_min_candidate(
      fun({F, S}) ->
              nodes_perimeter(F, Less) + nodes_perimeter(S, Less)
      end, Candidates).


% All input Candidates must be non overlap-free
% Returns the candidate with the overlap with the minimum volume
-spec min_volume_overlap_candidate(Candidates :: [candidate()],
                                   Less :: lessfun()) -> candidate().
min_volume_overlap_candidate(Candidates, Less) ->
    find_min_candidate(
      fun({F, S}) ->
              MbbF = nodes_mbb(F, Less),
              MbbS = nodes_mbb(S, Less),
              IntersectedMbb = intersect_mbb(MbbF, MbbS, Less),
              calc_volume(IntersectedMbb)
      end, Candidates).


% All input Candidates must be non overlap-free
% Returns the candidate with the overlap with the minimum perimeter.
-spec min_perimeter_overlap_candidate(Candidates :: [candidate()],
                                      Less :: lessfun()) -> candidate().
min_perimeter_overlap_candidate(Candidates, Less) ->
    find_min_candidate(
      fun({F, S}) ->
              MbbF = nodes_mbb(F, Less),
              MbbS = nodes_mbb(S, Less),
              IntersectedMbb = intersect_mbb(MbbF, MbbS, Less),
              calc_perimeter(IntersectedMbb)
      end, Candidates).


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


-spec mbb_dim_center(Dim :: integer(), Mbb :: mbb()) -> number().
mbb_dim_center(Dim, Mbb) ->
    {Min, Max} = lists:nth(Dim, Mbb),
    Min + ((Max - Min)/2).
