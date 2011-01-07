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

-module(vtree).

-export([lookup/3, lookup/4, within/2, intersect/2, disjoint/2, insert/4,
         area/1, merge_mbr/2, find_area_min_nth/1, partition_node/1,
         calc_nodes_mbr/1, calc_mbr/1, best_split/1, minimal_overlap/2,
         calc_overlap/2, minimal_coverage/2, delete/4, add_remove/5,
         split_flipped_bbox/2, count_lookup/3, split_node/1]).

-export([get_node/2]).
-export([foldl_stop/3]).

% TODO vmx: Function parameters order is inconsitent between insert and delete.



% The bounding box coordinate order follows the GeoJSON specification (http://geojson.org/): {x-low, y-low, x-high, y-high}

% Design question: Should not fully filled nodes have only as many members as nodes, or be filled up with nils to their maximum number of nodes? - Current implementation is the first one (dynamic number of members).

% Nodes maximum/minimum filling grade (TODO vmx: shouldn't be hard-coded)
-define(MAX_FILLED, 40).
-define(MIN_FILLED, 20).
%-define(MAX_FILLED, 4).
%-define(MIN_FILLED, 2).


% NOTE vmx: At the moment "leaf" is used for the nodes that
%    contain the actual data (the doc IDs) and their parent node. It makes
%    the lookup code easier.
-record(node, {
    % type = inner | leaf
    type = inner
}).

% XXX vmx: tests are missing
add_remove(_Fd, Pos, TargetTreeeHeight, [], []) ->
    {ok, Pos, TargetTreeeHeight};
add_remove(Fd, Pos, TargetTreeHeight, AddKeyValues, KeysToRemove) ->
    % XXX vmx not sure about the structure of "KeysToRemove"
    NewPos = lists:foldl(fun({Mbr, DocId}, CurPos) ->
        io:format("vtree: delete (~p:~p): ~p~n", [Fd, CurPos, DocId]),
        % delete/4 returns {ok, integer()} or {empty, nil}
        {_, CurPos2} = delete(Fd, DocId, Mbr, CurPos),
        CurPos2
    end, Pos, KeysToRemove),
    T1 = get_timestamp(),
%    {NewPos2, TreeHeight} = lists:foldl(fun({{Mbr, DocId}, Value}, {CurPos, _}) ->
%        %io:format("vtree: add (~p:~p): {~p,~p}~n", [Fd, CurPos, DocId, Value]),
%        {ok, _NewMbr, CurPos2, TreeHeight} = insert(Fd, CurPos, DocId,
%                {Mbr, #node{type=leaf}, Value}),
%        {CurPos2, TreeHeight}
%    end, {NewPos, 0}, AddKeyValues),
    %{_Megaseconds,Seconds,_Microseconds} = erlang:now(),
%    AddKeyValues2 = lists:foldl(fun({{Mbr, DocId}, Value}, Acc) ->
%        [{Mbr, #node{type=leaf}, {DocId, Value}}|Acc]
%    end, [], AddKeyValues),
%    {Omt, TreeHeight} = vtree_bulk:omt_load(AddKeyValues2, ?MAX_FILLED),
%    {ok, MbrAndPosList} = vtree_bulk:omt_write_tree(Fd, Omt),
%    [{_Mbr, NewPos2}] = MbrAndPosList,
    AddKeyValues2 = lists:foldl(fun({{Mbr, DocId}, Value}, Acc) ->
        [{Mbr, #node{type=leaf}, {DocId, Value}}|Acc]
    end, [], AddKeyValues),
    {ok, NewPos2, TreeHeight} = vtree_bulk:bulk_load(
            Fd, Pos, TargetTreeHeight, AddKeyValues2),
    T2 = get_timestamp(),
    io:format(user, "It took: ~ps~n", [T2-T1]),
    io:format(user, "Tree height: ~p~n", [TreeHeight]),
    {ok, NewPos2, TreeHeight}.
    %{ok, 0}.

% Based on http://snipplr.com/view/23910/how-to-get-a-timestamp-in-milliseconds-from-erlang/ (2010-10-22)
get_timestamp() ->
    {Mega,Sec,Micro} = erlang:now(),
    ((Mega*1000000+Sec)*1000000+Micro)/1000000.

% Returns the number of matching geometries only
count_lookup(Fd, Pos, Bbox) ->
    case lookup(Fd, Pos, Bbox, {fun(Item, Acc) -> {ok, Acc+1} end, 0}) of
        {ok, []} -> 0;
        {ok, Count} -> Count
    end.


% All lookup functions return {ok|stop, Acc}

% This lookup function is mainly for testing
lookup(Fd, Pos, Bbox) ->
    % default function returns a list of 3-tuple with MBR, id, value
    lookup(Fd, Pos, Bbox, {fun({Bbox2, DocId, Value}, Acc) ->
         Acc2 = [{Bbox2, DocId, Value}|Acc],
         {ok, Acc2}
    end, []}).
lookup(_Fd, nil, _Bbox, {FoldFun, InitAcc}) ->
    {ok, InitAcc};
lookup(Fd, Pos, Bbox, {FoldFun, InitAcc}) when not is_list(Bbox) ->
    % default bounds are from this world
    lookup(Fd, Pos, Bbox, {FoldFun, InitAcc}, {-180, -90, 180, 90});
% For spatial index and spatial list requests InitAcc/Acc is:
% {ok|stop, {Resp, SomeRestFromPreviousRow}}
lookup(Fd, Pos, Bboxes, {FoldFun, InitAcc}) ->
    {ok, Parent} = couch_file:pread_term(Fd, Pos),
    {ParentMbr, ParentMeta, NodesPos} = Parent,
    case ParentMeta#node.type of
    inner ->
        foldl_stop(fun(EntryPos, Acc) ->
            case bboxes_not_disjoint(ParentMbr, Bboxes) of
            true ->
                lookup(Fd, EntryPos, Bboxes, {FoldFun, Acc});
            false ->
                {ok, Acc}
            end
        end, InitAcc, NodesPos);
    leaf ->
        case bboxes_within(ParentMbr, Bboxes) of
        % all children are within the bbox we search with
        true ->
            foldl_stop(fun({Mbr, _Meta, {Id, Value}}, Acc) ->
                FoldFun({Mbr, Id, Value}, Acc)
            end, InitAcc, NodesPos);
        false ->
            % loop through all data nodes and find not disjoint ones
            foldl_stop(fun({Mbr, _Meta, {Id, Value}}, Acc) ->
                case bboxes_not_disjoint(Mbr, Bboxes) of
                true ->
                    FoldFun({Mbr, Id, Value}, Acc);
                false ->
                    {ok, Acc}
                end
            end, InitAcc, NodesPos)
        end
    end.


% Only a single bounding box. It may be split if it covers the data line
lookup(Fd, Pos, Bbox, FoldFunAndAcc, Bounds) when not is_list(Bbox) ->
    case split_flipped_bbox(Bbox, Bounds) of
    not_flipped ->
        lookup(Fd, Pos, [Bbox], FoldFunAndAcc);
    Bboxes ->
        lookup(Fd, Pos, Bboxes, FoldFunAndAcc)
    end.

% It's just like lists:foldl/3. The difference is that it can be stopped.
% Therefore you always need to return a tuple with either "ok" or "stop"
% and the actual accumulator.
foldl_stop(_, Acc, []) ->
    {ok, Acc};
foldl_stop(Fun, Acc, [H|T]) ->
    Return = Fun(H, Acc),
    case Return of
    {ok, Acc2} ->
        foldl_stop(Fun, Acc2, T);
    {stop, Acc2} ->
        {stop, Acc2}
    end.

% Loops recursively through a list of bounding boxes and returns
% true if the given MBR is not disjoint with one of the bounding boxes
bboxes_not_disjoint(_Mbr, []) ->
    false;
bboxes_not_disjoint(Mbr, [Bbox|Tail]) ->
    case disjoint(Mbr, Bbox) of
    false ->
        true;
    true ->
        bboxes_not_disjoint(Mbr, Tail)
    end.

% Loops recursively through a list of bounding boxes and returns
% true if the given MBR is within one of the bounding boxes
bboxes_within(_Mbr, []) ->
    false;
bboxes_within(Mbr, [Bbox|Tail]) ->
    case within(Mbr, Bbox) of
    true ->
        true;
    false ->
        bboxes_within(Mbr, Tail)
    end.

% Splits a bounding box (BBox) at specific Bounds that is flipped
split_flipped_bbox({W, S, E, N}, {BW, BS, BE, BN}) when E < W, N < S ->
    [{W, S, BE, BN}, {BW, BS, E, N}];
split_flipped_bbox({W, S, E, N}, {BW, _, BE, _}) when E < W ->
    [{W, S, BE, N}, {BW, S, E, N}];
split_flipped_bbox({W, S, E, N}, {_, BS, _, BN}) when N < S ->
    [{W, S, E, BN}, {W, BS, E, N}];
split_flipped_bbox(_Bbox, _Bounds) ->
    not_flipped.


% Tests if Inner is within Outer box
within(Inner, Outer) ->
    %io:format("(within) Inner, Outer: ~p, ~p~n", [Inner, Outer]),
    {IW, IS, IE, IN} = Inner,
    {OW, OS, OE, ON} = Outer,
    (IW >= OW) and (IS >= OS) and (IE =< OE) and (IN =< ON).


% Returns true if one Mbr intersects with another Mbr
intersect(Mbr1, Mbr2) ->
    {W1, S1, E1, N1} = Mbr1,
    {W2, S2, E2, N2} = Mbr2,
    % N or S of Mbr1 is potentially intersected with a vertical edge
    % from Mbr2
    ((((N2 >= N1) and (S2 =< N1)) or ((N2 >= S1) and (S2 =< S1))) and
    % N or S of Mbr1 *is* intersected if a vertical line of Mbr2 isn't
    % next to Mbr1
    (((W2 >= W1) and (W2 =< E1)) or ((E2 >= W1) and (E2 =< E1))))
    or
    % W or E of Mbr1 is potentially intersected with a horizontal edge
    % from Mbr2
    ((((E2 >= E1) and (W2 =< E1)) or ((E2 >= W1) and (W2 =< W1))) and
    % W or E of Mbr1 *is* intersected if a horizontal line of Mbr2 isn't
    % above or below Mbr1
    (((S2 >= S1) and (S2 =< N1)) or ((N2 >= S1) and (N2 =< N1)))).


% Returns true if two MBRs are spatially disjoint
disjoint(Mbr1, Mbr2) ->
    %io:format("(disjoint) Mbr1, Mbr2: ~p, ~p~n", [Mbr1, Mbr2]),
    not (within(Mbr1, Mbr2) or within(Mbr2, Mbr1) or intersect(Mbr1, Mbr2)).


split_node({_Mbr, Meta, _Entries}=Node) ->
    %io:format("We need to split~n~p~n", [Node]),
    Partition = partition_node(Node),
    SplittedLeaf = best_split(Partition),
    [{Mbr1, Children1}, {Mbr2, Children2}] = case SplittedLeaf of
    {tie, PartitionMbrs} ->
        SplittedLeaf2 = minimal_overlap(Partition, PartitionMbrs),
        case SplittedLeaf2 of
        tie ->
            minimal_coverage(Partition, PartitionMbrs);
        _ ->
            SplittedLeaf2
        end;
    _ ->
        SplittedLeaf
    end,
    case Meta#node.type of
    leaf ->
        {merge_mbr(Mbr1, Mbr2),
         {Mbr1, #node{type=leaf}, Children1},
         {Mbr2, #node{type=leaf}, Children2}};
    inner ->
        % Child nodes were expanded (read from file) to do some calculations,
        % now get the pointers to their position in the file back and return
        % only a list of these positions.
        ChildrenPos1 = lists:map(fun(Entry) ->
            {_, _, EntryPos} = Entry,
            EntryPos
        end, Children1),
        ChildrenPos2 = lists:map(fun(Entry) ->
            {_, _, EntryPos} = Entry,
            EntryPos
        end, Children2),
        {merge_mbr(Mbr1, Mbr2),
         {Mbr1, #node{type=inner}, ChildrenPos1},
         {Mbr2, #node{type=inner}, ChildrenPos2}}
    end.


% Return values of insert:
% At top-level: {ok, MBR, position_in_file, height_of_tree}
% XXX vmx MBR_of_both_nodes could be calculated if needed
% If a split occurs: {splitted, MBR_of_both_nodes,
%                     {MBR_of_node1, position_in_file_node1},
%                     {MBR_of_node2, position_in_file_node2}}
insert(Fd, nil, Id, {Mbr, Meta, Value}) ->
    InitialTree = {Mbr, #node{type=leaf}, [{Mbr, Meta, {Id, Value}}]},
    {ok, Pos} = couch_file:append_term(Fd, InitialTree),
    {ok, Mbr, Pos, 1};

insert(Fd, RootPos, Id, Node) ->
    {ok, Mbr, NewPos, Height} = insert(Fd, RootPos, Id, Node, 0),
    {ok, Mbr, NewPos, Height+1}.

insert(Fd, RootPos, NewNodeId,
       {NewNodeMbr, NewNodeMeta, NewNodeValue}, CallDepth) ->
    NewNode = {NewNodeMbr, NewNodeMeta#node{type=leaf}, NewNodeValue},
    % EntriesPos is only a pointer to the node (position in file)
    {ok, {TreeMbr, Meta, EntriesPos}} = couch_file:pread_term(Fd, RootPos),
    EntryNum = length(EntriesPos),
    %Entries = case Meta#node.type of
    Inserted = case Meta#node.type of
    leaf ->
        % NOTE vmx: Currently we don't save each entry individually, but the
        %     whole set of entries. This might be worth changing in the future
        %lists:map(fun(EntryPos) ->
        %    {ok, CurEntry} = couch_file:pread_term(Fd, EntryPos),
        %    CurEntry
        %end, EntriesPos);
        Entries = EntriesPos,
        LeafNodeMbr = merge_mbr(TreeMbr, element(1, NewNode)),

        % store the ID with every node:
        NewNode2 = {NewNodeMbr, NewNodeMeta#node{type=leaf},
               {NewNodeId, NewNodeValue}},

        LeafNode = {LeafNodeMbr, #node{type=leaf}, Entries ++ [NewNode2]},
        if
        EntryNum < ?MAX_FILLED ->
            %io:format("There's plenty of space (leaf node)~n", []),
            {ok, Pos} = couch_file:append_term(Fd, LeafNode),
            {ok, LeafNodeMbr, Pos, CallDepth};
        % do the fancy split algorithm
        true ->
            %io:format("We need to split (leaf node)~n~p~n", [LeafNode]),
            {SplittedMbr, {Node1Mbr, _, _}=Node1, {Node2Mbr, _, _}=Node2}
                    = split_node(LeafNode),
            {ok, Pos1} = couch_file:append_term(Fd, Node1),
            {ok, Pos2} = couch_file:append_term(Fd, Node2),
            {splitted, SplittedMbr, {Node1Mbr, Pos1}, {Node2Mbr, Pos2},
             CallDepth}
        end;
    % If the nodes are inner nodes, they only contain pointers to their child
    % nodes. We only need their MBRs, position, but not their children's
    % position. Read them from disk, but store their position in file (pointer
    % from parent node) instead of their child nodes.
    inner ->
        Entries = lists:map(fun(EntryPos) ->
            {ok, CurEntry} = couch_file:pread_term(Fd, EntryPos),
            {EntryMbr, EntryMeta, _} = CurEntry,
            {EntryMbr, EntryMeta, EntryPos}
        end, EntriesPos),

        %io:format("I'm an inner node~n", []),
        % Get entry where smallest MBR expansion is needed
        Expanded = lists:map(
            fun(Entry) ->
                {EntryMbr, _, _} = Entry,
                {NewNodeMbr2, _, _} = NewNode,
                EntryArea = area(EntryMbr),
                AreaMbr = merge_mbr(EntryMbr, NewNodeMbr2),
                NewArea = area(AreaMbr),
                AreaDiff = NewArea - EntryArea,
                %{EntryArea, NewArea, AreaDiff}
                {AreaDiff, AreaMbr}
            end, Entries),
        MinPos = find_area_min_nth(Expanded),
        SubTreePos = lists:nth(MinPos, EntriesPos),
        case insert(Fd, SubTreePos, NewNodeId, NewNode, CallDepth+1) of
        {ok, ChildMbr, ChildPos, TreeHeight} ->
            %io:format("not splitted:~n", []),
            NewMbr = merge_mbr(TreeMbr, ChildMbr),
            {A, B} = lists:split(MinPos-1, EntriesPos),
            % NOTE vmx: I guess child nodes don't really have an order, we
            %     could remove the old node and append the new one at the
            %     end of the list.
            NewNode2 = {NewMbr, #node{type=inner}, A ++ [ChildPos] ++ tl(B)},
            {ok, Pos} = couch_file:append_term(Fd, NewNode2),
            {ok, NewMbr, Pos, TreeHeight};
        {splitted, ChildMbr, {Child1Mbr, ChildPos1}, {Child2Mbr, ChildPos2},
         TreeHeight} ->
            %io:format("EntryNum: ~p~n", [EntryNum]),
            NewMbr = merge_mbr(TreeMbr, ChildMbr),
            if
            % Both nodes of the split fit in the current inner node
            EntryNum+1 < ?MAX_FILLED ->
                {A, B} = lists:split(MinPos-1, EntriesPos),
                NewNode2 = {NewMbr, #node{type=inner},
                            A ++ [ChildPos1, ChildPos2] ++ tl(B)},
                {ok, Pos} = couch_file:append_term(Fd, NewNode2),
                {ok, NewMbr, Pos, TreeHeight};
            % We need to split the inner node
            true ->
                %io:format("We need to split (inner node)~n~p~n", [Entries]),
                {_, ChildNodeMeta, _} = lists:nth(1, Entries),
                Child1 = {Child1Mbr, ChildNodeMeta, ChildPos1},
                Child2 = {Child2Mbr, ChildNodeMeta, ChildPos2},

                % Original node, that was split, needs to be removed
                {A, B} = lists:split(MinPos-1, Entries),

                {SplittedMbr, {Node1Mbr, _, _}=Node1, {Node2Mbr, _, _}=Node2}
                        = split_node({NewMbr, #node{type=inner},
                                      A ++ [Child1, Child2] ++ tl(B)}),
                {ok, Pos1} = couch_file:append_term(Fd, Node1),
                {ok, Pos2} = couch_file:append_term(Fd, Node2),
                {splitted, SplittedMbr, {Node1Mbr, Pos1}, {Node2Mbr, Pos2},
                 TreeHeight}
            end
        end
    end,
    case {Inserted, CallDepth} of
        % Root node needs to be split => new root node
        {{splitted, NewRootMbr, {SplittedNode1Mbr, SplittedNode1},
          {SplittedNode2Mbr, SplittedNode2}, TreeHeight2}, 0} ->
            %io:format("Creating new root node~n", []),
            NewRoot = {NewRootMbr, #node{type=inner},
                           [SplittedNode1, SplittedNode2]},
            {ok, NewRootPos} = couch_file:append_term(Fd, NewRoot),
            {ok, NewRootMbr, NewRootPos, TreeHeight2+1};
        _ ->
            Inserted
    end.

area(Mbr) ->
    {W, S, E, N} = Mbr,
    abs(E-W) * abs(N-S).


merge_mbr(Mbr1, Mbr2) ->
    {W1, S1, E1, N1} = Mbr1,
    {W2, S2, E2, N2} = Mbr2,
    {min(W1, W2), min(S1, S2), max(E1, E2), max(N1, N2)}.


find_area_min_nth([H|T]) ->
    find_area_min_nth(1, T, {H, 1}).

find_area_min_nth(_Count, [], {_Min, MinCount}) ->
    MinCount;

find_area_min_nth(Count, [{HMin, HMbr}|T], {{Min, _Mbr}, _MinCount})
  when HMin < Min->
    find_area_min_nth(Count+1, T, {{HMin, HMbr}, Count+1});

find_area_min_nth(Count, [_H|T], {{Min, Mbr}, MinCount}) ->
    find_area_min_nth(Count+1, T, {{Min, Mbr}, MinCount}).

partition_node({Mbr, _Meta, Nodes}) ->
    {MbrW, MbrS, MbrE, MbrN} = Mbr,
    %io:format("(partition_node) Mbr: ~p~n", [Mbr]),
    Tmp = lists:foldl(
        fun(Node,  {AccW, AccS, AccE, AccN}) ->
            {{W, S, E, N}, _NodeMeta, _Id} = Node,
            if
                W-MbrW < MbrE-E ->
                    NewAccW = AccW ++ [Node],
                    NewAccE = AccE;
                true ->
                    NewAccW = AccW,
                    NewAccE = AccE ++ [Node]
            end,
            if
                S-MbrS < MbrN-N ->
                    NewAccS = AccS ++ [Node],
                    NewAccN = AccN;
                true ->
                    NewAccS = AccS,
                    NewAccN = AccN ++ [Node]
            end,
            {NewAccW, NewAccS, NewAccE, NewAccN}
        end, {[],[],[],[]}, Nodes),
    %io:format("(partition_node) Partitioned: ~p~n", [Tmp]),
    % XXX vmx This is a hack! A better partitioning algorithm should be used.
    %     If the two corresponding partitions are empty, split node in the
    %     middle
    case Tmp of
        {[], [], Es, Ns} ->
            {NewW, NewE} = lists:split(length(Es) div 2, Es),
            {NewS, NewN} = lists:split(length(Ns) div 2, Ns),
            {NewW, NewS, NewE, NewN};
        {Ws, Ss, [], []} ->
            {NewW, NewE} = lists:split(length(Ws) div 2, Ws),
            {NewS, NewN} = lists:split(length(Ss) div 2, Ss),
            {NewW, NewS, NewE, NewN};
        {Ws, [], [], Ns} ->
            {NewW, NewE} = lists:split(length(Ws) div 2, Ws),
            {NewS, NewN} = lists:split(length(Ns) div 2, Ns),
            {NewW, NewS, NewE, NewN};
        {[], Ss, Es, []} ->
            {NewS, NewN} = lists:split(length(Ss) div 2, Ss),
            {NewW, NewE} = lists:split(length(Es) div 2, Es),
            {NewW, NewS, NewE, NewN};
        _ ->
            Tmp
    end.


calc_nodes_mbr(Nodes) ->
    {Mbrs, _Meta, _Ids} = lists:unzip3(Nodes),
    calc_mbr(Mbrs).

calc_mbr([]) ->
    error;
calc_mbr([H|T]) ->
    calc_mbr(T, H).

calc_mbr([Mbr|T], AccMbr) ->
    MergedMbr = merge_mbr(Mbr, AccMbr),
    calc_mbr(T, MergedMbr);
calc_mbr([], Acc) ->
    Acc.


best_split({PartW, PartS, PartE, PartN}) ->
    %io:format("(best_split) PartW, PartS, PartE, PartN: ~p, ~p, ~p, ~p~n", [PartW, PartS, PartE, PartN]),
    MbrW = calc_nodes_mbr(PartW),
    MbrE = calc_nodes_mbr(PartE),
    MbrS = calc_nodes_mbr(PartS),
    MbrN = calc_nodes_mbr(PartN),
    MaxWE = max(length(PartW), length(PartE)),
    MaxSN = max(length(PartS), length(PartN)),
    if
        MaxWE < MaxSN ->
            [{MbrW, PartW}, {MbrE, PartE}];
        MaxWE > MaxSN ->
            [{MbrS, PartS}, {MbrN, PartN}];
        true ->
            % XXX vmx This is very unlikely to happen (but can for small node
            %     sizes) It means that the partition consists of a single node
            %     only.
            % XXX vmx This is a hack! A better partitioning algorithm should
            %     be used.
            %     If there is only one node, use that one.
            case {MbrW, MbrS, MbrE, MbrN} of
                {error, error, _, _} ->
                    io:format("vtree: WORKAROUND WAS USED, PLEASE TELL vmx~n"),
                    [{MbrE, PartE}, {MbrN, PartN}];
                {_, _, error, error} ->
                    io:format("vtree: WORKAROUND WAS USED, PLEASE TELL vmx~n"),
                    [{MbrW, PartW}, {MbrS, PartS}];
                {_, error, error, _} ->
                    io:format("vtree: WORKAROUND WAS USED, PLEASE TELL vmx~n"),
                    [{MbrW, PartW}, {MbrN, PartN}];
                {error, _, _, error} ->
                    io:format("vtree: WORKAROUND WAS USED, PLEASE TELL vmx~n"),
                    [{MbrS, PartS}, {MbrE, PartE}];
                _ ->
                    % XXX vmx this is the right, normal case (i.e. the hack
                    % is above)
                    % MBRs are needed for further calculation
                    {tie,  {MbrW, MbrS, MbrE, MbrN}}
            end
    end.


minimal_overlap({PartW, PartS, PartE, PartN}, {MbrW, MbrS, MbrE, MbrN}) ->
    %io:format("(minimal_overlap) MbrW, MbrS, MbrE, MbrN: ~p, ~p, ~p, ~p~n", [MbrW, MbrS, MbrE, MbrN]),
    OverlapWE = area(calc_overlap(MbrW, MbrE)),
    OverlapSN = area(calc_overlap(MbrS, MbrN)),
    %io:format("overlap: ~p|~p~n", [OverlapWE, OverlapSN]),
    if
        OverlapWE < OverlapSN ->
            [{MbrW, PartW}, {MbrE, PartE}];
        OverlapWE > OverlapSN ->
            [{MbrS, PartS}, {MbrN, PartN}];
        true ->
            tie
    end.

minimal_coverage({PartW, PartS, PartE, PartN}, {MbrW, MbrS, MbrE, MbrN}) ->
    CoverageWE = area(MbrW) + area(MbrE),
    CoverageSN = area(MbrS) + area(MbrN),
    %io:format("coverage: ~p|~p~n", [CoverageWE, CoverageSN]),
    if
        CoverageWE < CoverageSN ->
            [{MbrW, PartW}, {MbrE, PartE}];
        true ->
            [{MbrS, PartS}, {MbrN, PartN}]
    end.

calc_overlap(Mbr1, Mbr2) ->
%    io:format("(calc_overlap) Mbr1, Mbr2: ~p, ~p~n", [Mbr1, Mbr2]),
    IsDisjoint = disjoint(Mbr1, Mbr2),
    if
        not IsDisjoint ->
            {W1, S1, E1, N1} = Mbr1,
            {W2, S2, E2, N2} = Mbr2,
            {max(W1, W2), max(S1, S2), min(E1, E2), min(N1, N2)};
        true ->
            {0, 0, 0, 0}
    end.

% Returns: {ok, NewRootPos} or
%     not_found or
%     empty (if tree is empty because of that deletion)
delete(Fd, DeleteId, DeleteMbr, RootPos) when not is_list(RootPos) ->
    case delete(Fd, DeleteId, DeleteMbr, [RootPos]) of
    {ok, RootNewPos, RootPos} ->
        {ok, RootNewPos};
    % Tree is completely empty
    {empty, RootPos} ->
        {empty, nil};
    not_found ->
        not_found
    end;

delete(_Fd, _DeleteId, _DeleteMbr, []) ->
    not_found;

% Returns: {ok, NewPos, OldPos} or
%     not_found or
%     {empty, OldPos}
delete(Fd, DeleteId, DeleteMbr, [NodePos|NodePosTail]) ->
    {ok, Node} = couch_file:pread_term(Fd, NodePos),
    {NodeMbr, NodeMeta, NodeEntriesPos} = Node,
    case within(DeleteMbr, NodeMbr) of
    true ->
        case NodeMeta#node.type of
        inner ->
            case delete(Fd, DeleteId, DeleteMbr, NodeEntriesPos) of
            {ok, ChildNewPos, ChildOldPos} ->
                % Delete pointer to old node, add new one
                EntriesPosNew = lists:delete(ChildOldPos, NodeEntriesPos) ++
                        [ChildNewPos],
                NodeNewPos = rebuild_node(Fd, NodeMeta, EntriesPosNew),
                {ok, NodeNewPos, NodePos};
            % Node doesn't have children any longer
            {empty, ChildOldPos} ->
                EntriesPosNew = lists:delete(ChildOldPos, NodeEntriesPos),
                % This node is empty as well
                if length(EntriesPosNew) == 0 ->
                    %io:format("It's empty again: ~p~n", [NodePos]),
                    {empty, NodePos};
                true ->
                    NodeNewPos = rebuild_node(Fd, NodeMeta, EntriesPosNew),
                    {ok, NodeNewPos, NodePos}
                end;
            not_found ->
                % go on with sibling nodes
                delete(Fd, DeleteId, DeleteMbr, NodePosTail)
            end;
        leaf ->
            case funtake(fun({_, _, {DocId, _Value}}) -> DocId end,
                         DeleteId, NodeEntriesPos) of
            {value, EntriesNew} ->
                case EntriesNew of
                [] ->
                    {empty, NodePos};
                _ ->
                    NodeMbrNew = calc_nodes_mbr(EntriesNew),
                    {ok, NodeNewPos} = couch_file:append_term(Fd,
                                          {NodeMbrNew, NodeMeta, EntriesNew}),
                    % NodePos is the old position in file
                    {ok, NodeNewPos, NodePos}
                end;
            false ->
                not_found
            end
        end;
    false ->
        % go on with sibling nodes
        delete(Fd, DeleteId, DeleteMbr, NodePosTail)
    end.

rebuild_node(Fd, NodeMeta, EntriesPos) ->
    Entries = pos_to_data(Fd, EntriesPos),
    Mbr = calc_nodes_mbr(Entries),
    {ok, NodePos} = couch_file:append_term(Fd, {Mbr, NodeMeta, EntriesPos}),
    NodePos.

% It's a bit like lists:keytake/3, but uses a function that returns a key
% instead of using the key directly
funtake(Pred, ToFind, List) ->
    funtake(Pred, ToFind, List, []).

funtake(_Pred, _ToFind, [], _Acc) ->
    false;
funtake(Pred, ToFind, [H|T], Acc) ->
    case Pred(H) of
    ToFind ->
        {value, Acc ++ T};
    _ ->
        funtake(Pred, ToFind, T, Acc ++ [H])
    end.

% Transforms a list of positions in a file to the actual data
pos_to_data(Fd, List) ->
    pos_to_data(Fd, List, []).

pos_to_data(_Fd, [], DataList) ->
    DataList;

pos_to_data(Fd, [H|T], DataList) ->
    {ok, Data} = couch_file:pread_term(Fd, H),
    pos_to_data(Fd, T, DataList ++ [Data]).

min(A, B) ->
    if A > B -> B ; true -> A end.


max(A, B) ->
    if A > B -> A ; true -> B end.


get_node(Fd, Pos) ->
    couch_file:pread_term(Fd, Pos).
