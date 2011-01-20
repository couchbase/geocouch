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

-module(vtree_test).
-export([start/0, build_random_tree/2, random_node/0, random_node/1]).

-define(FILENAME, "/tmp/vtree.bin").


start() ->
    test_within(),
    test_intersect(),
    test_disjoint(),
    test_lookup(),
    test_multilookup(),
    test_split_flipped_bbox(),
    test_area(),
    test_merge_mbr(),
    test_find_area_min_nth(),
    test_partition_node(),
    test_calc_mbr(),
    test_calc_nodes_mbr(),
    test_best_split(),
    test_minimal_overlap(),
    test_minimal_coverage(),
    test_calc_overlap(),
    test_insert(),
    test_delete(),
    test_split_node(),
    test_count_total(),

    etap:end_tests().


-record(node, {
    % type = inner | leaf
    type=leaf}).

test_insert() ->
    etap:plan(6),

    {ok, Fd} = case couch_file:open(?FILENAME, [create, overwrite]) of
    {ok, Fd2} ->
        {ok, Fd2};
    {error, Reason} ->
        io:format("ERROR (~s): Couldn't open file (~s) for tree storage~n",
                  [Reason, ?FILENAME])
    end,

    Node1 = {{10,5,13,15}, #node{type=leaf}, <<"Node1">>},
    Node2 = {{-18,-3,-10,-1}, #node{type=leaf}, <<"Node2">>},
    Node3 = {{-21,2,-10,14}, #node{type=leaf}, <<"Node3">>},
    Node4 = {{5,-32,19,-25}, #node{type=leaf}, <<"Node4">>},
    Node5 = {{-5,-16,4,19}, #node{type=leaf}, <<"Node5">>},
    % od = on disk
    Node1od = {{10,5,13,15}, #node{type=leaf}, {<<"Node1">>,<<"Node1">>}},
    Node2od = {{-18,-3,-10,-1}, #node{type=leaf}, {<<"Node2">>,<<"Node2">>}},
    Node3od = {{-21,2,-10,14}, #node{type=leaf}, {<<"Node3">>,<<"Node3">>}},
    Node4od = {{5,-32,19,-25}, #node{type=leaf}, {<<"Node4">>,<<"Node4">>}},
    Node5od = {{-5,-16,4,19}, #node{type=leaf}, {<<"Node5">>,<<"Node5">>}},
    Mbr1 = {10,5,13,15},
    Mbr1_2 = {-18,-3,13,15},
    Mbr1_2_3 = {-21,-3,13,15},
    Mbr1_2_3_4 = {-21,-32,19,15},
    Mbr1_2_3_4_5 = {-21,-32,19,19},
    Mbr1_4_5 = {-5,-32,19,19},
    Mbr2_3 = {-21,-3,-10,14},
    Tree1Node1 = {Mbr1, #node{type=leaf}, [Node1od]},
    Tree1Node1_2 = {Mbr1_2, #node{type=leaf}, [Node1od, Node2od]},
    Tree1Node1_2_3 = {Mbr1_2_3, #node{type=leaf}, [Node1od, Node2od, Node3od]},
    Tree1Node1_2_3_4 = {Mbr1_2_3_4, #node{type=leaf},
                        [Node1od, Node2od, Node3od, Node4od]},
    Tree1Node1_2_3_4_5 = {Mbr1_2_3_4_5, #node{type=inner},
                          [{ok, {Mbr2_3, #node{type=leaf}, [Node2od, Node3od]}},
                           {ok, {Mbr1_4_5, #node{type=leaf},
                                 [Node1od, Node4od, Node5od]}}]},

    etap:is(vtree:insert(Fd, nil, <<"Node1">>, Node1), {ok, Mbr1, 0, 1},
            "Insert a node into an empty tree (write to disk)"),
    etap:is(vtree:get_node(Fd, 0), {ok, Tree1Node1},
            "Insert a node into an empty tree" ++
            " (check if it was written correctly)"),
    {ok, Mbr1_2, Pos2, 1} = vtree:insert(Fd, 0, <<"Node2">>, Node2),
    etap:is(vtree:get_node(Fd, Pos2), {ok, Tree1Node1_2},
            "Insert a node into a not yet full leaf node (root node) (a)"),
    {ok, Mbr1_2_3, Pos3, 1} = vtree:insert(Fd, Pos2, <<"Node3">>, Node3),
    etap:is(vtree:get_node(Fd, Pos3), {ok, Tree1Node1_2_3},
            "Insert a node into a not yet full leaf node (root node) (b)"),
    {ok, Mbr1_2_3_4, Pos4, 1} = vtree:insert(Fd, Pos3, <<"Node4">>, Node4),
    etap:is(vtree:get_node(Fd, Pos4), {ok, Tree1Node1_2_3_4},
            "Insert a nodes into a then to be full leaf node (root node)"),
    {ok, Mbr1_2_3_4_5, Pos5, 2} = vtree:insert(Fd, Pos4, <<"Node5">>, Node5),
    {ok, {Mbr1_2_3_4_5, #node{type=inner}, [Pos5_1, Pos5_2]}} =
                vtree:get_node(Fd, Pos5),
    etap:is({ok, {Mbr1_2_3_4_5, #node{type=inner},
                  [vtree:get_node(Fd, Pos5_1), vtree:get_node(Fd, Pos5_2)]}},
                  {ok, Tree1Node1_2_3_4_5},
            "Insert a nodes into a full leaf node (root node)"),
    ok.


test_within() ->
    etap:plan(4),
    Bbox1 = {-20, -10, 30, 21},
    Bbox2 = {-20, -10, 0, 0},
    Mbr1_2 = {-18,-3,13,15},
    Node1 = {{10,5,13,15}, <<"Node1">>},
    {Node1Mbr, _} = Node1,
    etap:is(vtree:within(Node1Mbr, Bbox1), true, "MBR is within the BBox"),
    etap:is(vtree:within(Node1Mbr, Node1Mbr), true, "MBR is within itself"),
    etap:is(vtree:within(Node1Mbr, Bbox2), false,
            "MBR is not at all within BBox"),
    etap:is(vtree:within(Mbr1_2, Bbox2), false, "MBR intersects BBox"),
    ok.

test_intersect() ->
    etap:plan(17),
    Mbr1_2 = {-18,-3,13,15},
    etap:is(vtree:intersect(Mbr1_2, {-20, -11, 0, 0}), true,
            "MBR intersectton (S and W edge)"),
    etap:is(vtree:intersect(Mbr1_2, {-21, 4, -2, 11}), true,
            "MBR intersecttion (W edge)"),
    etap:is(vtree:intersect(Mbr1_2, {-21, 4, -2, 17}), true,
            "MBR intersection (W and N edge)"),
    etap:is(vtree:intersect(Mbr1_2, {-13, 4, -2, 17}), true,
            "MBR intersection (N edge)"),
    etap:is(vtree:intersect(Mbr1_2, {-13, 4, 16, 17}), true,
            "MBR intersection (N and E edge)"),
    etap:is(vtree:intersect(Mbr1_2, {5, -1, 16, 10}), true,
            "MBR intersection (E edge)"),
    etap:is(vtree:intersect(Mbr1_2, {5, -9, 16, 10}), true,
            "MBR intersection (E and S edge)"),
    etap:is(vtree:intersect(Mbr1_2, {5, -9, 11, 10}), true,
            "MBR intersection (S edge)"),
    etap:is(vtree:intersect(Mbr1_2, {-27, -9, 11, 10}), true,
            "MBR intersection (S and W edge)"),
    etap:is(vtree:intersect(Mbr1_2, {-27, -9, 18, 10}), true,
            "MBR intersection (W and E edge (bottom))"),
    etap:is(vtree:intersect(Mbr1_2, {-27, 2, 18, 10}), true,
            "MBR intersection (W and E edge (middle))"),
    etap:is(vtree:intersect(Mbr1_2, {-10, -9, 18, 10}), true,
            "MBR intersection (W and E edge (top))"),
    etap:is(vtree:intersect(Mbr1_2, {-25, -4, 2, 12}), true,
            "MBR intersection (N and S edge (left))"),
    etap:is(vtree:intersect(Mbr1_2, {-15, -4, 2, 12}), true,
            "MBR intersection (N and S edge (middle))"),
    etap:is(vtree:intersect(Mbr1_2, {-15, -4, 2, 22}), true,
            "MBR intersection (N and S edge (right))"),
    etap:is(vtree:intersect(Mbr1_2, {-14, -1, 10, 5}), false,
            "One MBR within the other"),
    etap:is(vtree:intersect(Mbr1_2, Mbr1_2), true,
            "MBR is within itself"),
    ok.

test_disjoint() ->
    etap:plan(2),
    Mbr1_2 = {-18,-3,13,15},
    etap:is(vtree:disjoint(Mbr1_2, {27, 20, 38, 40}), true,
            "MBRs are disjoint"),
    etap:is(vtree:disjoint(Mbr1_2, {-27, 2, 18, 10}), false,
            "MBRs are not disjoint").


test_lookup() ->
    etap:plan(6),

    {ok, Fd} = case couch_file:open(?FILENAME, [create, overwrite]) of
    {ok, Fd2} ->
        {ok, Fd2};
    {error, Reason} ->
        io:format("ERROR (~s): Couldn't open file (~s) for tree storage~n",
                  [Reason, ?FILENAME])
    end,

    Node1 = {{10,5,13,15}, #node{type=leaf}, <<"Node1">>},
    Node2 = {{-18,-3,-10,-1}, #node{type=leaf}, <<"Node2">>},
    Node3 = {{-21,2,-10,14}, #node{type=leaf}, <<"Node3">>},
    Node4 = {{5,-32,19,-25}, #node{type=leaf}, <<"Node4">>},
    Node5 = {{-5,-16,4,19}, #node{type=leaf}, <<"Node5">>},
    {Mbr1, _, Id1} = Node1,
    {Mbr2, _, Id2} = Node2,
    {_, _, Id3} = Node3,
    {_, _, Id4} = Node4,
    {Mbr5, _, Id5} = Node5,
    Mbr1_2 = {-18,-3,13,15},
    Mbr1_2_3 = {-21,-3,13,15},
    Mbr1_2_3_4 = {-21,-32,19,15},
    Mbr1_2_3_4_5 = {-21,-32,19,19},
    Bbox1 = {-20, -10, 30, 21},
    Bbox2 = {-20, -10, 0, 0},
    Bbox3 = {100, 200, 300, 400},
    Bbox4 = {0, 0, 20, 15},

    {ok, Mbr1, 0, 1} = vtree:insert(Fd, nil, Id1, Node1),
    {ok, Mbr1_2, Pos2, 1} = vtree:insert(Fd, 0, Id2, Node2),
    {ok, Mbr1_2_3, Pos3, 1} = vtree:insert(Fd, Pos2, Id3, Node3),
    {ok, Mbr1_2_3_4, Pos4, 1} = vtree:insert(Fd, Pos3, Id4, Node4),
    {ok, Mbr1_2_3_4_5, Pos5, 2} = vtree:insert(Fd, Pos4, Id5, Node5),

    {ok, Lookup1} = vtree:lookup(Fd, Pos2, Bbox1),
    etap:is(Lookup1, [{Mbr2, Id2, Id2},{Mbr1, Id1, Id1}],
            "Find all nodes in tree (tree height=1)"),
    {ok, Lookup2} = vtree:lookup(Fd, Pos2, Bbox2),
    etap:is(Lookup2, [{Mbr2, Id2, Id2}],
            "Find some nodes in tree (tree height=1)"),
    {ok, Lookup3} = vtree:lookup(Fd, Pos2, Bbox3),
    etap:is(Lookup3, [], "Query window outside of all nodes (tree height=1)"),
    {ok, Lookup4} = vtree:lookup(Fd, Pos5, Bbox2),
    etap:is(Lookup4, [{Mbr5, Id5, Id5}, {Mbr2, Id2, Id2}],
            "Find some nodes in tree (tree height=2) (a)"),
    {ok, Lookup5} = vtree:lookup(Fd, Pos5, Bbox4),
    etap:is(Lookup5, [{Mbr5, Id5, Id5}, {Mbr1, Id1, Id1}],
            "Find some nodes in tree (tree height=2) (b)"),
    {ok, Lookup6} = vtree:lookup(Fd, Pos5, Bbox3),
    etap:is(Lookup6, [], "Query window outside of all nodes (tree height=2)"),
    ok.


test_multilookup() ->
    etap:plan(5),

    {ok, Fd} = case couch_file:open(?FILENAME, [create, overwrite]) of
    {ok, Fd2} ->
        {ok, Fd2};
    {error, Reason} ->
        io:format("ERROR (~s): Couldn't open file (~s) for tree storage~n",
                  [Reason, ?FILENAME])
    end,

    Node1 = {{10,5,13,15}, #node{type=leaf}, <<"Node1">>},
    Node2 = {{-18,-3,-10,-1}, #node{type=leaf}, <<"Node2">>},
    Node3 = {{-21,2,-10,14}, #node{type=leaf}, <<"Node3">>},
    Node4 = {{5,-32,19,-25}, #node{type=leaf}, <<"Node4">>},
    Node5 = {{-5,-16,4,19}, #node{type=leaf}, <<"Node5">>},
    {Mbr1, _, Id1} = Node1,
    {Mbr2, _, Id2} = Node2,
    {_, _, Id3} = Node3,
    {_, _, Id4} = Node4,
    {_, _, Id5} = Node5,
    Mbr1_2 = {-18,-3,13,15},
    Mbr1_2_3 = {-21,-3,13,15},
    Mbr1_2_3_4 = {-21,-32,19,15},
    Mbr1_2_3_4_5 = {-21,-32,19,19},
    Bbox1 = {-20, -10, 30, 21},
    Bbox1a = {-20, -10, 5, 10},
    Bbox1b = {5, 10, 30, 21},
    Bbox2a = {-20, -17, -17, 0},
    Bbox2b = {-15, -5, -8, -2},
    Bbox2c = {200, 100, 190, 90},
    Bbox3 = {100, 200, 300, 400},

    {ok, Mbr1, 0, 1} = vtree:insert(Fd, nil, Id1, Node1),
    {ok, Mbr1_2, Pos2, 1} = vtree:insert(Fd, 0, Id2, Node2),
    {ok, Mbr1_2_3, Pos3, 1} = vtree:insert(Fd, Pos2, Id3, Node3),
    {ok, Mbr1_2_3_4, Pos4, 1} = vtree:insert(Fd, Pos3, Id4, Node4),
    {ok, Mbr1_2_3_4_5, Pos5, 2} = vtree:insert(Fd, Pos4, Id5, Node5),

    {ok, Lookup1} = vtree:lookup(Fd, Pos2, [Bbox1]),
    etap:is(Lookup1, [{Mbr2, Id2, Id2}, {Mbr1, Id1, Id1}],
            "Find all nodes in tree (tree height=1) with one bbox"),
    {ok, Lookup2} = vtree:lookup(Fd, Pos2, [Bbox1a, Bbox1b]),
    etap:is(Lookup2, [{Mbr2, Id2, Id2}, {Mbr1, Id1, Id1}],
            "Find all nodes in tree (tree height=1) with two bboxes"),
    {ok, Lookup3} = vtree:lookup(Fd, Pos5, [Bbox2a, Bbox2b]),
    etap:is(Lookup3, [{Mbr2, Id2, Id2}],
            "Find some nodes in tree (tree height=2) 2 bboxes"),
    {ok, Lookup4} = vtree:lookup(Fd, Pos5, [Bbox2a, Bbox2b, Bbox2c]),
    etap:is(Lookup4, [{Mbr2, Id2, Id2}],
            "Find some nodes in tree (tree height=2) 3 bboxes (one outside "
            "of all nodes"),
    {ok, Lookup5} = vtree:lookup(Fd, Pos5, [Bbox2c, Bbox3]),
    etap:is(Lookup5, [],
            "Don't find any nodes in tree (tree height=2) 2 bboxes (both "
            "outside of all nodes"),
    ok.

test_split_flipped_bbox() ->
    etap:plan(8),

    etap:is(vtree:split_flipped_bbox({160,40,-120,60}, {-180,-90,180,90}),
            [{160,40,180,60}, {-180,40,-120,60}],
            "Bbox over the date line and north (flipped in x direction)"),
    etap:is(vtree:split_flipped_bbox({160,-60,-120,-40}, {-180,-90,180,90}),
            [{160,-60,180,-40}, {-180,-60,-120,-40}],
            "Bbox over the date line and south (flipped in x direction)"),
    etap:is(vtree:split_flipped_bbox({160,-40,-120,60}, {-180,-90,180,90}),
            [{160,-40,180,60}, {-180,-40,-120,60}],
            "Bbox over the date line and over equator (flipped in x "
            "direction)"),

    etap:is(vtree:split_flipped_bbox({20,70,30,-60}, {-180,-90,180,90}),
            [{20,70,30,90}, {20,-90,30,-60}],
            "Bbox over the pole and east (flipped in y direction)"),
    etap:is(vtree:split_flipped_bbox({-30,70,-20,-60}, {-180,-90,180,90}),
            [{-30,70,-20,90}, {-30,-90,-20,-60}],
            "Bbox over the pole and west (flipped in y direction)"),
    etap:is(vtree:split_flipped_bbox({-30,70,20,-60}, {-180,-90,180,90}),
            [{-30,70,20,90}, {-30,-90,20,-60}],
            "Bbox over the pole and Greenwich (flipped in y direction)"),

    etap:is(vtree:split_flipped_bbox({150,70,30,-60}, {-180,-90,180,90}),
            [{150,70,180,90}, {-180,-90,30,-60}],
            "Bbox over the pole and date line (flipped in x and y direction)"),
    etap:is(vtree:split_flipped_bbox({-30,-60,-20,70}, {-180,-90,180,90}),
            not_flipped,
            "Not flipped Bbox"),
    ok.


test_area() ->
    etap:plan(5),
    Mbr1 = {10,5,13,15},
    Mbr2 = {-18,-3,-10,-1},
    Mbr3 = {-21,2,-10,14},
    Mbr4 = {5,-32,19,-25},
    Mbr5 = {-5,-16,4,19},
    etap:is(vtree:area(Mbr1), 30, "Area of MBR in the NE"),
    etap:is(vtree:area(Mbr2), 16, "Area of MBR in the SW"),
    etap:is(vtree:area(Mbr3), 132, "Area of MBR in the NW"),
    etap:is(vtree:area(Mbr4), 98, "Area of MBR in the SE"),
    etap:is(vtree:area(Mbr5), 315, "Area of MBR covering all quadrants"),
    ok.

test_merge_mbr() ->
    etap:plan(7),
    Mbr1 = {10,5,13,15},
    Mbr2 = {-18,-3,-10,-1},
    Mbr3 = {-21,2,-10,14},
    Mbr4 = {5,-32,19,-25},
    etap:is(vtree:merge_mbr(Mbr1, Mbr2), {-18, -3, 13, 15},
            "Merge MBR of MBRs in NE and SW"),
    etap:is(vtree:merge_mbr(Mbr1, Mbr3), {-21, 2, 13, 15},
            "Merge MBR of MBRs in NE and NW"),
    etap:is(vtree:merge_mbr(Mbr1, Mbr4), {5, -32, 19, 15},
            "Merge MBR of MBRs in NE and SE"),
    etap:is(vtree:merge_mbr(Mbr2, Mbr3), {-21, -3, -10, 14},
            "Merge MBR of MBRs in SW and NW"),
    etap:is(vtree:merge_mbr(Mbr2, Mbr4), {-18, -32, 19, -1},
            "Merge MBR of MBRs in SW and SE"),
    etap:is(vtree:merge_mbr(Mbr3, Mbr4), {-21, -32, 19, 14},
            "Merge MBR of MBRs in NW and SE"),
    etap:is(vtree:merge_mbr(Mbr1, Mbr1), Mbr1,
            "Merge MBR of equal MBRs"),
    ok.

test_find_area_min_nth() ->
    etap:plan(5),
    etap:is(vtree:find_area_min_nth([{5, {23,64,24,79}}]), 1,
            "Find position of minimum area in a list with one element"),
    etap:is(vtree:find_area_min_nth([{538, {2,64,4,79}}, {29, {2,64,4,79}}]), 2,
            "Find position of minimum area in a list with two elements (1>2)"),
    etap:is(vtree:find_area_min_nth([{54, {2,64,4,79}}, {538, {2,64,4,79}}]), 1,
            "Find position of minimum area in a list with two elements (1<2)"),
    etap:is(vtree:find_area_min_nth([{54, {2,64,4,79}}, {54, {2,64,4,79}}]), 1,
            "Find position of minimum area in a list with two equal elements"),
    etap:is(vtree:find_area_min_nth(
              [{329, {2,64,4,79}}, {930, {2,64,4,79}}, {203, {2,64,4,79}},
               {72, {2,64,4,79}}, {402, {2,64,4,79}}, {2904, {2,64,4,79}},
               {283, {2,64,4,79}}]), 4,
            "Find position of minimum area in a list"),
    ok.

test_partition_node() ->
    etap:plan(3),
    Node1 = {{10,5,13,15}, #node{type=leaf}, <<"Node1">>},
    Node2 = {{-18,-3,-10,-1}, #node{type=leaf}, <<"Node2">>},
    Node3 = {{-21,2,-10,14}, #node{type=leaf}, <<"Node3">>},
    Node4 = {{5,-32,19,-25}, #node{type=leaf}, <<"Node4">>},
    Node5 = {{-5,-16,4,19}, #node{type=leaf}, <<"Node5">>},
    Children3 = [Node1, Node2, Node4],
    Children4 = Children3 ++ [Node3],
    Children5 = Children4 ++ [Node5],
    Mbr1_2_4 = {-18,-25,19,15},
    Mbr1_2_3_4_5 = {-21,-25,19,19},
    etap:is(vtree:partition_node({Mbr1_2_4, #node{type=leaf}, Children3}),
            {[Node2], [Node4], [Node1, Node4], [Node1, Node2]},
            "Partition 3 nodes"),
    etap:is(vtree:partition_node({Mbr1_2_3_4_5, #node{type=leaf}, Children4}),
            {[Node2, Node3], [Node4],
             [Node1, Node4], [Node1, Node2, Node3]},
            "Partition 4 nodes"),
    etap:is(vtree:partition_node({Mbr1_2_3_4_5, #node{type=leaf}, Children5}),
            {[Node2, Node3], [Node4],
             [Node1, Node4, Node5], [Node1, Node2, Node3, Node5]},
            "Partition 5 nodes"),
    ok.


test_calc_mbr() ->
    etap:plan(9),
    Mbr1 = {10,5,13,15},
    Mbr2 = {-18,-3,-10,-1},
    Mbr3 = {-21,2,-10,14},
    Mbr4 = {5,-32,19,-25},
    etap:is(vtree:calc_mbr([]), error,
            "Calculate MBR of an empty list"),
    etap:is(vtree:calc_mbr([Mbr1]), {10, 5, 13, 15},
            "Calculate MBR of a single MBR"),
    etap:is(vtree:calc_mbr([Mbr1, Mbr2]), {-18, -3, 13, 15},
            "Calculate MBR of MBRs in NE and SW"),
    etap:is(vtree:calc_mbr([Mbr1, Mbr3]), {-21, 2, 13, 15},
            "Calculate MBR of MBRs in NE and NW"),
    etap:is(vtree:calc_mbr([Mbr1, Mbr4]), {5, -32, 19, 15},
            "Calculate MBR of MBRs in NE and SE"),
    etap:is(vtree:calc_mbr([Mbr2, Mbr3]), {-21, -3, -10, 14},
            "Calculate MBR of MBRs in SW and NW"),
    etap:is(vtree:calc_mbr([Mbr2, Mbr4]), {-18, -32, 19, -1},
            "Calculate MBR of MBRs in SW and SE"),
    etap:is(vtree:calc_mbr([Mbr3, Mbr4]), {-21, -32, 19, 14},
            "Calculate MBR of MBRs in NW and SE"),
    etap:is(vtree:calc_mbr([Mbr1, Mbr2, Mbr3]), {-21, -3, 13, 15},
            "Calculate MBR of MBRs in NE, SW, NW"),
    etap:is(vtree:calc_mbr([Mbr1, Mbr2, Mbr4]), {-18, -32, 19, 15},
            "Calculate MBR of MBRs in NE, SW, SE"),
    ok.

test_calc_nodes_mbr() ->
    etap:plan(9),
    Node1 = {{10,5,13,15}, #node{type=leaf}, <<"Node1">>},
    Node2 = {{-18,-3,-10,-1}, #node{type=leaf}, <<"Node2">>},
    Node3 = {{-21,2,-10,14}, #node{type=leaf}, <<"Node3">>},
    Node4 = {{5,-32,19,-25}, #node{type=leaf}, <<"Node4">>},
    etap:is(vtree:calc_nodes_mbr([Node1]), {10, 5, 13, 15},
            "Calculate MBR of a single nodes"),
    etap:is(vtree:calc_nodes_mbr([Node1, Node2]), {-18, -3, 13, 15},
            "Calculate MBR of nodes in NE and SW"),
    etap:is(vtree:calc_nodes_mbr([Node1, Node3]), {-21, 2, 13, 15},
            "Calculate MBR of nodes in NE and NW"),
    etap:is(vtree:calc_nodes_mbr([Node1, Node4]), {5, -32, 19, 15},
            "Calculate MBR of nodes in NE and SE"),
    etap:is(vtree:calc_nodes_mbr([Node2, Node3]), {-21, -3, -10, 14},
            "Calculate MBR of nodes in SW and NW"),
    etap:is(vtree:calc_nodes_mbr([Node2, Node4]), {-18, -32, 19, -1},
            "Calculate MBR of nodes in SW and SE"),
    etap:is(vtree:calc_nodes_mbr([Node3, Node4]), {-21, -32, 19, 14},
            "Calculate MBR of nodes in NW and SE"),
    etap:is(vtree:calc_nodes_mbr([Node1, Node2, Node3]), {-21, -3, 13, 15},
            "Calculate MBR of nodes in NE, SW, NW"),
    etap:is(vtree:calc_nodes_mbr([Node1, Node2, Node4]), {-18, -32, 19, 15},
            "Calculate MBR of nodes in NE, SW, SE"),
    ok.

test_best_split() ->
    etap:plan(4),
    Node1 = {{10,5,13,15}, #node{type=leaf}, <<"Node1">>},
    Node2 = {{-18,-3,-10,-1}, #node{type=leaf}, <<"Node2">>},
    Node3 = {{-21,2,-10,14}, #node{type=leaf}, <<"Node3">>},
    Node4 = {{5,-32,19,-25}, #node{type=leaf}, <<"Node4">>},
    Node5 = {{-5,-16,4,19}, #node{type=leaf}, <<"Node5">>},
    Node6 = {{-15,10,-12,17}, #node{type=leaf}, <<"Node6">>},
    {Mbr2, _, _} = Node2,
    {Mbr4, _, _} = Node4,
    Mbr1_2 = {-18,-3,13,15},
    Mbr1_4 = {5,-32,19,15},
    Mbr1_4_5 = {-5,-32,19,19},
    Mbr2_3 = {-21,-3,-10,14},
    Mbr4_6 = {-15,-32,19,17},
    Partition3 = {[Node2], [Node4], [Node1, Node4], [Node1, Node2]},
    Partition4 = {[Node2, Node3], [Node4],
                  [Node1, Node4], [Node1, Node2, Node3]},
    Partition5 = {[Node2, Node3], [Node4],
                  [Node1, Node4, Node5], [Node1, Node2, Node3, Node5]},
    Partition4b = {[Node2], [Node4, Node6],
                   [Node1, Node4, Node6], [Node1, Node2]},
    etap:is(vtree:best_split(Partition3), {tie, {Mbr2, Mbr4, Mbr1_4, Mbr1_2}},
            "Best split: tie (3 nodes)"),
    etap:is(vtree:best_split(Partition4), [{Mbr2_3, [Node2, Node3]},
                                           {Mbr1_4, [Node1, Node4]}],
            "Best split: horizontal (W/E) nodes win (4 nodes)"),
    etap:is(vtree:best_split(Partition5), [{Mbr2_3, [Node2, Node3]},
                                           {Mbr1_4_5, [Node1, Node4, Node5]}],
            "Best split: horizontal (W/E) nodes win (5 nodes)"),
    etap:is(vtree:best_split(Partition4b), [{Mbr4_6, [Node4, Node6]},
                                            {Mbr1_2, [Node1, Node2]}],
            "Best split: vertical (S/N) nodes win (4 nodes)"),
    ok.

test_minimal_overlap() ->
    % XXX vmx: test fir S/N split is missing
    etap:plan(2),
    Node1 = {{10,5,13,15}, <<"Node1">>},
    Node2 = {{-18,-3,-10,-1}, <<"Node2">>},
    Node3 = {{-21,2,-10,14}, <<"Node3">>},
    Node4 = {{5,-32,19,-25}, <<"Node4">>},
    Node5 = {{-11,-9,12,10}, <<"Node5">>},
    {Mbr2, _} = Node2,
    {Mbr4, _} = Node4,
    Mbr1_2 = {-18,-3,13,15},
    Mbr1_3 = {-21,2,13,15},
    Mbr1_4 = {5,-32,19,15},
    Mbr1_4_5 = {-11,-32,19,15},
    Mbr2_3 = {-21,-3,-10,14},
    Mbr2_4_5 = {-18,-32,19,10},
    Partition3 = {[Node2], [Node4], [Node1, Node4], [Node1, Node2]},
    Partition5 = {[Node2, Node3], [Node2, Node4, Node5],
                  [Node1, Node4, Node5], [Node1, Node3]},
    etap:is(vtree:minimal_overlap(
                Partition5, {Mbr2_3, Mbr2_4_5, Mbr1_4_5, Mbr1_3}),
            [{Mbr2_3, [Node2, Node3]}, {Mbr1_4_5, [Node1, Node4, Node5]}],
            "Minimal Overlap: horizontal (W/E) nodes win (5 Nodes)"),
    etap:is(vtree:minimal_overlap(Partition3, {Mbr2, Mbr4, Mbr1_4, Mbr1_2}),
            tie, "Minimal Overlap: tie"),
    ok.


test_minimal_coverage() ->
    % XXX vmx: test for equal coverage is missing
    etap:plan(2),
    Node1 = {{10,5,13,15}, <<"Node1">>},
    Node2 = {{-18,-3,-10,-1}, <<"Node2">>},
    Node3 = {{-21,2,-10,14}, <<"Node3">>},
    Node4 = {{5,-32,19,-25}, <<"Node4">>},
    Node5 = {{-11,-9,12,10}, <<"Node5">>},
    Node6 = {{-11,-9,12,24}, <<"Node6">>},
    {Mbr4, _} = Node4,
    Mbr1_3 = {-21,2,13,15},
    Mbr1_2_3_6 = {-21,-9,13,24},
    Mbr1_4_5 = {-11,-32,19,15},
    Mbr1_4_6 = {-11,-32,19,24},
    Mbr2_3 = {-21,-3,-10,14},
    Mbr2_4_5 = {-18,-32,19,10},
    Partition5 = {[Node2, Node3], [Node2, Node4, Node5],
                  [Node1, Node4, Node5], [Node1, Node3]},
    Partition6 = {[Node2, Node3], [Node4],
                  [Node1, Node4, Node6], [Node1, Node2, Node3, Node6]},
    etap:is(vtree:minimal_coverage(
                Partition5, {Mbr2_3, Mbr2_4_5, Mbr1_4_5, Mbr1_3}),
            [{Mbr2_3, [Node2, Node3]}, {Mbr1_4_5, [Node1, Node4, Node5]}],
            "Minimal Overlap: horizontal (W/E) nodes win)"),
    etap:is(vtree:minimal_coverage(
                Partition6, {Mbr2_3, Mbr4, Mbr1_4_6, Mbr1_2_3_6}),
            [{Mbr4, [Node4]}, {Mbr1_2_3_6, [Node1, Node2, Node3, Node6]}],
            "Minimal Overlap: vertical (S/N) nodes win"),
    ok.


test_calc_overlap() ->
    etap:plan(7),
    Mbr1 = {10,5,13,15},
    Mbr2 = {-18,-3,-10,-1},
    Mbr3 = {-21,2,-10,14},
    Mbr4 = {5,-32,19,-25},
    Mbr5 = {-11,-9,12,10},
    Mbr6 = {-5,-6,4,9},
    Mbr7 = {4,-11,20,-3},
    etap:is(vtree:calc_overlap(Mbr1, Mbr5), {10, 5, 12, 10},
            "Calculate overlap of MBRs in NE and center"),
    etap:is(vtree:calc_overlap(Mbr2, Mbr5), {-11, -3, -10, -1},
            "Calculate overlap of MBRs in SW and center"),
    etap:is(vtree:calc_overlap(Mbr3, Mbr5), {-11, 2, -10, 10},
            "Calculate overlap of MBRs in NW and center"),
    etap:is(vtree:calc_overlap(Mbr7, Mbr5), {4, -9, 12, -3},
            "Calculate overlap of MBRs in SE and center"),
    etap:is(vtree:calc_overlap(Mbr6, Mbr5), {-5, -6, 4, 9},
            "Calculate overlap of one MBRs enclosing the other (1)"),
    etap:is(vtree:calc_overlap(Mbr5, Mbr6), {-5, -6, 4, 9},
            "Calculate overlap of one MBRs enclosing the other (2)"),
    etap:is(vtree:calc_overlap(Mbr4, Mbr5), {0, 0, 0, 0},
            "Calculate overlap of MBRs with no overlap"),
    ok.

test_delete() ->
    etap:plan(9),

    {ok, Fd} = case couch_file:open(?FILENAME, [create, overwrite]) of
    {ok, Fd2} ->
        {ok, Fd2};
    {error, Reason} ->
        io:format("ERROR (~s): Couldn't open file (~s) for tree storage~n",
                  [Reason, ?FILENAME])
    end,

    Node1 = {{10,5,13,15}, #node{type=leaf}, <<"Node1">>},
    Node2 = {{-18,-3,-10,-1}, #node{type=leaf}, <<"Node2">>},
    Node3 = {{-21,2,-10,14}, #node{type=leaf}, <<"Node3">>},
    Node4 = {{5,-32,19,-25}, #node{type=leaf}, <<"Node4">>},
    Node5 = {{-5,-16,4,19}, #node{type=leaf}, <<"Node5">>},
    % od = on disk
    Node1od = {{10,5,13,15}, #node{type=leaf}, {<<"Node1">>,<<"Node1">>}},
    Node2od = {{-18,-3,-10,-1}, #node{type=leaf}, {<<"Node2">>,<<"Node2">>}},
    Node3od = {{-21,2,-10,14}, #node{type=leaf}, {<<"Node3">>,<<"Node3">>}},
    Node4od = {{5,-32,19,-25}, #node{type=leaf}, {<<"Node4">>,<<"Node4">>}},
    Node5od = {{-5,-16,4,19}, #node{type=leaf}, {<<"Node5">>,<<"Node5">>}},
    Mbr1 = {10,5,13,15},
    Mbr1_2 = {-18,-3,13,15},
    Mbr1_2_3 = {-21,-3,13,15},
    Mbr1_2_3_4 = {-21,-32,19,15},
    Mbr1_2_3_4_5 = {-21,-32,19,19},
    Mbr1_3_4 = {-21,-32,19,15},
    Mbr1_3_4_5 = {-21,-32,19,19},
    Mbr1_4_5 = {-5,-32,19,19},
    Mbr1_4 = {5,-32,19,15},
    {Mbr2, _, _} = Node2,
    Mbr2_3 = {-21,-3,-10,14},
    Mbr2_3_4_5 = {-21,-32,19,19},
    {Mbr3, _, _} = Node3,
    Mbr4_5 = {-5,-32,19,19},

    {ok, Mbr1, 0, 1} = vtree:insert(Fd, nil, <<"Node1">>, Node1),
    {ok, Mbr1_2, Pos2, 1} = vtree:insert(Fd, 0, <<"Node2">>, Node2),
    {ok, Mbr1_2_3, Pos3, 1} = vtree:insert(Fd, Pos2, <<"Node3">>, Node3),
    {ok, Mbr1_2_3_4, Pos4, 1} = vtree:insert(Fd, Pos3, <<"Node4">>, Node4),
    {ok, Mbr1_2_3_4_5, Pos5, 2} = vtree:insert(Fd, Pos4, <<"Node5">>, Node5),
    Tree3 = {Mbr3, #node{type=leaf}, [Node3od]},
    Tree2_3 = {Mbr2_3, #node{type=leaf}, [Node2od, Node3od]},
    Tree4_5 = {Mbr4_5, #node{type=leaf}, [Node4od, Node5od]},
    Tree1_4_5 = {Mbr1_4_5, #node{type=leaf}, [Node1od, Node4od, Node5od]},
    Tree2_3_4_5 = {Mbr2_3_4_5, #node{type=inner}, [Tree2_3, Tree4_5]},
    Tree1_3_4_5 = {Mbr1_3_4_5, #node{type=inner}, [Tree1_4_5, Tree3]},
    Tree1_4 = {Mbr1_4, #node{type=leaf}, [Node1od, Node4od]},
    Tree1_3_4 = {Mbr1_3_4, #node{type=inner}, [Tree3, Tree1_4]},

    {Node1Mbr, _, Node1Id} = Node1,
    {Node2Mbr, _, Node2Id} = Node2,
    {Node3Mbr, _, Node3Id} = Node3,
    {Node4Mbr, _, Node4Id} = Node4,
    {Node5Mbr, _, Node5Id} = Node5,
    etap:is(vtree:delete(Fd, <<"bliblablubfoobar">>, Node1Mbr, Pos2),
            not_found,
            "Delete a node which ID's doesn't exist (tree height=1)"),

    {ok, Pos2_1} = vtree:delete(Fd, Node1Id, Node1Mbr, Pos2),
    etap:is(vtree:get_node(Fd, Pos2_1),
            {ok, {Mbr2, #node{type=leaf}, [Node2od]}},
            "Delete a node (tree height=1) (a)"),

    {ok, Pos2_2} = vtree:delete(Fd, Node2Id, Node2Mbr, Pos2),
    etap:is(vtree:get_node(Fd, Pos2_2),
            {ok, {Mbr1, #node{type=leaf}, [Node1od]}},
            "Delete a node (tree height=1) (b)"),

    {ok, Pos5_1} = vtree:delete(Fd, Node1Id, Node1Mbr, Pos5),
    {ok, {Pos5_1Mbr, Pos5_1Meta, [Pos5_1C1, Pos5_1C2]}} = vtree:get_node(
                                                            Fd, Pos5_1),
    {ok, Pos5_1Child1} = vtree:get_node(Fd, Pos5_1C1),
    {ok, Pos5_1Child2} = vtree:get_node(Fd, Pos5_1C2),
    etap:is({Pos5_1Mbr, Pos5_1Meta, [Pos5_1Child1, Pos5_1Child2]}, Tree2_3_4_5,
            "Delete a node (tree height=2) (a)"),

    {ok, Pos5_2} = vtree:delete(Fd, Node2Id, Node2Mbr, Pos5),
    {ok, {Pos5_2Mbr, Pos5_2Meta, [Pos5_2C1, Pos5_2C2]}} = vtree:get_node(
                                                            Fd, Pos5_2),
    {ok, Pos5_2Child1} = vtree:get_node(Fd, Pos5_2C1),
    {ok, Pos5_2Child2} = vtree:get_node(Fd, Pos5_2C2),
    etap:is({Pos5_2Mbr, Pos5_2Meta, [Pos5_2Child1, Pos5_2Child2]}, Tree1_3_4_5,
            "Delete a node (tree height=2) (b)"),

    {ok, Pos5_3} = vtree:delete(Fd, Node3Id, Node3Mbr, Pos5_2),
    {ok, {Pos5_3Mbr, Pos5_3Meta, [Pos5_3C]}} = vtree:get_node(Fd, Pos5_3),
    {ok, Pos5_3Child} = vtree:get_node(Fd, Pos5_3C),
    etap:is({Pos5_3Mbr, Pos5_3Meta, [Pos5_3Child]},
            {Mbr1_4_5, #node{type=inner}, [Tree1_4_5]},
            "Delete a node which is the only child (tree height=2) (b)"),

    {ok, Pos5_4} = vtree:delete(Fd, Node5Id, Node5Mbr, Pos5_2),
    {ok, {Pos5_4Mbr, Pos5_4Meta, [Pos5_4C1, Pos5_4C2]}} = vtree:get_node(
                                                            Fd, Pos5_4),
    {ok, Pos5_4Child1} = vtree:get_node(Fd, Pos5_4C1),
    {ok, Pos5_4Child2} = vtree:get_node(Fd, Pos5_4C2),
    etap:is({Pos5_4Mbr, Pos5_4Meta, [Pos5_4Child1, Pos5_4Child2]}, Tree1_3_4,
            "Delete a node (tree height=2) (b)"),

    % previous tests test the same code path already
    {ok, Pos5_5} = vtree:delete(Fd, Node4Id, Node4Mbr, Pos5_4),
    {ok, Pos5_6} = vtree:delete(Fd, Node3Id, Node3Mbr, Pos5_5),

    etap:is(vtree:delete(Fd, Node1Id, Node1Mbr, Pos5_6), {empty, nil},
            "After deletion of node, the tree is empty (tree height=2)"),

    etap:is(vtree:delete(Fd, Node5Id, Node5Mbr, Pos5_6), not_found,
            "Node can't be found (tree height=2)"),
    ok.


test_split_node() ->
    etap:plan(3),

    NodeToSplit1 =  {{52,45,969,960},{node,leaf}, [
        {{52,320,597,856},{node,leaf},{<<"Node7">>,<<"Node7">>}},
        {{270,179,584,331},{node,leaf},{<<"Node9">>,<<"Node9">>}},
        {{441,502,580,960},{node,leaf},{<<"Node13">>,<<"Node13">>}},
        {{462,520,543,911},{node,leaf},{<<"Node15">>,<<"Node15">>}},
        {{493,45,969,938},{node,leaf},{<<"Node17">>,<<"Node17">>}}]},

    {SplittedMbr1, Node1a, Node1b} = vtree:split_node(NodeToSplit1),
    etap:is(SplittedMbr1, vtree:calc_nodes_mbr(element(3, NodeToSplit1)),
        "Split node with one item too much (MBR is right)"),
    etap:is(length(element(3, Node1a)), 3,
        "Split node with one item too much (correct number of children (a))"),
    etap:is(length(element(3, Node1b)), 2,
        "Split node with one item too much (correct number of children (b))"),
    ok.

test_count_total() ->
    etap:plan(2),

    {ok, {Fd1, {RootPos1, _}}} = vtree_test:build_random_tree(
            "/tmp/randtree.bin", 20),
    Count1 = vtree:count_total(Fd1, RootPos1),
    etap:is(Count1, 20, "Total number of geometries is correct (a)"),

    {ok, {Fd2, {RootPos2, _}}} = vtree_test:build_random_tree(
            "/tmp/randtree.bin", 338),
    Count2 = vtree:count_total(Fd2, RootPos2),
    etap:is(Count2, 338, "Total number of geometries is correct (b)"),

    Count3 = vtree:count_total(Fd2, nil),
    etap:is(Count3, 0,
            "Total number of geometries is correct (for empty tree)"),
    ok.


-spec build_random_tree(Filename::string(), Num::integer()) ->
        {ok, {file:io_device(), {integer(), integer()}}} | {error, string()}.
build_random_tree(Filename, Num) ->
    build_random_tree(Filename, Num, {654, 642, 698}).
-spec build_random_tree(Filename::string(), Num::integer(),
        Seed::{integer(), integer(), integer()}) ->
        {ok, {file:io_device(), {integer(), integer()}}} | {error, string()}.
build_random_tree(Filename, Num, Seed) ->
    random:seed(Seed),
    case couch_file:open(Filename, [create, overwrite]) of
    {ok, Fd} ->
        Max = 1000,
        {Tree, TreeHeight} = lists:foldl(
            fun(Count, {CurTreePos, _CurTreeHeight}) ->
                {W, X, Y, Z} = {random:uniform(Max), random:uniform(Max),
                                random:uniform(Max), random:uniform(Max)},
                RandomMbr = {erlang:min(W, X), erlang:min(Y, Z),
                             erlang:max(W, X), erlang:max(Y, Z)},
                %io:format("~p~n", [RandomMbr]),
                {ok, _, NewRootPos, NewTreeHeight} = vtree:insert(
                    Fd, CurTreePos,
                    list_to_binary("Node" ++ integer_to_list(Count)),
                    {RandomMbr, #node{type=leaf},
                     list_to_binary("Node" ++ integer_to_list(Count))}),
                %io:format("test_insertion: ~p~n", [NewRootPos]),
                {NewRootPos, NewTreeHeight}
            end, {nil, 0}, lists:seq(1,Num)),
        %io:format("Tree: ~p~n", [Tree]),
        {ok, {Fd, {Tree, TreeHeight}}};
    {error, _Reason} ->
        io:format("ERROR: Couldn't open file (~s) for tree storage~n",
                  [Filename])
    end.

% @doc Create a random node. Return the ID of the node and the node itself.
-spec random_node() -> {string(), tuple()}.
random_node() ->
    random_node({654, 642, 698}).
-spec random_node(Seed::{integer(), integer(), integer()}) -> {string(), tuple()}.
random_node(Seed) ->
    random:seed(Seed),
    Max = 1000,
    {W, X, Y, Z} = {random:uniform(Max), random:uniform(Max),
                    random:uniform(Max), random:uniform(Max)},
    RandomMbr = {erlang:min(W, X), erlang:min(Y, Z),
                 erlang:max(W, X), erlang:max(Y, Z)},
    {list_to_binary("Node" ++ integer_to_list(Y) ++ integer_to_list(Z)),
     {RandomMbr, #node{type=leaf},
      list_to_binary("Value" ++ integer_to_list(Y) ++ integer_to_list(Z))}}.
