#!/usr/bin/env escript
%% -*- erlang -*-
%%! -na me insert@127.0.0.1

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

-include_lib("../include/vtree.hrl").

-define(MOD, vtree_modify).
-define(FILENAME, "/tmp/vtree_modify_vtree.bin").

main(_) ->
    % Set the random seed once. It might be reset a certain tests
    random:seed(1, 11, 91),

    % Apache CouchDB doesn't have the couch_file_write_guard module
    try
        couch_file_write_guard:sup_start_link()
    catch error:undef ->
        ok
    end,

    code:add_pathz(filename:dirname(escript:script_name())),
    etap:plan(37),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            % Somehow etap:diag/1 and etap:bail/1 don't work properly
            %etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            %etap:bail(Other),
            io:format(standard_error, "Test died abnormally:~n~p~n", [Other])
     end.


test() ->
    test_write_new_root(),
    test_insert_into_nodes(),
    test_get_key(),
    test_get_chunk_threshold(),
    test_get_overflowing_subset(),
    test_write_nodes(),
    test_write_multiple_nodes(),
    test_split_node(),
    % NOTE vmx 2012-11-07: modify_multiple/5 isn't tested, as the
    %     vtree_insert:insert/2 and vtree_delete:delete/2 already test it
    ok.


test_write_new_root() ->
    Less = fun(A, B) -> A < B end,
    Fd = vtree_test_util:create_file(?FILENAME),

    NodesKp = vtree_test_util:generate_kpnodes(5),

    Vtree1 = #vtree{
                kp_chunk_threshold = (?ext_size(NodesKp)/5)*4.5,
                min_fill_rate = 0.4,
                less = Less,
                fd = Fd
               },


    Root1 = ?MOD:write_new_root(Vtree1, [hd(NodesKp)]),
    etap:is(Root1, hd(NodesKp), "Single node is new root"),

    Root2 = ?MOD:write_new_root(Vtree1, tl(NodesKp)),
    Root2Children = vtree_io:read_node(Fd, Root2#kp_node.childpointer),
    etap:is(Root2Children, tl(NodesKp),
            "A new root node was written (one new level)"),

    Root3 = ?MOD:write_new_root(Vtree1, NodesKp),
    Root3Children = vtree_io:read_node(Fd, Root3#kp_node.childpointer),
    ChildPointer = [C#kp_node.childpointer || C <- Root3Children],
    Root3ChildrenChildren = lists:append(
                              [vtree_io:read_node(Fd, C#kp_node.childpointer)
                               || C <- Root3Children]),
    etap:is(lists:sort(Root3ChildrenChildren), lists:sort(NodesKp),
            "A new root node was written (two new levels)"),

    NodesKv = vtree_test_util:generate_kvnodes(4),
    MbbOKv = (hd(NodesKv))#kv_node.key,

    Root4 = ?MOD:write_new_root(Vtree1, NodesKv),
    Root4Children = vtree_io:read_node(Fd, Root4#kp_node.childpointer),
    etap:is(Root4Children, [N#kv_node{size=0} || N <- NodesKv],
            "A new root node (for KV-nodes) was written (one new level)"),

    couch_file:close(Fd).


test_insert_into_nodes() ->
    NodeSize = 240,
    Tests = [
             {4*NodeSize, 0.4},
             {6*NodeSize, 0.4},
             {30*NodeSize, 0.3}
            ],
    lists:foreach(fun({FillMin, FillMax}) ->
                          insert_into_nodes(FillMin, FillMax)
                  end, Tests).

insert_into_nodes(FillMax, MinRate) ->
    Less = fun(A, B) -> A < B end,

    Vtree = #vtree{
               kp_chunk_threshold = FillMax,
               min_fill_rate = MinRate,
               less = Less
              },

    Nodes1 = vtree_test_util:generate_kpnodes(4),
    Nodes2 = vtree_test_util:generate_kpnodes(20),
    MbbO = (hd(Nodes1))#kp_node.key,

    Inserted = ?MOD:insert_into_nodes(Vtree, [Nodes1], MbbO, Nodes2),
    etap:is(length(lists:append(Inserted)), 24,
            "All nodes got inserted"),

    NodesFilledOkFun =
        fun(N) ->
                NSize = ?ext_size(N),
                NSize >= FillMax*MinRate andalso NSize =< FillMax
        end,
    etap:ok(lists:all(NodesFilledOkFun, Inserted),
            "All child nodes have the right number of nodes"),

    NodesSize = ?ext_size([Nodes1, Nodes2]),
    etap:ok(length(Inserted) >= NodesSize/FillMax andalso
            length(Inserted) =< NodesSize/(FillMax*MinRate),
            "Right number of child nodes").


test_get_key() ->
    [KvNode] = vtree_test_util:generate_kvnodes(1),
    [KpNode] = vtree_test_util:generate_kpnodes(1),

    etap:is(?MOD:get_key(KvNode), KvNode#kv_node.key,
            "Returns the key of a KV-node"),
    etap:is(?MOD:get_key(KpNode), KpNode#kp_node.key,
            "Returns the key of a KP-node").


test_get_chunk_threshold() ->
    [KvNode] = vtree_test_util:generate_kvnodes(1),
    [KpNode] = vtree_test_util:generate_kpnodes(1),
    Vtree = #vtree{
               kp_chunk_threshold = 720,
               kv_chunk_threshold = 1420
              },

    etap:is(?MOD:get_chunk_threshold(Vtree, KvNode),
            Vtree#vtree.kv_chunk_threshold,
            "Returns the chunk threshold for a KV-node"),
    etap:is(?MOD:get_chunk_threshold(Vtree, KpNode),
            Vtree#vtree.kp_chunk_threshold,
            "Returns the chunk threshold for a KP-node").


test_get_overflowing_subset() ->
    Nodes1 = vtree_test_util:generate_kvnodes(10),

    SplitSize1 = ?ext_size(Nodes1)/3,
    etap:is(?MOD:get_overflowing_subset(SplitSize1, Nodes1),
            lists:split(4, Nodes1),
            "Splitted at the right position (a)"),
    SplitSize2 = ?ext_size(Nodes1)/2,
    etap:is(?MOD:get_overflowing_subset(SplitSize2, Nodes1),
            lists:split(5, Nodes1),
            "Splitted at the right position (b)"),
    SplitSize3 = ?ext_size(Nodes1),
    etap:is(?MOD:get_overflowing_subset(SplitSize3, Nodes1),
            lists:split(10, Nodes1),
            "Splitted at the right position (c)"),

    Nodes2 = vtree_test_util:generate_kvnodes(33),
    SplitSize4 = ?ext_size(Nodes2)/3,
    etap:is(?MOD:get_overflowing_subset(SplitSize4, Nodes2),
            lists:split(11, Nodes2),
            "Splitted at the right position (d)").


test_write_nodes() ->
    % This tests depends on the values of the MBBs, hence reset the seed
    random:seed(1, 11, 91),
    Tests = [
             {4, 1, "Less then the maximum number of nodes were written"},
             {5, 1, "The maximum number of nodes were written"},
             {6, 2, "One more than maximum number of nodes were written"},
             {8, 2, "A bit more than maximum number of nodes were written"},
             % The expected 9 nodes depend on the choose_subtree algorithm
             {30, 9, "Way more than maximum number of nodes were written"}
            ],
    lists:foreach(fun({Insert, Expected, Message}) ->
                          write_nodes(Insert, Expected, Message)
                  end, Tests).

write_nodes(Insert, Expected, Message) ->
    Less = fun(A, B) -> A < B end,
    Fd = vtree_test_util:create_file(?FILENAME),
    NodeSize = ?ext_size(vtree_test_util:generate_kpnodes(1))*1.5,

    Nodes = vtree_test_util:generate_kpnodes(Insert),
    Vtree = #vtree{
               kp_chunk_threshold = 730,
               min_fill_rate = 0.3,
               less = Less,
               fd = Fd
              },

    MbbO = (hd(Nodes))#kp_node.key,
    WrittenNodes = ?MOD:write_nodes(Vtree, Nodes, MbbO),
    etap:is(length(WrittenNodes), Expected, Message),
    etap:ok(lists:all(fun(#kp_node{}) -> true; (_) -> false end, WrittenNodes),
            "The return values are KP-nodes"),

    couch_file:close(Fd).


test_write_multiple_nodes() ->
    Less = fun(A, B) -> A < B end,
    Fd1a = vtree_test_util:create_file(?FILENAME),

    Vtree1 = #vtree{
                less = Less,
                fd = Fd1a
               },

    Nodes1a = vtree_test_util:generate_kvnodes(5),
    Nodes1b = vtree_test_util:generate_kvnodes(8),
    WrittenNodes1 = ?MOD:write_multiple_nodes(Vtree1, [Nodes1a, Nodes1b]),
    couch_file:close(Fd1a),
    Fd1b = vtree_test_util:create_file(?FILENAME),
    {ok, WrittenNodes1a} = vtree_io:write_node(Fd1b, Nodes1a, Less),
    {ok, WrittenNodes1b} = vtree_io:write_node(Fd1b, Nodes1b, Less),
    % The #kp_node.mmb_orig isn't set by vtree_io, hence set it manually
    etap:is(WrittenNodes1,
            [WrittenNodes1a#kp_node{mbb_orig=WrittenNodes1a#kp_node.key},
             WrittenNodes1b#kp_node{mbb_orig=WrittenNodes1b#kp_node.key}],
            "Multiple KV-nodes were correctly written"),
    couch_file:close(Fd1b),

    Fd2a = vtree_test_util:create_file(?FILENAME),
    Vtree2 = Vtree1#vtree{fd = Fd2a},
    Nodes2a = vtree_test_util:generate_kpnodes(5),
    Nodes2b = vtree_test_util:generate_kpnodes(8),
    WrittenNodes2 = ?MOD:write_multiple_nodes(Vtree2, [Nodes2a, Nodes2b]),
    couch_file:close(Fd2a),
    Fd2b = vtree_test_util:create_file(?FILENAME),
    {ok, WrittenNodes2a} = vtree_io:write_node(Fd2b, Nodes2a, Less),
    {ok, WrittenNodes2b} = vtree_io:write_node(Fd2b, Nodes2b, Less),
    % The #kp_node.mmb_orig isn't set bt vtree_io, hence set it manually
    etap:is(WrittenNodes2,
            [WrittenNodes2a#kp_node{mbb_orig=WrittenNodes2a#kp_node.key},
             WrittenNodes2b#kp_node{mbb_orig=WrittenNodes2b#kp_node.key}],
            "Multiple KP-nodes were correctly written"),

    couch_file:close(Fd2b).


test_split_node() ->
    KvNodes = vtree_test_util:generate_kvnodes(7),
    KpNodes = vtree_test_util:generate_kpnodes(7),
    Vtree = #vtree{
               kp_chunk_threshold = (?ext_size(KpNodes)/7)*6,
               kv_chunk_threshold = (?ext_size(KvNodes)/7)*6,
               min_fill_rate = 0.4
     },
    KvMbbO = (hd(KvNodes))#kv_node.key,
    KpMbbO = (hd(KpNodes))#kp_node.key,
    NodesFilledOkFun =
        fun(N) ->
                Threshold = case N of
                                [#kv_node{}|_] ->
                                    Vtree#vtree.kv_chunk_threshold;
                                [#kp_node{}|_] ->
                                    Vtree#vtree.kp_chunk_threshold
                            end,
                NSize = ?ext_size(N),
                NSize >= Threshold*Vtree#vtree.min_fill_rate andalso
                    NSize =< Threshold
        end,

    {KvNodesA, KvNodesB} = ?MOD:split_node(Vtree, KvNodes, KvMbbO),
    KvNodesFilledOk = lists:all(NodesFilledOkFun, [KvNodesA, KvNodesB]),
    etap:ok(KvNodesFilledOk,
            "Both partitions contain the right number of nodes"),
    etap:is(lists:sort(lists:append([KvNodesA, KvNodesB])),
            lists:sort(KvNodes),
            "All nodes are still there after the split"),

    {KpNodesA, KpNodesB} = ?MOD:split_node(Vtree, KpNodes, KpMbbO),
    KpNodesFilledOk = lists:all(NodesFilledOkFun, [KpNodesA, KpNodesB]),
    etap:ok(KpNodesFilledOk,
            "Both partitions contain the right number of nodes"),
    etap:is(lists:sort(lists:append([KpNodesA, KpNodesB])),
            lists:sort(KpNodes),
            "All nodes are still there after the split").
