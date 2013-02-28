#!/usr/bin/env escript
%% -*- erlang -*-

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

-define(MOD, vtree_io).
-define(FILENAME, "/tmp/vtree_io_vtree.bin").

main(_) ->
    % Set the random seed once, for the whole test suite
    random:seed(1, 11, 91),

    % Apache CouchDB doesn't have the couch_file_write_guard module
    try
        couch_file_write_guard:sup_start_link()
    catch error:undef ->
        ok
    end,

    code:add_pathz(filename:dirname(escript:script_name())),
    etap:plan(88),
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
    test_encode_decode_kvnode_value(),
    test_encode_decode_kpnode_value(),
    test_encode_decode_kvnodes(),
    test_encode_decode_kpnodes(),
    test_write_read_nodes(),
    test_write_kvnode_external(),
    test_read_kvnode_external(),
    test_encode_mbb(),
    test_decode_mbb(),
    ok.


test_encode_decode_kvnode_value() ->
    Fd = vtree_test_util:create_file(?FILENAME),
    [Node1, Node2] = vtree_test_util:generate_kvnodes(2),
    [Node1Ex, Node2Ex] = ?MOD:write_kvnode_external(Fd, [Node1, Node2]),

    {Encoded1, _Size1} = ?MOD:encode_value(Node1Ex),
    % We flush as late as possible, hence for testing it needs to be done
    % manually
    geocouch_file:flush(Fd),
    Decoded1 = ?MOD:decode_kvnode_value(Encoded1),
    etap:is(Decoded1, Node1Ex#kv_node{key=[], size=-1},
            "KV-node value got correctly encoded and decoded (a)"),

    {Encoded2, _Size2} = ?MOD:encode_value(Node2Ex),
    geocouch_file:flush(Fd),
    Decoded2 = ?MOD:decode_kvnode_value(Encoded2),
    etap:is(Decoded2, Node2Ex#kv_node{key=[], size=-1},
            "KV-node value got correctly encoded and decoded (b)"),
    couch_file:close(Fd).


test_encode_decode_kpnode_value() ->
    Fd = vtree_test_util:create_file(?FILENAME),
    [Node1, Node2] = vtree_test_util:generate_kpnodes(2),
    {Encoded1, _Size1} = ?MOD:encode_value(Node1),
    % We flush as late as possible, hence for testing it needs to be done
    % manually
    geocouch_file:flush(Fd),
    Decoded1 = ?MOD:decode_kpnode_value(Encoded1),
    etap:is(Decoded1, Node1#kp_node{key=[]},
            "KP-node value got correctly encoded and decoded (a)"),

    {Encoded2, _Size2} = ?MOD:encode_value(Node2),
    geocouch_file:flush(Fd),
    Decoded2 = ?MOD:decode_kpnode_value(Encoded2),
    etap:is(Decoded2, Node2#kp_node{key=[]},
            "KP-node value got correctly encoded and decoded (b)"),
    couch_file:close(Fd).


test_encode_decode_kvnodes() ->
    Fd = vtree_test_util:create_file(?FILENAME),

    Nodes1 = vtree_test_util:generate_kvnodes(1),
    Nodes1Ex = ?MOD:write_kvnode_external(Fd, Nodes1),
    {Encoded1, _Size1} = ?MOD:encode_node(Fd, Nodes1Ex),
    % We flush as late as possible, hence for testing it needs to be done
    % manually
    geocouch_file:flush(Fd),
    Decoded1 = ?MOD:decode_node(Encoded1),
    etap:is(Decoded1, [N#kv_node{size=-1} || N <- Nodes1Ex],
            "KV-node got correctly encoded and decoded (a)"),

    Nodes2 = vtree_test_util:generate_kvnodes(5),
    Nodes2Ex = ?MOD:write_kvnode_external(Fd, Nodes2),
    {Encoded2, _Size2} = ?MOD:encode_node(Fd, Nodes2Ex),
    geocouch_file:flush(Fd),
    Decoded2 = ?MOD:decode_node(Encoded2),
    etap:is(Decoded2, [N#kv_node{size=-1} || N <- Nodes2Ex],
            "KV-node got correctly encoded and decoded (b)"),
    couch_file:close(Fd).


test_encode_decode_kpnodes() ->
    Fd = vtree_test_util:create_file(?FILENAME),
    Nodes1 = vtree_test_util:generate_kpnodes(1),
    Nodes2 = vtree_test_util:generate_kpnodes(7),

    {Encoded1, _Size1} = ?MOD:encode_node(Fd, Nodes1),
    % We flush as late as possible, hence for testing it needs to be done
    % manually
    geocouch_file:flush(Fd),
    Decoded1 = ?MOD:decode_node(Encoded1),
    etap:is(Decoded1, Nodes1,
            "KP-node got correctly encoded and decoded (a)"),

    {Encoded2, _Size2} = ?MOD:encode_node(Fd, Nodes2),
    geocouch_file:flush(Fd),
    Decoded2 = ?MOD:decode_node(Encoded2),
    etap:is(Decoded2, Nodes2,
            "KP-node got correctly encoded and decoded (b)"),
    couch_file:close(Fd).


test_write_read_nodes() ->
    Fd = vtree_test_util:create_file(?FILENAME),
    Nodes1 = vtree_test_util:generate_kvnodes(6),
    Nodes1Ex = ?MOD:write_kvnode_external(Fd, Nodes1),
    Nodes2 = vtree_test_util:generate_kpnodes(2),
    Less = fun(A, B) -> A < B end,

    {ok, ParentNode1} = ?MOD:write_node(Fd, Nodes1Ex, Less),
    NodesWritten1Ex = ?MOD:read_node(Fd, ParentNode1#kp_node.childpointer),
    NodesWritten1 = ?MOD:read_kvnode_external(Fd, NodesWritten1Ex),
    etap:is(NodesWritten1, Nodes1,
            "KV-nodes were correctly written and read back"),

    {ok, ParentNode2} = ?MOD:write_node(Fd, Nodes2, Less),
    NodesWritten2 = ?MOD:read_node(Fd, ParentNode2#kp_node.childpointer),
    etap:is(NodesWritten2, Nodes2,
            "KP-nodes were correctly written and read back"),

    couch_file:close(Fd).


test_write_kvnode_external() ->
    Fd = vtree_test_util:create_file(?FILENAME),
    Nodes = vtree_test_util:generate_kvnodes(6),

    NodesExternal = ?MOD:write_kvnode_external(Fd, Nodes),
    lists:foreach(fun({Node, External}) ->
                          etap:is(External#kv_node.docid, Node#kv_node.docid,
                                  "docid didn't change"),
                          etap:is(External#kv_node.key, Node#kv_node.key,
                                  "key didn't change"),
                          etap:is(Node#kv_node.size, 0,
                                  "size was originally 0"),
                          etap:ok(External#kv_node.size > 0, "size was set"),
                          {ok, Geom} = geocouch_file:pread_chunk(
                                         Fd, External#kv_node.geometry),
                          etap:is(binary_to_term(Geom), Node#kv_node.geometry,
                                  "geometry didn't change"),
                          {ok, Body} = geocouch_file:pread_chunk(
                                         Fd, External#kv_node.body),
                          etap:is(Body, Node#kv_node.body,
                                  "body didn't change")
                  end, lists:zip(Nodes, NodesExternal)),

    couch_file:close(Fd).


test_read_kvnode_external() ->
    Fd = vtree_test_util:create_file(?FILENAME),
    Less = fun(A, B) -> A < B end,
    Nodes = vtree_test_util:generate_kvnodes(6),

    NodesExternal = ?MOD:write_kvnode_external(Fd, Nodes),
    {ok, ParentNode} = ?MOD:write_node(Fd, NodesExternal, Less),
    NodesWrittenExternal = ?MOD:read_node(Fd, ParentNode#kp_node.childpointer),
    NodesWritten = ?MOD:read_kvnode_external(Fd, NodesWrittenExternal),

    lists:foreach(fun({External, Node}) ->
                          etap:is(Node#kv_node.docid, External#kv_node.docid,
                                  "docid didn't change"),
                          etap:is(Node#kv_node.key, External#kv_node.key,
                                  "key didn't change"),
                          etap:is(External#kv_node.size, -1,
                                  "size is not knwon"),
                          etap:is(Node#kv_node.size, 0,
                                  "size is not set"),
                          {ok, Geom} = geocouch_file:pread_chunk(
                                         Fd, External#kv_node.geometry),
                          etap:is(Node#kv_node.geometry, binary_to_term(Geom),
                                  "geometry didn't change"),
                          {ok, Body} = geocouch_file:pread_chunk(
                                         Fd, External#kv_node.body),
                          etap:is(Node#kv_node.body, Body,
                                  "body didn't change")
                  end, lists:zip(NodesWrittenExternal, NodesWritten)),

    couch_file:close(Fd).


test_encode_mbb() ->
    Mbb1 = [{39.93, 48.9483}, {20, 90}, {-29.4, 83}],
    Mbb2 = [{8,9}],
    Mbb3 = [{-39.42, -4.2}, {48, 48}, {0, 3}],

    etap:is(ejson:decode(?MOD:encode_mbb(Mbb1)),
            [[39.93, 48.9483], [20, 90], [-29.4, 83]],
            "MBB got correctly encoded (a)"),
    etap:is(ejson:decode(?MOD:encode_mbb(Mbb2)),
            [[8, 9]],
            "MBB got correctly encoded (b)"),
    etap:is(ejson:decode(?MOD:encode_mbb(Mbb3)),
            [[-39.42, -4.2], [48, 48], [0, 3]],
            "MBB got correctly encoded (c)").


test_decode_mbb() ->
    Mbb1 = <<"[[39.93,48.9483],[20,90],[-29.4,83]]">>,
    Mbb2 = <<"[[8,9]]">>,
    Mbb3 =  <<"[[-39.42,-4.2],[48,48],[0,3]]">>,

    etap:is(?MOD:decode_mbb(Mbb1),
            [{39.93, 48.9483}, {20, 90}, {-29.4, 83}],
            "MBB got correctly decoded (a)"),
    etap:is(?MOD:decode_mbb(Mbb2),
            [{8,9}],
            "MBB got correctly decoded (b)"),
    etap:is(?MOD:decode_mbb(Mbb3),
            [{-39.42, -4.2}, {48, 48}, {0, 3}],
            "MBB got correctly decoded (c)").
