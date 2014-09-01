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
    etap:plan(13),
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
    test_encode_decode_mbb(),
    ok.


test_encode_decode_kvnode_value() ->
    Fd = vtree_test_util:create_file(?FILENAME),
    [Node1, Node2] = vtree_test_util:generate_kvnodes(2),

    {Encoded1, _Size1} = ?MOD:encode_value(Node1),
    % We flush as late as possible, hence for testing it needs to be done
    % manually
    geocouch_file:flush(Fd),
    Decoded1 = ?MOD:decode_kvnode_value(Encoded1),
    etap:is(Decoded1, Node1#kv_node{key=[], docid=nil, size=0},
            "KV-node value got correctly encoded and decoded (a)"),

    {Encoded2, _Size2} = ?MOD:encode_value(Node2),
    geocouch_file:flush(Fd),
    Decoded2 = ?MOD:decode_kvnode_value(Encoded2),
    etap:is(Decoded2, Node2#kv_node{key=[], docid=nil, size=0},
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
    {Encoded1, _Size1} = ?MOD:encode_node(Nodes1),
    % We flush as late as possible, hence for testing it needs to be done
    % manually
    geocouch_file:flush(Fd),
    Decoded1 = ?MOD:decode_node(Encoded1),
    etap:is(Decoded1, [N#kv_node{size=0} || N <- Nodes1],
            "KV-node got correctly encoded and decoded (a)"),

    Nodes2 = vtree_test_util:generate_kvnodes(5),
    {Encoded2, _Size2} = ?MOD:encode_node(Nodes2),
    geocouch_file:flush(Fd),
    Decoded2 = ?MOD:decode_node(Encoded2),
    etap:is(Decoded2, [N#kv_node{size=0} || N <- Nodes2],
            "KV-node got correctly encoded and decoded (b)"),
    couch_file:close(Fd).


test_encode_decode_kpnodes() ->
    Fd = vtree_test_util:create_file(?FILENAME),
    Nodes1 = vtree_test_util:generate_kpnodes(1),
    Nodes2 = vtree_test_util:generate_kpnodes(7),

    {Encoded1, _Size1} = ?MOD:encode_node(Nodes1),
    % We flush as late as possible, hence for testing it needs to be done
    % manually
    geocouch_file:flush(Fd),
    Decoded1 = ?MOD:decode_node(Encoded1),
    etap:is(Decoded1, Nodes1,
            "KP-node got correctly encoded and decoded (a)"),

    {Encoded2, _Size2} = ?MOD:encode_node(Nodes2),
    geocouch_file:flush(Fd),
    Decoded2 = ?MOD:decode_node(Encoded2),
    etap:is(Decoded2, Nodes2,
            "KP-node got correctly encoded and decoded (b)"),
    couch_file:close(Fd).


test_write_read_nodes() ->
    Fd = vtree_test_util:create_file(?FILENAME),
    Nodes1 = vtree_test_util:generate_kvnodes(6),
    Nodes2 = vtree_test_util:generate_kpnodes(2),
    Less = fun(A, B) -> A < B end,

    {ok, ParentNode1} = ?MOD:write_node(Fd, Nodes1, Less),
    NodesWritten1 = ?MOD:read_node(Fd, ParentNode1#kp_node.childpointer),
    etap:is(NodesWritten1, Nodes1,
            "KV-nodes were correctly written and read back"),

    {ok, ParentNode2} = ?MOD:write_node(Fd, Nodes2, Less),
    NodesWritten2 = ?MOD:read_node(Fd, ParentNode2#kp_node.childpointer),
    etap:is(NodesWritten2, Nodes2,
            "KP-nodes were correctly written and read back"),

    couch_file:close(Fd).


test_encode_decode_mbb() ->
    Mbb1 = [{39.93, 48.9483}, {20, 90}, {-29.4, 83}],
    Mbb2 = [{8,9}],
    Mbb3 = [{-39.42, -4.2}, {48, 48}, {0, 3}],

    etap:is(?MOD:decode_mbb(?MOD:encode_mbb(Mbb1)),
            Mbb1,
            "MBB got correctly encoded (a)"),
    etap:is(?MOD:decode_mbb(?MOD:encode_mbb(Mbb2)),
            Mbb2,
            "MBB got correctly encoded (b)"),
    etap:is(?MOD:decode_mbb(?MOD:encode_mbb(Mbb3)),
            Mbb3,
            "MBB got correctly encoded (c)").
