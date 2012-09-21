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

% This module implements the insertion into the vtree. It follows the normal
% R-tree rules and is implementation independent. It just calls out to
% modules for the choosing the correct subtree and splitting the nodes.

% NOTE vmx 2012-09-04: It might make sense to rename this module to
% geocouch_io, as it is more of storage specific to the backend (Apache
% CouchDB vs. Couchbase). I'm not really sure about it, as geocouch_file
% should abstract away from the differences between the backends.
-module(vtree_io).

-include("vtree.hrl").
-include("couch_db.hrl").

-export([write_node/3, read_node/2]).

-ifdef(makecheck).
-compile(export_all).
-endif.


% Writes a node of the tree (which is a list of KV- or KV-nodes) to disk and
% return a KP-node with the corresponding information. No checks on the number
% of nodes is performed, they are just written do disk as given. Potential
% node splitting needs to happen before.
-spec write_node(Fd :: file:io_device(), Nodes :: [#kv_node{} | #kp_node{}],
                 Less :: lessfun()) -> {ok, #kp_node{}}.
write_node(Fd, Nodes, Less) ->
    {Bin, TreeSize} = encode_node(Fd, Nodes),
    {ok, Pointer, Size} = geocouch_file:append_chunk(Fd, Bin),
    geocouch_file:flush(Fd),
    % The enclosing bounding box for all children
    Mbb = vtree_util:nodes_mbb(Nodes, Less),
    % XXX TODO calculate the reduce value somehow
    Reduce = nil,
    KpNode = #kp_node{
      key = Mbb,
      childpointer = Pointer,
      treesize = Size+TreeSize,
      reduce = Reduce
     },
    {ok, KpNode}.


% Read a node from a certain disk position and return the key-value pairs it
% it contains as Erlang records.
% In case of KV nodes it returns only pointers to the geometry and body. No
% longer true, it's not only the pointers, but the real values
-spec read_node(Fd :: file:io_device(), Pointer :: non_neg_integer()) ->
                       [#kp_node{} | #kv_node{}].
read_node(Fd, Pointer) ->
    {ok, BinNode} = geocouch_file:pread_chunk(Fd, Pointer),
    decode_node(Fd, BinNode).


% Returns the binary that will be stored, and the number size of the subtree
% (resp. in case of a KV node, the number of bytes that were written during
% encoding), to make sure the calculation of the subtree size is correct.
-spec encode_node(Fd :: file:io_device(),
                  Nodes :: [#kp_node{} | #kv_node{}]) ->
                         {binary(), non_neg_integer()}.
encode_node(Fd, [#kv_node{}|_]=Nodes) ->
    encode_node(Fd, Nodes, {<<?KV_NODE:8>>, 0});
encode_node(Fd, [#kp_node{}|_]=Nodes) ->
    encode_node(Fd, Nodes, {<<?KP_NODE:8>>, 0}).
-spec encode_node(Fd :: file:io_device(),
                  Nodes :: [#kp_node{} | #kv_node{}],
                  Acc :: {binary(), non_neg_integer()}) ->
                         {binary(), non_neg_integer()}.
encode_node(_Fd, [], Acc) ->
    Acc;
encode_node(Fd, [Node|T], {BinAcc, TreeSizeAcc}) ->
    % TODO vmx 2012-09-05: The key is JSON, store it the same way as the views
    %     do and not as erlang terms
    %BinK = ?term_to_bin(Node#kv_node.key),
    BinK = encode_key(Node),
    SizeK = erlang:size(BinK),
    case SizeK < 4096 of
        true -> ok;
        false -> throw({error, key_too_long})
    end,

    %{BinV, TreeSize} = encode_value(Fd, NodeType, V),
    {BinV, TreeSize} = encode_value(Fd, Node),
    SizeV = erlang:iolist_size(BinV),
    case SizeV < 268435456 of
        true -> ok;
        false -> throw({error, value_too_big})
    end,

    Bin = <<SizeK:12, SizeV:28, BinK/binary, BinV/binary>>,
    encode_node(Fd, T,
                {<<BinAcc/binary, Bin/binary>>, TreeSize + TreeSizeAcc}).


encode_key(#kv_node{}=Node) ->
    % TODO vmx 2012-09-05: The key is JSON, store it the same way as the views
    %     do and not as erlang terms
    ?term_to_bin(Node#kv_node.key);
encode_key(#kp_node{}=Node) ->
    ?term_to_bin(Node#kp_node.key).


% Encode the value of a Key-Value pair. It returns the encoded value and the
% size of the subtree (in case of a KV node, the number of bytes that were
% written during encoding). The treesize is used to calculate the disk usage
% of the data in the tree.
-spec encode_value(Fd :: file:io_device(), Node :: #kv_node{} | #kp_node{}) ->
                          {Bin :: binary(), Size :: non_neg_integer()}.
encode_value(Fd, #kv_node{}=Node) ->
    #kv_node{
              docid = DocId,
              geometry = Geom,
              body = Body
            } = Node,
    SizeDocId = erlang:iolist_size(DocId),
    % Geom is not a WKB yet, hence store it as external terms
    {ok, PointerGeom, SizeGeom} = geocouch_file:append_chunk(
                                    Fd, ?term_to_bin(Geom)),
    {ok, PointerBody, SizeBody} = geocouch_file:append_chunk(Fd, Body),
    % The `SizeDoc` is used to get the actual size stored in disk, hence it
    % includes the body and the geometry
    SizeDoc = SizeGeom + SizeBody,

    case SizeDocId < 4096 of
        true -> ok;
        false -> throw({error, docid_too_long})
    end,
    case SizeDoc < 268435456 of
        true -> ok;
        false -> throw({error, document_too_big})
    end,

    Bin = <<SizeDocId:12, SizeDoc:28, PointerGeom:48, PointerBody:48,
            DocId/binary>>,
    % Returning only the size of the written body and geometry is enough,
    % as all other bytes will be accounted when the whole chunk is written
    {Bin, SizeDoc};
encode_value(_Fd, #kp_node{}=Node) ->
    #kp_node{
              childpointer = PointerNode,
              treesize = TreeSize,
              reduce = Reduce,
              mbb_orig = MbbO
            } = Node,
    BinReduce = ?term_to_bin(Reduce),
    SizeReduce = erlang:iolist_size(BinReduce),
    BinMbbO = ?term_to_bin(MbbO),
    SizeMbbO = erlang:size(BinMbbO),
    % 12 would be enough for MbbO, but we like to have it padded to full bytes
    BinValue = <<PointerNode:48, TreeSize:48, SizeReduce:16,
                 BinReduce:SizeReduce/binary, SizeMbbO:16, BinMbbO/binary>>,
    % Return `0` as no additional bytes are written. The bytes that will
    % be written are accounted when the whole chunk gets written.
    {BinValue, 0}.



% This will only return the pointers to the geometry and body
% NOTE vmx 2012-09-04: Not sure if that is needed
%decode_kvnode_value_shallow(BinValue) ->
%    <<_SizeDocId:12, _SizeDoc:28, PointerGeom:48,
%      PointerBody:48, DocId/binary>> = BinValue,
%    {DocId, PointerGeom, PointerBody}.

% Decode the value of a KV-node pair
-spec decode_kvnode_value(Fd :: file:io_device(), BinValue :: binary()) ->
                                 #kv_node{}.
decode_kvnode_value(Fd, BinValue) ->
    <<_SizeDocId:12, _SizeDoc:28, PointerGeom:48,
      PointerBody:48, DocId/binary>> = BinValue,
    {ok, Geom} = geocouch_file:pread_chunk(Fd, PointerGeom),
    {ok, Body} = geocouch_file:pread_chunk(Fd, PointerBody),
    %{DocId, erlang:binary_to_term(Geom), Body}.
    #kv_node{
              docid = DocId,
              geometry = erlang:binary_to_term(Geom),
              body = Body
            }.


% Decode the value of a KP-node pair
-spec decode_kpnode_value(BinValue :: binary()) -> #kp_node{}.
decode_kpnode_value(BinValue) ->
    <<PointerNode:48, TreeSize:48, SizeReduce:16, BinReduce:SizeReduce/binary,
      SizeMbbO:16, BinMbbO:SizeMbbO/binary>> = BinValue,
    Reduce = erlang:binary_to_term(BinReduce),
    MbbO = erlang:binary_to_term(BinMbbO),
    %{PointerNode, TreeSize, Reduce}.
    #kp_node{
              childpointer = PointerNode,
              treesize = TreeSize,
              reduce = Reduce,
              mbb_orig = MbbO
            }.


% Decode the value of the KP nodes to Erlang terms
-spec decode_node(Fd :: file:io_device(), BinValue :: binary()) ->
                         [#kp_node{} | #kv_node{}].
decode_node(Fd, <<?KV_NODE:8, Rest/binary>>) ->
    decode_kvnode_pairs(Fd, Rest, []);
decode_node(_Fd, <<?KP_NODE:8, Rest/binary>>) ->
    decode_kpnode_pairs(Rest, []).


% Decode KV-nodes key value pairs to an Erlang record
-spec decode_kvnode_pairs(Fd :: file:io_device(), BinValue :: binary(),
                          Acc :: [#kv_node{}]) -> [#kv_node{}].
decode_kvnode_pairs(_Fd, <<>>, Acc) ->
    lists:reverse(Acc);
decode_kvnode_pairs(Fd, Bin, Acc) ->
    <<SizeK:12, SizeV:28, BinK:SizeK/binary, BinV:SizeV/binary,
      Rest/binary>> = Bin,
    Key = erlang:binary_to_term(BinK),
    Node0 = decode_kvnode_value(Fd, BinV),
    Node = Node0#kv_node{key = Key},
    decode_kvnode_pairs(Fd, Rest, [Node|Acc]).


% Decode KP-nodes key value pairs to an Erlang record
-spec decode_kpnode_pairs(BinValue :: binary(), Acc :: [#kp_node{}]) ->
                                 [#kp_node{}].
decode_kpnode_pairs(<<>>, Acc) ->
    lists:reverse(Acc);
decode_kpnode_pairs(Bin, Acc) ->
    <<SizeK:12, SizeV:28, BinK:SizeK/binary, BinV:SizeV/binary,
      Rest/binary>> = Bin,
    Key = erlang:binary_to_term(BinK),
    Node0 = decode_kpnode_value(BinV),
    Node = Node0#kp_node{key = Key},
    decode_kpnode_pairs(Rest, [Node|Acc]).
