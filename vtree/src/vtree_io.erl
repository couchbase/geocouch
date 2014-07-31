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

% keys and docIds have the same size
-define(KEY_BITS,       12).
-define(VALUE_BITS,     28).
-define(POINTER_BITS,   48).
-define(TREE_SIZE_BITS, 48).
-define(RED_BITS,       16).
% 12 bits would be enough for MbbO, but we like to have it padded to full bytes
-define(MBBO_BITS,      16).

-define(MAX_KEY_SIZE,     ((1 bsl ?KEY_BITS) - 1)).
-define(MAX_VALUE_SIZE,   ((1 bsl ?VALUE_BITS) - 1)).


% Writes a node of the tree (which is a list of KV- or KV-nodes) to disk and
% return a KP-node with the corresponding information. No checks on the number
% of nodes is performed, they are just written do disk as given. Potential
% node splitting needs to happen before.
-spec write_node(Fd :: file:io_device(), Nodes :: [#kv_node{} | #kp_node{}],
                 Less :: lessfun()) -> {ok, #kp_node{}}.
write_node(Fd, Nodes, Less) ->
    {Bin, TreeSize} = encode_node(Nodes),
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


-spec read_node(Fd :: file:io_device(), Pointer :: non_neg_integer()) ->
                       [#kp_node{} | #kv_node{}].
read_node(Fd, Pointer) ->
    {ok, BinNode} = geocouch_file:pread_chunk(Fd, Pointer),
    decode_node(BinNode).


% Returns the binary that will be stored, and the number size of the subtree
% (resp. in case of a KV node, the number of bytes that were written during
% encoding), to make sure the calculation of the subtree size is correct.
-spec encode_node(Nodes :: [#kp_node{} | #kv_node{}]) ->
                         {binary(), non_neg_integer()}.
encode_node([#kv_node{}|_]=Nodes) ->
    encode_node(Nodes, {<<?KV_NODE:8>>, 0});
encode_node([#kp_node{}|_]=Nodes) ->
    encode_node(Nodes, {<<?KP_NODE:8>>, 0}).
-spec encode_node(Nodes :: [#kp_node{} | #kv_node{}],
                  Acc :: {binary(), non_neg_integer()}) ->
                         {binary(), non_neg_integer()}.
encode_node([], Acc) ->
    Acc;
encode_node([Node|T], {BinAcc, TreeSizeAcc}) ->
    BinK = encode_key(Node),
    SizeK = erlang:size(BinK),
    case SizeK < ?MAX_KEY_SIZE of
        true -> ok;
        false -> throw({error, key_too_long})
    end,

    {BinV, TreeSize} = encode_value(Node),
    SizeV = erlang:iolist_size(BinV),
    case SizeV < ?MAX_VALUE_SIZE of
        true -> ok;
        false -> throw({error, value_too_big})
    end,

    Bin = <<SizeK:?KEY_BITS, SizeV:?VALUE_BITS, BinK/binary, BinV/binary>>,
    encode_node(T, {<<BinAcc/binary, Bin/binary>>, TreeSize + TreeSizeAcc}).


encode_key(#kv_node{key = Key, docid = DocId}) ->
    encode_key_docid(Key, DocId);
encode_key(#kp_node{key = Mbb}) ->
    BinMbb = encode_mbb(Mbb),
    <<(length(Mbb) * 2):16, BinMbb/binary>>.


% Encode the value of a Key-Value pair. It returns the encoded value and the
% size of the subtree (in case of a KV node, the number of bytes that were
% written during encoding). The treesize is used to calculate the disk usage
% of the data in the tree.
-spec encode_value(Node :: #kv_node{} | #kp_node{}) ->
                          {Bin :: binary(), Size :: non_neg_integer()}.
encode_value(#kv_node{}=Node) ->
    #kv_node{
        body = Body
      } = Node,
    {Body, byte_size(Body)};
encode_value(#kp_node{}=Node) ->
    #kp_node{
              childpointer = PointerNode,
              treesize = TreeSize,
              mbb_orig = MbbO
            } = Node,
    BinMbbO = encode_mbb(MbbO),
    NumMbbO = length(MbbO) * 2,
    BinReduce = <<NumMbbO:16, BinMbbO/binary>>,
    SizeReduce = byte_size(BinReduce),
    BinValue = <<PointerNode:?POINTER_BITS, TreeSize:?TREE_SIZE_BITS,
                 SizeReduce:?RED_BITS, BinReduce:SizeReduce/binary>>,
                 %SizeMbbO:?MBBO_BITS, BinMbbO/binary>>,
    % Return `0` as no additional bytes are written. The bytes that will
    % be written are accounted when the whole chunk gets written.
    {BinValue, 0}.


decode_kvnode_value(BinValue) ->
    % XXX vmx 2014-07-20: Add support for geometries
    #kv_node{
       geometry = 0,
       body = BinValue,
       % XXX vmx 2014-07-20: What is the size used for?
       size = 0
      }.


% Decode the value of a KP-node pair
-spec decode_kpnode_value(BinValue :: binary()) -> #kp_node{}.
decode_kpnode_value(BinValue) ->
    <<PointerNode:?POINTER_BITS, TreeSize:?TREE_SIZE_BITS,
      SizeReduce:?RED_BITS, Reduce:SizeReduce/binary>> = BinValue,
    <<_NumMbb:16, BinMbbO/binary>> = Reduce,
    MbbO = decode_mbb(BinMbbO),
    #kp_node{
              childpointer = PointerNode,
              treesize = TreeSize,
              reduce = nil,
              mbb_orig = MbbO
            }.


% Decode the value of the KP nodes to Erlang terms
-spec decode_node(BinValue :: binary()) -> [#kp_node{} | #kv_node{}].
decode_node(<<?KV_NODE:8, Rest/binary>>) ->
    decode_kvnode_pairs(Rest, []);
decode_node(<<?KP_NODE:8, Rest/binary>>) ->
    decode_kpnode_pairs(Rest, []).


% Decode KV-nodes key value pairs to an Erlang record
-spec decode_kvnode_pairs(BinValue :: binary(), Acc :: [#kv_node{}]) ->
                                 [#kv_node{}].
decode_kvnode_pairs(<<>>, Acc) ->
    lists:reverse(Acc);
% Matchning the binary in the function (and not the body) is an optimization
decode_kvnode_pairs(<<SizeK:?KEY_BITS, SizeV:?VALUE_BITS,
                      BinK:SizeK/binary, BinV:SizeV/binary,
                      Rest/binary>>, Acc) ->
    {Mbb, DocId} = decode_key_docid(BinK),
    Node0 = decode_kvnode_value(BinV),
    Node = Node0#kv_node{key = Mbb, docid = DocId},
    decode_kvnode_pairs(Rest, [Node|Acc]).


% Decode KP-nodes key value pairs to an Erlang record
-spec decode_kpnode_pairs(BinValue :: binary(), Acc :: [#kp_node{}]) ->
                                 [#kp_node{}].
decode_kpnode_pairs(<<>>, Acc) ->
    lists:reverse(Acc);
% Matchning the binary in the function (and not the body) is an optimization
decode_kpnode_pairs(<<SizeK:?KEY_BITS, SizeV:?VALUE_BITS,
                      BinK:SizeK/binary, BinV:SizeV/binary,
                      Rest/binary>>, Acc) ->
    <<_NumMbb:16, BinMbb/binary>> = BinK,
    Mbb = decode_mbb(BinMbb),
    Node0 = decode_kpnode_value(BinV),
    Node = Node0#kp_node{key = Mbb},
    decode_kpnode_pairs(Rest, [Node|Acc]).


-spec encode_mbb(Mbb :: mbb()) -> binary().
encode_mbb(Mbb) ->
    << <<Min:64/native-float, Max:64/native-float>> || {Min, Max} <- Mbb>>.


-spec encode_key_docid(Mbb :: mbb(), DocId :: binary()) -> binary().
encode_key_docid(Mbb, DocId) ->
    BinMbb = encode_mbb(Mbb),
    % Number of numbers is two times the dimension
    <<(length(Mbb) * 2):16, BinMbb/binary, DocId/binary>>.

-spec decode_mbb(BinMbb :: binary()) -> mbb().
decode_mbb(<<BinMbb/binary>>) ->
    [{Min, Max} || <<Min:64/native-float, Max:64/native-float>> <= BinMbb].


-spec decode_key_docid(Key :: binary()) -> {mbb(), binary()}.
decode_key_docid(<<Num:16, KeyDocId/binary>>) ->
    KeySize = Num * 8,
    <<BinMbb:KeySize/binary, DocId/binary>> = KeyDocId,
    {decode_mbb(BinMbb), DocId}.
