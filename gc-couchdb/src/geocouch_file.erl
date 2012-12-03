-module(geocouch_file).
-extends(couch_file).

-define(SIZE_BLOCK, 4096).

-export([append_chunk/2, pread_chunk/2, pread_chunk_term/2, flush/1]).

-include("couch_db.hrl").

% XXX vmx 2012-11-08: Make it work with CRC32 as Couchbase does
append_chunk(Fd, Bin) ->
    {ok, Compressed} = snappy:compress(Bin),
    couch_file:append_binary(Fd, Compressed).

pread_chunk(Fd, Pointer) ->
    {ok, Compressed} = couch_file:pread_binary(Fd, Pointer),
    snappy:decompress(Compressed).

pread_chunk_term(Fd, Pointer) ->
    {ok, Compressed} = couch_file:pread_binary(Fd, Pointer),
    {ok, erlang:binary_to_term(couch_compress:decompress(Compressed))}.

% This is used by Couchbase, but it's a no-op in Apache CouchDB
flush(_Fd) ->
    ok.
