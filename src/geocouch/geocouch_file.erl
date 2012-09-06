-module(geocouch_file).
-extends(couch_file).

-define(SIZE_BLOCK, 4096).

-export([append_chunk/2, pread_chunk/2, pread_chunk_term/2, flush/1]).

% New new fileformat always uses compression and CRC32 for every chunk
append_chunk(Fd, Bin) ->
    Compressed = couch_compress:compress(Bin),
    % Prepend a "1" to mark it as a chunk that has a CRC32
    Chunk = [<<1:1/integer, (erlang:size(Compressed)):31/integer,
               (erlang:crc32(Compressed)):32/integer>>,
             Compressed],
    gen_server:call(Fd, {append_bin, Chunk}, infinity).

% Read the chunk that was written by append_chunk/2.
pread_chunk(Fd, Pointer) ->
    {ok, Compressed} = couch_file:pread_binary(Fd, Pointer),
    {ok, couch_compress:decompress(Compressed)}.

pread_chunk_term(Fd, Pointer) ->
    {ok, Compressed} = couch_file:pread_binary(Fd, Pointer),
    {ok, erlang:binary_to_term(couch_compress:decompress(Compressed))}.

% This will be a no-op in Apache CouchDB
flush(Fd) ->
    couch_file:flush(Fd).
