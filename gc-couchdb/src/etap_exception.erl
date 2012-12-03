% This is kind of a dirty hack to make the etaps tests pass on both Couchbase
% and Apache CouchDB.
% Couchbase currently doesn't use the single file version of etap (but will in
% the future. Until then, I just put and etap_exception module here that calls
% the single file version functions:
-module(etap_exception).
-extends(etap).
