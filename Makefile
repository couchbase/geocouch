ERL=erl
VERSION=$(shell git describe)
GEOCOUCH_PLT ?= ../geocouch.plt

COMPILER_OPTIONS=$(shell $(ERL) -noinput +B -eval 'Options = case os:getenv("ERL_COMPILER_OPTIONS") of false ->[]; Else -> {ok,Tokens,_} = erl_scan:string(Else ++ "."),{ok,Term} = erl_parse:parse_term(Tokens), Term end, io:format("~p~n", [[{i, filename:absname("${COUCH_SRC}")}] ++ Options]), halt(0).')
COMPILER_OPTIONS_MAKE_CHECK=$(shell $(ERL) -noinput +B -eval 'Options = case os:getenv("ERL_COMPILER_OPTIONS") of false -> []; Else -> {ok,Tokens,_} = erl_scan:string(Else ++ "."),{ok,Term} = erl_parse:parse_term(Tokens), Term end, io:format("~p~n", [[{i, filename:absname("${COUCH_SRC}")},{d, makecheck}] ++ Options]), halt(0).')


couchbase couchbase-check: REBAR_CONFIG := rebar_couchbase.config
couchdb couchdb-check: REBAR_CONFIG := rebar_couchdb.config


all:
	@echo "Try \"make couchbase\" or \"make couchdb\"instead."

check:
	@echo "Try \"make couchbase-check\" \"make couchdb-check\"instead."

.PHONY: couchbase
couchbase: compile
couchbase-check: do-check runtests-couchbase clean-again

.PHONY: couchdb
couchdb: compile
couchdb-check: do-check clean-again

compile:
	ERL_COMPILER_OPTIONS='$(COMPILER_OPTIONS)' ./rebar -C $(REBAR_CONFIG) compile

compileforcheck:
	ERL_COMPILER_OPTIONS='$(COMPILER_OPTIONS_MAKE_CHECK)' ./rebar -C $(REBAR_CONFIG) compile

runtests:
	ERL_LIBS=. ERL_FLAGS="-pa ${COUCH_SRC} -pa ${COUCH_SRC}/../etap -pa ${COUCH_SRC}/../snappy -pa ${COUCH_SRC}/../../test/etap -pa ${COUCH_SRC}/../couch_set_view/ebin -pa ${COUCH_SRC}/../mochiweb -pa ${COUCH_SRC}/../lhttpc -pa ${COUCH_SRC}/../erlang-oauth -pa ${COUCH_SRC}/../ejson -pa ${COUCH_SRC}/../mapreduce" prove ./vtree/test/*.t

do-check: clean compileforcheck dialyzer runtests

runtests-couchbase:
	PATH=${PATH}:../couchstore ERL_LIBS=. ERL_FLAGS="-pa ${COUCH_SRC} -pa ${COUCH_SRC}/../etap -pa ${COUCH_SRC}/../snappy -pa ${COUCH_SRC}/../../test/etap -pa ${COUCH_SRC}/../couch_set_view/ebin -pa ${COUCH_SRC}/../mochiweb -pa ${COUCH_SRC}/../lhttpc -pa ${COUCH_SRC}/../erlang-oauth -pa ${COUCH_SRC}/../ejson -pa ${COUCH_SRC}/../mapreduce -pa ${COUCH_SRC}/../couch_set_view/test" prove gc-couchbase/test/*.t

clean clean-again:
	./rebar -C rebar_couchbase.config clean -r
	./rebar -C rebar_couchdb.config clean -r
	rm -f *.tar.gz


$(GEOCOUCH_PLT):
	dialyzer --output_plt ../geocouch.plt --build_plt --apps kernel stdlib

dialyzer: $(GEOCOUCH_PLT)
	dialyzer --verbose --plt $(GEOCOUCH_PLT) -r vtree


geocouch-$(VERSION).tar.gz:
	git archive --prefix=geocouch-$(VERSION)/ --format tar HEAD | gzip -9vc > $@

dist: geocouch-$(VERSION).tar.gz
