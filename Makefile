ERL=erl
VERSION=$(shell git describe)

all: compile

compile:
	./rebar compile

compileforcheck:
	MAKECHECK=1 ./rebar compile

buildandtest: all test

runtests:
	ERL_FLAGS="-pa ebin -pa ${COUCH_SRC} -pa ${COUCH_SRC}/../etap -pa ${COUCH_SRC}/../snappy -pa ${COUCH_SRC}/../../test/etap -pa ${COUCH_SRC}/../couch_set_view/ebin -pa ${COUCH_SRC}/../couch_index_merger/ebin -pa ${COUCH_SRC}/../mochiweb -pa ${COUCH_SRC}/../lhttpc -pa ${COUCH_SRC}/../erlang-oauth -pa ${COUCH_SRC}/../ejson -pa ${COUCH_SRC}/../mapreduce" prove ./test/*.t

check: clean compileforcheck runtests
	./rebar clean

clean:
	./rebar clean
	rm -f *.tar.gz

geocouch-$(VERSION).tar.gz:
	git archive --prefix=geocouch-$(VERSION)/ --format tar HEAD | gzip -9vc > $@

dist: geocouch-$(VERSION).tar.gz
