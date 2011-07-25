ERL = erl -boot start_clean
VERSION=$(shell git describe)

all: builddir compile 

compile:
	@$(ERL) -pa build -noinput +B -eval 'case make:all([{i, "'${COUCH_SRC}'"}]) of up_to_date -> halt(0); error -> halt(1) end.'

compileforcheck:
	@$(ERL) -pa build -noinput +B -eval 'case make:all([{i, "'${COUCH_SRC}'"}, {d, makecheck}]) of up_to_date -> halt(0); error -> halt(1) end.'

builddir:
	@mkdir -p build

buildandtest: all test

runtests:
	ERL_FLAGS="-pa ${COUCH_SRC} -pa ${COUCH_SRC}/../etap -pa ${COUCH_SRC}/../snappy" prove ./test/*.t

check: clean builddir compileforcheck runtests
	rm -rf build
	rm -f test/*.beam

#coverage: compile
#	mkdir -p coverage
##	erl -noshell -pa build -pa ${COUCH_SRC} -pa /home/vmx/src/erlang/coverize/coverize/ebin -s vtree_bulk run_cover -s init stop
#	erl -noshell -pa build -pa test/ebin -pa ${COUCH_SRC} -pa /home/vmx/src/erlang/coverize/coverize/ebin -s run_tests run_cover -s init stop

cover: compile
	@mkdir -p coverage
	erl -noshell -pa build -pa ${COUCH_SRC} -eval 'cover:compile("src/vtree/vtree_bulk", [{i, "'${COUCH_SRC}'"}]),vtree_bulk:test(),cover:analyse_to_file(vtree_bulk, "coverage/vtree_bulk_coverage.html", [html]).' -s init stop;

clean:
	rm -rf build
	rm -f test/*.beam
	rm -f *.tar.gz

geocouch-$(VERSION).tar.gz:
	git archive --prefix=geocouch-$(VERSION)/ --format tar HEAD | gzip -9vc > $@

dist: geocouch-$(VERSION).tar.gz
