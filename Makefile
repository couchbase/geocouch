ERL = erl -boot start_clean

all: builddir compile 

compile:
	@$(ERL) -pa build -noinput +B -eval 'case make:all([{i, "'${COUCH_SRC}'"}]) of up_to_date -> halt(0); error -> halt(1) end.'

builddir:
	@mkdir -p build

buildandtest: all test

test:
	erl -pa build -pa ${COUCH_SRC} -run vtree_bulk test -run init stop -noshell

#coverage: compile
#	mkdir -p coverage
##	erl -noshell -pa build -pa ${COUCH_SRC} -pa /home/vmx/src/erlang/coverize/coverize/ebin -s vtree_bulk run_cover -s init stop
#	erl -noshell -pa build -pa test/ebin -pa ${COUCH_SRC} -pa /home/vmx/src/erlang/coverize/coverize/ebin -s run_tests run_cover -s init stop

cover: compile
	@mkdir -p coverage
	erl -noshell -pa build -pa ${COUCH_SRC} -eval 'cover:compile("src/vtree/vtree_bulk", [{i, "'${COUCH_SRC}'"}]),vtree_bulk:test(),cover:analyse_to_file(vtree_bulk, "coverage/vtree_bulk_coverage.html", [html]).' -s init stop;

clean:	
	rm -rf build
