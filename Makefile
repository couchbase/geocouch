ERL = erl -boot start_clean

all: builddir compile 

compile:
	@$(ERL) -pa build -noinput +B -eval 'case make:all([{i, "'${COUCH_SRC}'"}]) of up_to_date -> halt(0); error -> halt(1) end.'

builddir:
	@mkdir -p build

buildandtest: all test

test:
	erl -pa build -pa ${COUCH_SRC} -run vtree_bulk test -run init stop -noshell

clean:	
	rm -rf build
