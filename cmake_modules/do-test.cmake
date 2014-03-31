# Find all test files and run the test.

FILE (GLOB testfiles RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" test/*.t)

SET (ENV{ERL_FLAGS} "-pa ebin -pa ${COUCH_SRC} -pa ${COUCH_SRC}/../etap -pa ${COUCH_SRC}/../snappy -pa ${COUCH_SRC}/../../test/etap -pa ${COUCH_SRC}/../couch_set_view/ebin -pa ${COUCH_SRC}/../couch_index_merger/ebin -pa ${COUCH_SRC}/../mochiweb -pa ${COUCH_SRC}/../lhttpc -pa ${COUCH_SRC}/../erlang-oauth -pa ${COUCH_SRC}/../ejson -pa ${COUCH_SRC}/../mapreduce")
MESSAGE ("${testfiles}")
MESSAGE ("$ENV{ERL_FLAGS}")

EXECUTE_PROCESS(RESULT_VARIABLE _failure
  COMMAND "${PROVE_EXECUTABLE}" ${testfiles})
IF (_failure)
  MESSAGE (FATAL_ERROR "failed running tests")
ENDIF (_failure)
