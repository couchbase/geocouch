FILE(GLOB TEST_FILES *.t)
LIST(SORT TEST_FILES)

FOREACH (it ${TEST_FILES})
   GET_FILENAME_COMPONENT(testname ${it} NAME_WE)
   ADD_TEST(geocouch-gc-couchbase-${testname} ${PYTHON_EXECUTABLE} ${COUCHDB_RUNTEST} -c ${COUCHSTORE_BIN_PATH} -p ${CouchDB_BINARY_DIR}/src -l ${GEOCOUCH_BINARY_DIR} -m "couch_set_view/test" -f "-pa ${CMAKE_CURRENT_BINARY_DIR}" -t ${it})
ENDFOREACH (it)

FILE(GLOB VTREE_TEST_SRC *.erl)
ERL_BUILD("gc-couchbase_test" ${VTREE_TEST_SRC})
