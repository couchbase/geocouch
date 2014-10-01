PROJECT (geocouch)

CMAKE_MINIMUM_REQUIRED (VERSION 2.8)

INCLUDE (FindCouchbaseErlang)

# XXX vmx 2014-09-02: The "+export_all" is just a temp hack
SET(ERLANG_COMPILE_FLAGS +debug_info -Werror +export_all)

SET(COUCHDB_SOURCE_DIR ${PROJECT_SOURCE_DIR}/../couchdb)
SET(COUCHDB_BINARY_DIR ${CMAKE_BINARY_DIR}/couchdb)
SET(GEOCOUCH_BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR})
SET(COUCHSTORE_BIN_PATH ${CMAKE_BINARY_DIR}/couchstore)

ADD_ERLANG_INCLUDE_DIR(${COUCHDB_SOURCE_DIR}/src/couchdb)
ADD_ERLANG_INCLUDE_DIR(${PROJECT_SOURCE_DIR})

# For the tests
SET(COUCHDB_RUNTEST ${COUCHDB_SOURCE_DIR}/test/etap/runtest.py)
ADD_CUSTOM_TARGET(geocouch-check COMMAND ${CMAKE_CTEST_COMMAND} -j4
  DEPENDS geocouch-dialyzer)

ADD_SUBDIRECTORY(vtree)
ADD_SUBDIRECTORY(gc-couchbase)


SET(GEOCOUCH_PLT_FILE ${CMAKE_CURRENT_BINARY_DIR}/geocouch.plt)
ADD_CUSTOM_TARGET(geocouch-check-plt
       COMMAND
           ${CMAKE_COMMAND}
               -DPLT_FILE=${GEOCOUCH_PLT_FILE}
               -P ${CMAKE_MODULE_PATH}/ErlangCheckPlt.cmake)

ADD_CUSTOM_COMMAND(OUTPUT ${GEOCOUCH_PLT_FILE}
  COMMAND dialyzer
    --output_plt ${GEOCOUCH_PLT_FILE}
    --build_plt
    --apps crypto erts inets kernel ssl stdlib xmerl
    -r ${COUCHDB_BINARY_DIR}/src/mochiweb
  VERBATIM)

ADD_CUSTOM_TARGET(geocouch-dialyzer
  COMMAND  dialyzer
    --plt ${GEOCOUCH_PLT_FILE}
    --verbose
    -Wno_improper_lists
    -r
      ${CMAKE_CURRENT_BINARY_DIR}/vtree
      ${CMAKE_CURRENT_BINARY_DIR}/gc-couchbase
      ${COUCHDB_BINARY_DIR}/src/couch_set_view
      ${COUCHDB_BINARY_DIR}/src/snappy
      ${COUCHDB_BINARY_DIR}/src/couch_view_parser
      ${COUCHDB_BINARY_DIR}/src/couch_index_merger
      ${COUCHDB_BINARY_DIR}/src/couch_dcp
      ${COUCHDB_BINARY_DIR}/src/mapreduce
      ${COUCHDB_BINARY_DIR}/src/lhttpc
      ${COUCHDB_BINARY_DIR}/src/ejson
      ${COUCHDB_BINARY_DIR}/src/couchdb
      ${COUCHDB_BINARY_DIR}/src/etap
      ${COUCHDB_BINARY_DIR}/test/etap
  DEPENDS
    geocouch-check-plt
    ${GEOCOUCH_PLT_FILE})
