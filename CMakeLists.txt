PROJECT (geocouch)

CMAKE_MINIMUM_REQUIRED (VERSION 2.8)

SET(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH}
  "${CMAKE_SOURCE_DIR}/../tlm/cmake/Modules")
INCLUDE (FindCouchbaseErlang)

SET(COUCHDB_SOURCE_DIR ${PROJECT_SOURCE_DIR}/../couchdb)
SET(COUCHDB_BINARY_DIR ${PROJECT_BINARY_DIR}/../couchdb)

ADD_ERLANG_INCLUDE_DIR(${COUCHDB_SOURCE_DIR}/src/couchdb)
ADD_ERLANG_INCLUDE_DIR(${PROJECT_SOURCE_DIR})

# In order to be able to test all functions need to be exported. Hence a
# separate build is needed.
IF(GEOCOUCH_BUILD_FOR_UNIT_TESTS)
  ENABLE_TESTING()
  INCLUDE(FindCouchbasePythonInterp)

  MESSAGE(STATUS "Add +export_all flag for testing")
  SET(ERLANG_COMPILE_FLAGS +debug_info -Werror +export_all)

  SET(COUCHSTORE_BIN_PATH ${PROJECT_BINARY_DIR}/../couchstore)
  SET(COUCHDB_RUNTEST ${COUCHDB_SOURCE_DIR}/test/etap/runtest.py)

  ADD_CUSTOM_TARGET(geocouch-test COMMAND ${CMAKE_CTEST_COMMAND} -j4)
ELSE(GEOCOUCH_BUILD_FOR_UNIT_TESTS)
  SET(ERLANG_COMPILE_FLAGS +debug_info -Werror)

  # Dialyzer shouldn't be run on the build where all functions are exported
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
ENDIF(GEOCOUCH_BUILD_FOR_UNIT_TESTS)

ADD_SUBDIRECTORY(vtree)
ADD_SUBDIRECTORY(gc-couchbase)
