FILE(GLOB SOURCES src/*.erl)
FILE(GLOB HEADERS include/*.hrl)

ADD_ERLANG_INCLUDE_DIR(${COUCHDB_SOURCE_DIR}/src)

ERL_BUILD_OTP (APPNAME "gc-couchbase" VERSION 1.0.0
  INSTALL_PATH "lib/couchdb/plugins"
  HEADERS ${HEADERS} SOURCES ${SOURCES})

INSTALL (FILES "etc/couchdb/default.d/geocouch.ini"
  DESTINATION "etc/couchdb/default.d")

ADD_SUBDIRECTORY(test)
