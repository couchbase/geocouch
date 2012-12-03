#!/bin/sh
for testfile in $2/*.js
do
    COUCHDB_NO_START=1 $1 $testfile
done
