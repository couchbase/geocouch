Welcome to the world of GeoCouch
================================

GeoCouch is a spatial extension for Couchbase and Apache CouchDB.


For Couchbase
-------------

### Checkout the code

Check the code out with repo:

    mkdir newvtree
    cd newvtree
    repo init -u git://github.com/couchbase/manifest.git -m toy/toy-newvtree.xml
    repo sync


### Build instructions

Make sure you have built CouchDB from source including `make dev`. So
go to your CouchDB directory and run:

    ./bootstrap
    ./configure
    make dev

After that you can compile GeoCouch from within the GeoCouch directory:

    COUCH_SRC=<path-to-couchdb-source>/src/couchdb make couchbase


### Running tests

After you've followed the build instructions you can run the tests with

    COUCH_SRC=<path-to-couchdb-source>/src/couchdb make couchbase-check
