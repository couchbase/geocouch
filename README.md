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


For Apache CouchDB
------------------

This version of GeoCouch needs at least Apache CouchDB 1.3.x.


### Checkout the code

First checkout the source code for Apache CouchDB into a directory that will
be referred to as <path-to-couchdb-source>.

Then checkout the GeoCouch source:

    git clone -b newvtree https://github.com/couchbase/geocouch.git

There's a new directory called `geocouch` created. From now on this directory
will be referred to as <path-to-geocouch-source>.


### Build instructions

Make sure you have built Apache CouchDB from source including `make dev`. So
go to your <path-to-couchdb-source> and run:

    ./bootstrap
    ./configure
    make dev

After that you can compile GeoCouch from within the GeoCouch directory:

    COUCH_SRC=<path-to-couchdb-source>/src/couchdb make couchdb

Now copy the configuration file into your Apache CouchDB directory:

    cp etc/couchdb/default.d/geocouch.ini <path-to-couchdb-source>/etc/couchdb/default.d/


### Running tests

After you've followed the build instructions you can run the tests with

    COUCH_SRC=<path-to-couchdb-source>/src/couchdb make couchdb-check

In order to run the JavaScript based tests, you need to start Apache CouchDB first:

    cd <path-to-couchdb-source>
    ERL_LIBS="<path-to-geocouch-source>" ./utils/run

The tests can either be run from the command line or the browser.


#### From command line

From the command line the easiest way is to use the supplied runner script.
From within the <path-to-geocouch-source>:

    cd gc-couchdb
    ./utils/runjstests.sh <path-to-couchdb-source>/test/javascript/run ./share/www/script/test


#### From browser

To run it from the browser first copy the JavaScript tests into the same directory as the other Apache CouchDB tests:

   cp <path-to-geocouch-source>/gc-couchdb/share/www/script/test/* <path-to-couchdb-source>/share/www/script/test/

Then add the tests to `<path-to-couchdb-source>/share/www/script/couch_tests.js`

    loadTest("spatial.js");
    loadTest("list_spatial.js");
    loadTest("etags_spatial.js");
    loadTest("multiple_spatial_rows.js");
    loadTest("spatial_compaction.js");
    loadTest("spatial_design_docs.js");
    loadTest("spatial_bugfixes.js");
    loadTest("spatial_offsets.js");
