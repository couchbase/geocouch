Welcome to the world of GeoCouch
================================

See README for the original CouchDB information.

Prerequisites
-------------

Clone the GeoCouch branch:

    # default branch is already geocouch
    git clone http://github.com/vmx/couchdb.git
    cd couchdb

Compile it:

    ./bootstrap
    ./configure
    make dev

Run it:

    ./utils/run


Using GeoCouch
--------------

Create a database:

    curl -X PUT http://127.0.0.1:5984/places

Add a Design Document with a spatial function:

    curl -X PUT -d '{"spatial":{"points":"function(doc) {\n    if (doc.loc) {\n        emit({\n            type: \"Point\",\n            coordinates: [doc.loc[0], doc.loc[1]]\n        }, [doc._id, doc.loc]);\n    }};"}}' http://127.0.0.1:5984/places/_design/main

Put some data into it:

    curl -X PUT -d '{"loc": [-122.270833, 37.804444]}' http://127.0.0.1:5984/places/oakland
    curl -X PUT -d '{"loc": [10.898333, 48.371667]}' http://127.0.0.1:5984/places/augsburg

Make a bounding box request:

    curl -X GET 'http://localhost:5984/places/_design/main/_spatial/points?bbox=0,0,180,90'

It should return:

    {"update_seq":3,"rows":[
    {"id":"augsburg","bbox":[10.898333,48.371667,10.898333,48.371667],"value":["augsburg",[10.898333,48.371667]]}
    ]}

The Design Document Function
----------------------------

function(doc) {
    if (doc.loc) {
        emit({
            type: "Point",
            coordinates: [doc.loc[0], doc.loc[1]]
        }, [doc._id, doc.loc]);
    }};"

It uses the emit() from normal views. The key is a
[GeoJSON](http://geojson.org) geometry, the value is any arbitrary JSON. All
geometry types (even GemetryCollections) are supported.

If the GeoJSON geometry contains a `bbox` property it will be used instead
of calculating it from the geometry (even if it's wrong, i.e. is not
the actual bounding box).


Bounding box search and the date line
-------------------------------------

A common problem when performing bounding box searches is the date
line/poles. As the bounding box follows the GeoJSON specification,
where the first two numbers are the lower left coordinate, the last
two numbers the upper right coordinate, it is easy to map it over the
date line/poles. The lower coordinate would have a higher value than
the upper one. Such a bounding box has a seems invalid at first
glance, but isn't. For example a bounding box like `110,-60,-30,15`
would include Australia and South America, but not Africa.

GeoCouch automatically detects such bounding boxes and returns the
expected result. Give it a try (with the same Design Document as
above). Insert some Documents:

    curl -X PUT -d '{"loc": [17.15, -22.566667]}' http://127.0.0.1:5984/places/namibia
    curl -X PUT -d '{"loc": [135, -25]}' http://127.0.0.1:5984/places/australia
    curl -X PUT -d '{"loc": [-52.95, -10.65]}' http://127.0.0.1:5984/places/brasilia

And request only Australia and Brasilia:

    curl -X GET 'http://localhost:5984/places/_design/main/_spatial/points?bbox=110,-60,-30,15'

The result is as expected:

    {"update_seq":6,"rows":[
    {"id":"australia","bbox":[135,-25,135,-25],"value":["australia",[135,-25]]},
    {"id":"brasilia","bbox":[-52.95,-10.65,-52.95,-10.65],"value":["brasilia",[-52.95,-10.65]]}
    ]}

The bounding with the same numbers, but different order
(`-30,-60,110,15`) would only return Namibia:

    curl -X GET 'http://localhost:5984/places/_design/main/_spatial/points?bbox=-30,-60,110,15'

    {"update_seq":6,"rows":[
    {"id":"namibia","bbox":[17.15,-22.566667,17.15,-22.566667],"value":["namibia",[17.15,-22.566667]]}
    ]}

List function support
---------------------

GeoCouch supports List functions just as CouchDB does for Views. This way
you can output any arbitrary format, e.g. GeoRSS.

As an example we output the points as WKT. Add a new Design Document
with an additional List function (the rest is the same as above). Make
sure you use the right `_rev`:

    curl -X PUT -d '{"_rev": "1-121efc747b00743b8c7621ffccf1ac40", "lists": {"wkt": "function(head, req) {\n    var row;\n    while (row = getRow()) {\n        send(\"POINT(\" + row.value[1].join(\" \") + \")\\n\");\n    }\n};"}, "spatial":{"points":"function(doc) {\n    if (doc.loc) {\n        emit({\n            type: \"Point\",\n            coordinates: [doc.loc[0], doc.loc[1]]\n        }, [doc._id, doc.loc]);\n    }};"}}' http://127.0.0.1:5984/places/_design/main

Now you can request this List function as you would do for CouchDB,
though with a different Design handler (`_spatiallist` instead of
`_list` ):

    curl -X GET 'http://localhost:5984/places/_design/main/_spatiallist/wkt/points?bbox=-180,-90,180,90'

The result is:

    POINT(-122.270833 37.804444)
    POINT(10.898333 48.371667)
    POINT(17.15 -22.566667)
    POINT(135 -25)
    POINT(-52.95 -10.65)

Using List functions from Design Documents other than the one containing the
Spatial functions is supported as well. This time we add the Document
ID in parenthesis:

    curl -X PUT -d '{"lists": {"wkt": "function(head, req) {\n    var row;\n    while (row = getRow()) {\n        send(\"POINT(\" + row.value[1].join(\" \") + \") (\" + row.id + \")\\n\");\n    }\n};"}}' http://127.0.0.1:5984/places/_design/listfunonly

    curl -X GET 'http://localhost:5984/places/_design/listfunonly/_spatiallist/wkt/main/points?bbox=-180,-90,180,90'


Other supported query arguments
-------------------------------

### stale ###
`stale=ok` is supported. The spatial index won't be rebuild even if
new Documents were added. It works for normal spatial queries as well
as for the spatial List functions.

### count ###
`count` is a boolean. `count=true` will only return the number of geometries
the query will return, not the geometry themselves.

    curl -X GET 'http://localhost:5984/places/_design/main/_spatial/points?bbox=0,0,180,90'

    {"count":1}




New stuff
=========

Get it running
--------------

Note: always replace <vanilla-couch> with the path to your CouchDB source and <geocouch> with
the location of the GeoCouch source.

 - change "-I" path in <geocouch>/Makefile to match <vanilla-couch>/src/couchdb
 - run "make" in your <geocouch> directory
 - "make dev" your vanilla couch.
 - add the spatial indexer to you <vanilla-couch>/etc/couchdb/local_dev.ini
 
    [daemons]
    spatial_manager={couch_spatial, start_link, []}

    [httpd_design_handlers]
    _spatial = {couch_httpd_spatial, handle_spatial_req}
    _spatiallist = {couch_httpd_spatial_list, handle_spatial_list_req}

 - copy Futon tests over (from <geocouch>/share/www/script/test to
   <vanilla-couch>/share/www/script/test)
 - add them to <vanilla-couch>/share/www/script/test/couch_tests.js

    loadTest("list_spatial.js");
    loadTest("spatial.js");

 - Now run your CouchDB with GeoCouch in the Erlang path:

    ERL_FLAGS="-pa <geocouch>/ebin" <vanilla-couch>/utils/run
