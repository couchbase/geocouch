Welcome to the world of GeoCouch
================================

GeoCouch is a spatial extension for Apache CouchDB and Couchbase.

Prerequisites
-------------

A working installation of CouchDB with corresponding source
code. GeoCouch works best with Couchbase and the latest stable releases of
CouchDB (should be >= 1.1.0).

### Understanding the branches:

This repository contains several branches, please make sure you use
the correct one:

 - master: works with Apache CouchDB 1.6.x and supports multidimensional indexing
 - couchdb1.1.x: works with Apache CouchDB 1.1.x
 - couchdb1.2.x: works with Apache CouchDB 1.2.x
 - couchdb1.3.x: works with Apache CouchDB 1.3.x - 1.6.x


Installation
------------

See the [main README](../README.md) for installtion instructions.


Using GeoCouch
--------------

The following instruction refer to the newest version of GeoCouch which supports multidimensional indexing.

Create a database:

    curl -X PUT http://127.0.0.1:5984/places

Add a Design Document with a spatial function:

    curl -X PUT -d '{"spatial":{"points":"function(doc) {\n    if (doc.loc) {\n        emit([{\n            type: \"Point\",\n            coordinates: [doc.loc[0], doc.loc[1]]\n        }], [doc._id, doc.loc]);\n    }};"}}' http://127.0.0.1:5984/places/_design/main

Put some data into it:

    curl -X PUT -d '{"loc": [-122.270833, 37.804444]}' http://127.0.0.1:5984/places/oakland
    curl -X PUT -d '{"loc": [10.898333, 48.371667]}' http://127.0.0.1:5984/places/augsburg

Make a bounding box request:

    curl -X GET --globoff 'http://localhost:5984/places/_design/main/_spatial/points?start_range=[0,0]&end_range=[180,90]'

It should return:

    {"id":"augsburg","key":[[10.89833299999999916,10.89833299999999916],[48.37166700000000219,48.37166700000000219]],"geometry":{"type":"Point","coordinates":[10.89833299999999916,48.37166700000000219]},"value":["augsburg",[10.89833299999999916,48.37166700000000219]]}


The Design Document Function
----------------------------

function(doc) {
    if (doc.loc) {
        emit([{
            type: "Point",
            coordinates: [doc.loc[0], doc.loc[1]]
        }], [doc._id, doc.loc]);
    }};"

It uses the emit() from normal views. The key is a
[GeoJSON](http://geojson.org) geometry, the value is any arbitrary JSON. All
geometry types (even GemetryCollections) are supported.

If the GeoJSON geometry contains a `bbox` property it will be used instead
of calculating it from the geometry (even if it's wrong, i.e. is not
the actual bounding box).


List function support
---------------------

GeoCouch supports List functions just as CouchDB does for Views. This way
you can output any arbitrary format, e.g. GeoRSS.

As an example we output the points as WKT. Add a new Design Document
with an additional List function (the rest is the same as above). Make
sure you use the right `_rev`:

    curl -X PUT -d '{"_rev": "1-2a1d0e8c2d4ba3e64b9f6a9552d3d60f", "lists": {"wkt": "function(head, req) {\n    var row;\n    while (row = getRow()) {\n        send(\"POINT(\" + row.geometry.coordinates.join(\" \") + \")\\n\");\n    }\n};"}, "spatial":{"points":"function(doc) {\n    if (doc.loc) {\n        emit([{\n            type: \"Point\",\n            coordinates: [doc.loc[0], doc.loc[1]]\n        }], [doc._id, doc.loc]);\n    }};"}}' http://127.0.0.1:5984/places/_design/main

Now you can request this List function as you would do for CouchDB,
though with a different Design handler (`_spatial/_list` instead of
`_list` ):

    curl -X GET --globoff 'http://localhost:5984/places/_design/main/_spatial/_list/wkt/points?start_range=[-180,-90]&end_range=[180,90]'

The result is:

    POINT(10.898333 48.371667)
    POINT(-122.270833 37.804444

Using List functions from Design Documents other than the one containing the
Spatial functions is supported as well. This time we add the Document
ID in parenthesis:

    curl -X PUT -d '{"lists": {"wkt": "function(head, req) {\n    var row;\n    while (row = getRow()) {\n        send(\"POINT(\" + row.geometry.coordinates.join(\" \") + \") (\" + row.id + \")\\n\");\n    }\n};"}}' http://127.0.0.1:5984/places/_design/listfunonly

    curl -X GET --globoff 'http://localhost:5984/places/_design/listfunonly/_spatial/_list/wkt/main/points?start_range=[-180,-90]&end_range=[180,90]'

The result is:

    POINT(10.898333 48.371667) (augsburg)
    POINT(-122.270833 37.804444) (oakland)


Other supported query arguments
-------------------------------

### stale ###
`stale=ok` is supported. The spatial index won't be rebuilt even if
new Documents were added. It works for normal spatial queries as well
as for the spatial List functions.

### count ###
`count` is a boolean. `count=true` will only return the number of geometries
the query will return, not the geometry themselves.

    curl -X GET --globoff 'http://localhost:5984/places/_design/main/_spatial/points?start_range=[0,0]&end_range=[180,90]&count=true'

    {"count":1}

### limit ###
With `limit` you can limit the number of rows that should be returned.

    curl -X GET --globoff 'http://localhost:5984/places/_design/main/_spatial/points?start_range=[-180,-90]&end_range=[180,90]&limit=1'

    {"update_seq":3,"rows":[
    {"id":"augsburg","key":[[10.89833299999999916,10.89833299999999916],[48.37166700000000219,48.37166700000000219]],"geometry":{"type":"Point","coordinates":[10.89833299999999916,48.37166700000000219]},"value":["augsburg",[10.89833299999999916,48.37166700000000219]]}
    ]}

### skip ###
With `skip` you start to return the results at a certain offset.

    curl -X GET --globoff 'http://localhost:5984/places/_design/main/_spatial/points?start_range=[-180,-90]&end_range=[180,90]&skip=1'

    {"update_seq":3,"rows":[
    {"id":"oakland","key":[[-122.2708329999999961,-122.2708329999999961],[37.804443999999996606,37.804443999999996606]],"geometry":{"type":"Point","coordinates":[-122.2708329999999961,37.804443999999996606]},"value":["oakland",[-122.2708329999999961,37.804443999999996606]]}
    ]}


Compaction, cleanup and info
----------------------------

The API of GeoCouch's spatial indexes is similar to the one for the
Views. Compaction of spatial indexes is per Design Document, thus:

    curl -X POST 'http://localhost:5984/places/_design/main/_spatial/_compact' -H 'Content-Type: application/json'

To cleanup spatial indexes that are no longer in use (this is per database):

    curl -X POST 'http://localhost:5984/places/_spatial_cleanup' -H 'Content-Type: application/json'

To get information about the spatial indexes of a certain Design
Document use the the `_info` handler:

    curl -X GET 'http://localhost:5984/places/_design/main/_spatial/_info'
