// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

couchTests.spatial = function(debug) {
  var db = new CouchDB("test_suite_db", {"X-Couch-Full-Commit":"false"});
  db.deleteDb();
  db.createDb();

  if (debug) debugger;
    
    
  var designDoc = {
    _id:"_design/spatial",
    language: "javascript",
    spatial : {
      basicIndex : stringFun(function(doc) {
        emit({
          type: "Point",
          coordinates: [doc.loc[0], doc.loc[1]]
        }, doc.string);
      }),
      dontEmitAll : stringFun(function(doc) {
        if (doc._id>5) {
          emit({
            type: "Point",
            coordinates: [doc.loc[0], doc.loc[1]]
          }, doc.string);
        }
      }),
      emitNothing : stringFun(function(doc) {}),
      geoJsonGeoms : stringFun(function(doc) {
        if (doc._id.substr(0,3)=="geo") {
          emit(doc.geom, doc.string);
        }
      })
    }
  };

  T(db.save(designDoc).ok);


  function makeSpatialDocs(start, end, templateDoc) {
    var docs = makeDocs(start, end, templateDoc);
    for (var i=0; i<docs.length; i++) {
        docs[i].loc = [i-20+docs[i].integer, i+15+docs[i].integer];
    }
    return docs;
  }
  
  function extract_ids(str) {
    var json = JSON.parse(str);
    var res = [];
    for each (var r in json.rows) {
      res.push(r.id);
    }
    return res.join("");
  }

  var etag, xhr;
  var url_pre = '/test_suite_db/_design/spatial/_spatial/';
  var docs = makeSpatialDocs(0, 10);
  db.bulkSave(docs);
  

  // bounding box tests

  var bbox = [-180, -90, 180, 90];
  xhr = CouchDB.request("GET", url_pre + "basicIndex?bbox=" + bbox.join(","));
  TEquals('0123456789', extract_ids(xhr.responseText),
          "should return all geometries");

  bbox = [-20, 0, 0, 20];
  xhr = CouchDB.request("GET", url_pre + "basicIndex?bbox=" + bbox.join(","));
  TEquals('012', extract_ids(xhr.responseText),
          "should return a subset of the geometries");

  bbox = [0, 4, 180, 90];
  xhr = CouchDB.request("GET", url_pre + "basicIndex?bbox=" + bbox.join(","));
  TEquals("{\"rows\":[]}\n", xhr.responseText,
          "should return no geometries");

  bbox = [-18, 17, -14, 21];
  xhr = CouchDB.request("GET", url_pre + "basicIndex?bbox=" + bbox.join(","));
  TEquals('123', extract_ids(xhr.responseText),
          "should also return geometry at the bounds of the bbox");

  bbox = [-16, 19, -16, 19];
  xhr = CouchDB.request("GET", url_pre + "basicIndex?bbox=" + bbox.join(","));
  TEquals('2', extract_ids(xhr.responseText),
          "bbox collapsed to a point should return the geometries there");

  bbox = [-180, 28, 180, 17];
  xhr = CouchDB.request("GET", url_pre + "basicIndex?bbox=" + bbox.join(","));
  TEquals('01789', extract_ids(xhr.responseText),
          "bbox that spans the poles");

  bbox = [-10, -90, -20, 90];
  xhr = CouchDB.request("GET", url_pre + "basicIndex?bbox=" + bbox.join(","));
  TEquals('056789', extract_ids(xhr.responseText),
          "bbox that spans the date line");

  bbox = [-10, 28, -20, 17];
  xhr = CouchDB.request("GET", url_pre + "basicIndex?bbox=" + bbox.join(","));
  TEquals('0789', extract_ids(xhr.responseText),
          "bbox that spans the date line and poles");

  
  // count parameter tests

  bbox = [-180, -90, 180, 90];
  xhr = CouchDB.request("GET", url_pre + "basicIndex?bbox=" + bbox.join(",") +
                        "&count=true");
  TEquals('{"count":10}\n', xhr.responseText,
          "should return the count of all geometries");
  

  // stale tests
  
  xhr = CouchDB.request("GET", url_pre + "basicIndex?bbox=" + bbox.join(","));
  var lastUpdateSeq = JSON.parse(xhr.responseText).update_seq;
  
  // stale=ok
  db.save({"id": "stale1", "loc": [50,60]});
  xhr = CouchDB.request("GET", url_pre + "basicIndex?bbox=" + bbox.join(",") +
                        "&stale=ok");
  // NOTE vmx: stale=ok could potentially trigger an update (like
  //     stale=update_after). There's no good way to test it.
  T(JSON.parse(xhr.responseText).update_seq == lastUpdateSeq);
  // wait 5 seconds for the next assertions to pass in very slow machines
  var t0 = new Date(), t1;
  do {
    CouchDB.request("GET", "/");
    t1 = new Date();
  } while ((t1 - t0) < 3000);
    
  xhr = CouchDB.request("GET", url_pre + "basicIndex?bbox=" + bbox.join(",") +
                        "&stale=ok");
  T(JSON.parse(xhr.responseText).update_seq == lastUpdateSeq);
  xhr = CouchDB.request("GET", url_pre + "basicIndex?bbox=" + bbox.join(","));
  T(JSON.parse(xhr.responseText).update_seq == lastUpdateSeq+1);
  
  // stale=update_after
  lastUpdateSeq++;
  db.save({"id": "stale2", "loc": [60,70]});
  xhr = CouchDB.request("GET", url_pre + "basicIndex?bbox=" + bbox.join(",") +
                        "&stale=update_after");
  T(JSON.parse(xhr.responseText).update_seq == lastUpdateSeq);
  // wait 5 seconds for the next assertions to pass in very slow machines
  t0 = new Date();
  do {
    CouchDB.request("GET", "/");
    t1 = new Date();
  } while ((t1 - t0) < 3000);
  xhr = CouchDB.request("GET", url_pre + "basicIndex?bbox=" + bbox.join(",") +
                        "&stale=ok");
  T(JSON.parse(xhr.responseText).update_seq == lastUpdateSeq+1);
  xhr = CouchDB.request("GET", url_pre + "basicIndex?bbox=" + bbox.join(","));
  T(JSON.parse(xhr.responseText).update_seq == lastUpdateSeq+1);
  

  // emit tests
  
  // spatial function that doesn't always emit
  bbox = [-180, -90, 180, 90];
  xhr = CouchDB.request("GET", url_pre + "dontEmitAll?bbox=" + bbox.join(","));
  TEquals('6789', extract_ids(xhr.responseText),
          "should return geometries with id>5");
  
  xhr = CouchDB.request("GET", url_pre + "emitNothing?bbox=" + bbox.join(","));
  TEquals('{\"rows\":[]}\n', xhr.responseText, "nothing emitted at all");

  
  // GeoJSON geometry tests
  // NOTE vmx: (for all those tests) Should I test if the returned
  //     bounding box is correct as well?
  
  // some geometries are based on the GeoJSON specification
  // http://geojson.org/geojson-spec.html (2010-08-17)
  var geoJsonDocs = [{"_id": "geoPoint", "geom": { "type": "Point", "coordinates": [100.0, 0.0] }},
    {"_id": "geoLineString", "geom": { "type": "LineString", "coordinates":[
      [100.0, 0.0], [101.0, 1.0]
    ]}},
    {"_id": "geoPolygon", "geom": { "type": "Polygon", "coordinates": [
      [ [100.0, 0.0], [101.0, 0.0], [100.0, 1.0], [100.0, 0.0] ]
    ]}},
    {"_id": "geoPolygonWithHole", "geom": { "type": "Polygon", "coordinates": [
      [ [100.0, 0.0], [101.0, 0.0], [100.0, 1.0], [100.0, 0.0] ],
      [ [100.2, 0.2], [100.6, 0.2], [100.2, 0.6], [100.2, 0.2] ]
    ]}},
    {"_id": "geoMultiPoint", "geom": { "type": "MultiPoint", "coordinates": [
      [100.0, 0.0], [101.0, 1.0]
    ]}},
    {"_id": "geoMultiLineString", "geom": { "type": "MultiLineString",
      "coordinates": [
      [ [100.0, 0.0], [101.0, 1.0] ],
      [ [102.0, 2.0], [103.0, 3.0] ]
      ]
    }},
    {"_id": "geoMultiPolygon", "geom": { "type": "MultiPolygon",
      "coordinates": [
      [[[102.0, 2.0], [103.0, 2.0], [103.0, 3.0], [102.0, 3.0], [102.0, 2.0]]],
      [
        [[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]],
        [[100.2, 0.2], [100.8, 0.2], [100.8, 0.8], [100.2, 0.8], [100.2, 0.2]]
      ]
      ]
    }},
    {"_id": "geoGeometryCollection", "geom": { "type": "GeometryCollection",
      "geometries": [
        { "type": "Point", "coordinates": [100.0, 0.0] },
        { "type": "LineString", "coordinates": [ [101.0, 0.0], [102.0, 1.0] ]}
      ]
    }}
  ];
  db.bulkSave(geoJsonDocs);
  
  bbox = [100.0, 0.0, 100.0, 0.0];
  xhr = CouchDB.request("GET", url_pre + "geoJsonGeoms?bbox=" + bbox.join(","));
  TEquals(true, /geoPoint/.test(extract_ids(xhr.responseText)),
          "if bounding box calculation was correct, it should at least" +
          " return the geoPoint");
  
  bbox = [100.8, 0.8, 101.0, 1.0],
  xhr = CouchDB.request("GET", url_pre + "geoJsonGeoms?bbox=" + bbox.join(","));
  TEquals(true, /geoPolygon/.test(extract_ids(xhr.responseText)),
          "if bounding box calculation was correct, it should at least" +
          " return the geoPolygon");
  
  bbox = [100.8, 0.8, 101.0, 1.0],
  xhr = CouchDB.request("GET", url_pre + "geoJsonGeoms?bbox=" + bbox.join(","));
  TEquals(true, /geoPolygonWithHole/.test(extract_ids(xhr.responseText)),
          "if bounding box calculation was correct, it should at least" +
          " return the geoPolygonWithHole");
  
  bbox = [100.1, 0.8, 100.2, 1.5],
  xhr = CouchDB.request("GET", url_pre + "geoJsonGeoms?bbox=" + bbox.join(","));
  TEquals(true, /geoMultiPoint/.test(extract_ids(xhr.responseText)),
          "if bounding box calculation was correct, it should at least" +
          " return the geoMultiPoint");

  bbox = [101.2, 1.3, 101.6, 1.5];
  xhr = CouchDB.request("GET", url_pre + "geoJsonGeoms?bbox=" + bbox.join(","));
  TEquals(true, /geoMultiLineString/.test(extract_ids(xhr.responseText)),
          "if bounding box calculation was correct, it should at least" +
          " return the geoMultiLineString");
  
  bbox = [101.2, 2.3, 101.6, 3.5];
  xhr = CouchDB.request("GET", url_pre + "geoJsonGeoms?bbox=" + bbox.join(","));
  TEquals(true, /geoMultiPolygon/.test(extract_ids(xhr.responseText)),
          "if bounding box calculation was correct, it should at least" +
          " return the geoMultiPolygon");
  
  bbox = [102, 0, 102, 0];
  xhr = CouchDB.request("GET", url_pre + "geoJsonGeoms?bbox=" + bbox.join(","));
  TEquals(true, /geoGeometryCollection/.test(extract_ids(xhr.responseText)),
          "if bounding box calculation was correct, it should at least" +
          " return the geoGeometryCollection");
};
