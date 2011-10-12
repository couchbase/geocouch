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

couchTests.spatial_opensearch = function(debug) {
  var db = new CouchDB("test_suite_db", {"X-Couch-Full-Commit":"false"});
  db.deleteDb();
  db.createDb();

  if (debug) debugger;


  var designDoc = {
    _id:"_design/spatial",
    language: "javascript",
    spatial : {
      basicIndex : stringFun(function(doc) {
        if (doc.loc && doc.string) {
          emit({
            type: "Point",
            coordinates: [doc.loc[0], doc.loc[1]]
          }, doc.string);
        }
      }),
      dontEmitAll : stringFun(function(doc) {
        if (doc._id > 5 && doc.loc && doc.string) {
          emit({
            type: "Point",
            coordinates: [doc.loc[0], doc.loc[1]]
          }, doc.string);
        }
      }),
      emitNothing : stringFun(function(doc) {}),
      geoJsonGeoms : stringFun(function(doc) {
        if (doc._id.substr(0,3)=="geo" && doc.geom) {
          emit(doc.geom, null);
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
    for (var i in json.rows) {
      res.push(json.rows[i].id);
    }
    return res.sort();
  }

  // wait for a certain number of seconds
  function wait(secs) {
    var t0 = new Date(), t1;
    do {
      CouchDB.request("GET", "/");
      t1 = new Date();
    } while ((t1 - t0) < secs*1000);
  }

  function getGeomById(str, geomId) {
    var json = JSON.parse(str);
    for (var i in json.rows) {
      if (json.rows[i].id===geomId) {
        return json.rows[i].geometry;
      }
    }
  }

  var xhr;
  var url_pre = '/test_suite_db/_design/spatial/_spatial/';
  var docs = makeSpatialDocs(0, 10);
  db.bulkSave(docs);
  var bbox = [-180, -90, 180, 90];


  // bounding box tests

  xhr = CouchDB.request("GET", url_pre + "basicIndex?bbox=" + bbox.join(","));
  TEquals(['0','1','2','3','4','5','6','7','8','9'],
          extract_ids(xhr.responseText),
          "should return all geometries");

  bbox = [-20, 0, 0, 20];
  xhr = CouchDB.request("GET", url_pre + "basicIndex?bbox=" + bbox.join(","));
  TEquals(['0','1','2'], extract_ids(xhr.responseText),
          "should return a subset of the geometries");

  bbox = [0, 4, 180, 90];
  xhr = CouchDB.request("GET", url_pre + "basicIndex?bbox=" + bbox.join(","));
  TEquals("{\"rows\":[]}\n", xhr.responseText,
          "should return no geometries");

  bbox = [-18, 17, -14, 21];
  xhr = CouchDB.request("GET", url_pre + "basicIndex?bbox=" + bbox.join(","));
  TEquals(['1','2','3'], extract_ids(xhr.responseText),
          "should also return geometry at the bounds of the bbox");

  bbox = [-16, 19, -16, 19];
  xhr = CouchDB.request("GET", url_pre + "basicIndex?bbox=" + bbox.join(","));
  TEquals(['2'], extract_ids(xhr.responseText),
          "bbox collapsed to a point should return the geometries there");


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
    {"_id": "geoLshapedPolygon", "geom": {"type":"Polygon", "coordinates":[
      [[-11.25, 48.1640625], [-11.953125, 22.8515625], [35.859375, 21.4453125],
      [35.859375, -10.8984375], [61.171875, -11.6015625],
      [60.46875, 47.4609375], [60.46875, 46.0546875], [-11.25, 48.1640625]]]}},
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

  bbox = [-55, -35, 27, 16];
  xhr = CouchDB.request("GET", url_pre + "geoJsonGeoms?bbox=" + bbox.join(","));
  TEquals(true, /geoLshapedPolygon/.test(extract_ids(xhr.responseText)),
          "if bounding box calculation was correct, it should at least" +
          " return the l-shaped polygon. bbox only compares bounding boxes," +
          " hence the l-shaped polygon is included.");

  // Geometry search tests

  function geometryRequest(geom) {
      return CouchDB.request(
          "GET", url_pre + "geoJsonGeoms?geometry=" + escape(geom));
  }

  // The geometry is basically a bounding box
  var geom = 'POLYGON((-55 -35, 27 -35, 27 16, -55 16, -55 -35))';
  xhr = geometryRequest(geom);
  TEquals(false, /geoLshapedPolygon/.test(extract_ids(xhr.responseText)),
          "if the calculations were correct, it shouldn't return" +
          " the l-shaped polygon.");

  geom = 'LINESTRING(101.4 1.2, 99.7 0.4, 98 -1.8)';
  xhr = geometryRequest(geom);
  TEquals(3, extract_ids(xhr.responseText).length,
          "Intersects 3 geometries.");

  geom = 'POINT(100.68151855471 0.50092361844019)';
  xhr = geometryRequest(geom);
  TEquals(0, extract_ids(xhr.responseText).length,
          "Within a hole of a polygon. Shouldn't intersect anything.");

  geom = 'POLYGON((-19 0, 0 1, 1.5 20, -23 21.3, -19 0))';
  xhr = CouchDB.request("GET", url_pre + "basicIndex?geometry="+escape(geom));
  TEquals(['0','1','2'], extract_ids(xhr.responseText),
          "searching with polygon");

  geom = 'MULTILINESTRING((101.23083496095 0.53388125850398, 101.51647949219 0.13838103734708), (101.43957519531 0.87443120888936, 102.20861816403 0.41303579755691))';
  xhr = geometryRequest(geom);
  TEquals(1, extract_ids(xhr.responseText).length,
          "Intersects one LineString twice.");

  geom = 'MULTIPOINT((102.34594726559 2.6944553397438), (100.62109375004 1.8491806666906), (100.55517578129 -0.41367822221998))';
  xhr = geometryRequest(geom);
  TEquals(1, extract_ids(xhr.responseText).length,
          "Intersects one geometry.");

  geom = 'MULTIPOLYGON (((102.2196044921600020 1.6652462877900001, 101.1099853515799936 2.0385856805057001, 100.3079833984800047 3.0483190208145001, 101.2967529296900011 3.3225525920246000, 102.8348388671300029 3.5418849006547002, 104.1641845702000069 2.5764772510784999, 103.6038818358500038 2.4337915164603001, 102.8897705077500007 3.2128679544584999, 101.5823974609299967 3.1251117377839002, 101.2747802734400011 2.3898851337089000, 101.4285888671900011 2.0605442798878002, 101.4285888671900011 2.0715234662377000, 102.2196044921600020 1.6652462877900001)), ((100.1322021484899949 2.3679314141202998, 100.2091064453600069 1.3797028906988000, 100.5826416015899980 0.6876810668454200, 101.0440673828299936 1.5334617387448000, 101.5164794921900011 1.1490463293633000, 102.5272216796399931 -0.0154274216757980, 102.6810302733899931 1.2588853394238999, 100.1322021484899949 2.3679314141202998)))';
  xhr = geometryRequest(geom);
  TEquals(2, extract_ids(xhr.responseText).length,
          "Intersects two geometries.");
};
