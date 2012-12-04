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

couchTests.spatial_range = function(debug) {
  var db = new CouchDB("test_suite_db", {"X-Couch-Full-Commit":"false"});
  db.deleteDb();
  db.createDb();

  if (debug) debugger;


  var designDoc = {
    _id:"_design/spatial",
    language: "javascript",
    spatial: {
      withGeometry: (function(doc) {
        emit([{
          type: "Point",
          coordinates: doc.loc
        }, [doc.integer, doc.integer+5]], doc.string);
      }).toString(),
      noGeometry: (function(doc) {
        emit([[doc.integer, doc.integer+1], doc.integer*3,
          [doc.integer-14, doc.integer+100], doc.integer],
          doc.string);
      }).toString()
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

  // returns start_range and end_range arguments
  function rangeArgs(range) {
      return 'start_range=' + JSON.stringify(range.start) +
        '&end_range=' + JSON.stringify(range.end);
  }

  var xhr;
  var url_pre = '/test_suite_db/_design/spatial/_spatial/';
  var docs = makeSpatialDocs(0, 10);
  db.bulkSave(docs);
  db.save({_id: '10', string: '10', integer: 10, loc: [1,1]});
  var bbox = [-180, -90, 180, 90];


  // Tests with geometry

  var range = {start: [-20, 0, 6.4], end: [16, 25, 8.7]};
  xhr = CouchDB.request("GET", url_pre + "withGeometry?" + rangeArgs(range));
  TEquals(['2','3','4','5'], extract_ids(xhr.responseText),
          "should return a subset of the geometries");

  range = {start: [-17, 0, 8.8], end: [16, 25, 8.8]};
  xhr = CouchDB.request("GET", url_pre + "withGeometry?" + rangeArgs(range));
  TEquals(['4','5'], extract_ids(xhr.responseText),
          "should return a subset of the geometries " +
          "(3rd dimension is single point)");

  range = {start: [-17, 0, null], end: [16, 25, null]};
  xhr = CouchDB.request("GET", url_pre + "withGeometry?" + rangeArgs(range));
  TEquals(['10','2','3','4','5'], extract_ids(xhr.responseText),
          "should return a subset of the geometries " +
          "(3rd dimension is a wildcard)");

  range = {start: [-17, 0, null], end: [16, 25, 8.8]};
  xhr = CouchDB.request("GET", url_pre + "withGeometry?" + rangeArgs(range));
  TEquals(['2','3','4','5'], extract_ids(xhr.responseText),
          "should return a subset of the geometries " +
          "(3rd dimension is open at the start)");

  range = {start: [-17, 0, 8.8], end: [16, 25, null]};
  xhr = CouchDB.request("GET", url_pre + "withGeometry?" + rangeArgs(range));
  TEquals(['10','4','5'], extract_ids(xhr.responseText),
          "should return a subset of the geometries " +
          "(3rd dimension is open at the end)");


  // Tests without geometry

  range = {start: [3, 0, -10, 2], end: [10, 21, -9, 20]};
  xhr = CouchDB.request("GET", url_pre + "noGeometry?" + rangeArgs(range));
  TEquals(['2','3','4','5'], extract_ids(xhr.responseText),
          "should return a subset of the geometries");

  range = {start: [3, 0, -7, 5], end:[10, 21, -7, 20]};
  xhr = CouchDB.request("GET", url_pre + "noGeometry?" + rangeArgs(range));
  TEquals(['5','6','7'], extract_ids(xhr.responseText),
          "should return a subset of the geometries" +
          "(3rd dimension is a point)");

  range = {start: [3, null, -2, 4], end: [10, null, -2, 20]};
  xhr = CouchDB.request("GET", url_pre + "noGeometry?" + rangeArgs(range));
  TEquals(['10','4','5','6','7','8','9'], extract_ids(xhr.responseText),
          "should return a subset of the geometries" +
          "(2nd dimension is a wildcard)");

  range = {start: [3, null, -2, 4], end: [10, 15, -2, 20]};
  xhr = CouchDB.request("GET", url_pre + "noGeometry?" + rangeArgs(range));
  TEquals(['4','5'], extract_ids(xhr.responseText),
          "should return a subset of the geometries" +
          "(2nd dimension is open at the start)");

  range = {start: [3, 20, -2, 4], end: [10, null, -2, 20]};
  xhr = CouchDB.request("GET", url_pre + "noGeometry?" + rangeArgs(range));
  TEquals(['10','7','8','9'], extract_ids(xhr.responseText),
          "should return a subset of the geometries" +
          "(2nd dimension is open at the end)");
};
