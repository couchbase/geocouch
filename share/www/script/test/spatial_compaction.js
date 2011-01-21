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

couchTests.spatial_compaction = function(debug) {

  if (debug) debugger;

  var db = new CouchDB("test_suite_db", {"X-Couch-Full-Commit": "true"});

  db.deleteDb();
  db.createDb();

  var ddoc = {
    _id: "_design/compaction",
    language: "javascript",
    spatial: {
      basicIndex: (function(doc) {
        emit({
          type: "Point",
          coordinates: doc.loc
        }, doc.string);
      }).toString(),
      fooIndex: (function(doc) {
          if (doc._id<500) {
            emit({
              type: "Point",
              coordinates: [1, 2]
            }, 1);
          }
      }).toString()
    }
  };
  T(db.save(ddoc).ok);

  function makeSpatialDocs(start, end, templateDoc) {
    var docs = makeDocs(start, end, templateDoc);
    for (var i=0; i<docs.length; i++) {
        docs[i].loc = [i-20+docs[i].integer, i+15+docs[i].integer];
    }
    return docs;
  }

  var xhr;
  var url_pre = '/test_suite_db/_design/compaction/_spatial/';
  var docs = makeSpatialDocs(0, 1000);
  db.bulkSave(docs);

  var bbox = [-10000, -10000, 10000, 10000];

  xhr = CouchDB.request("GET", url_pre + "basicIndex?bbox=" + bbox.join(","));
  var resp = JSON.parse(xhr.responseText);
  T(resp.rows.length === 1000);

  xhr = CouchDB.request("GET", url_pre + "fooIndex?bbox=" + bbox.join(","));
  resp = JSON.parse(xhr.responseText);
  T(resp.rows.length === 500);

  //resp = db.designInfo("_design/compaction");
  xhr = CouchDB.request("GET",
      '/test_suite_db/_design/compaction/_spatialinfo');
  resp = JSON.parse(xhr.responseText);
  T(resp.spatial_index.update_seq === 1001);


  // update docs
  for (var i = 0; i < docs.length; i++) {
    docs[i].integer = docs[i].integer + 1;
  }
  db.bulkSave(docs);


  xhr = CouchDB.request("GET", url_pre + "basicIndex?bbox=" + bbox.join(","));
  resp = JSON.parse(xhr.responseText);
  T(resp.rows.length === 1000);

  xhr = CouchDB.request("GET", url_pre + "fooIndex?bbox=" + bbox.join(","));
  resp = JSON.parse(xhr.responseText);
  T(resp.rows.length === 500);

  xhr = CouchDB.request("GET",
      '/test_suite_db/_design/compaction/_spatialinfo');
  resp = JSON.parse(xhr.responseText);
  T(resp.spatial_index.update_seq === 2001);


  // update docs again...
  for (var i = 0; i < docs.length; i++) {
    docs[i].integer = docs[i].integer + 2;
  }
  db.bulkSave(docs);


  xhr = CouchDB.request("GET", url_pre + "basicIndex?bbox=" + bbox.join(","));
  resp = JSON.parse(xhr.responseText);
  T(resp.rows.length === 1000);

  xhr = CouchDB.request("GET", url_pre + "fooIndex?bbox=" + bbox.join(","));
  resp = JSON.parse(xhr.responseText);
  T(resp.rows.length === 500);

  xhr = CouchDB.request("GET",
      '/test_suite_db/_design/compaction/_spatialinfo');
  resp = JSON.parse(xhr.responseText);
  T(resp.spatial_index.update_seq === 3001);

  var disk_size_before_compact = resp.spatial_index.disk_size;

  // compact view group
  xhr = CouchDB.request("POST", "/" + db.name + "/_spatialcompact/compaction");
  T(JSON.parse(xhr.responseText).ok === true);

  xhr = CouchDB.request("GET",
      '/test_suite_db/_design/compaction/_spatialinfo');
  resp = JSON.parse(xhr.responseText);
  while (resp.spatial_index.compact_running === true) {
    xhr = CouchDB.request("GET",
        '/test_suite_db/_design/compaction/_spatialinfo');
    resp = JSON.parse(xhr.responseText);
  }


  xhr = CouchDB.request("GET", url_pre + "basicIndex?bbox=" + bbox.join(","));
  resp = JSON.parse(xhr.responseText);
  T(resp.rows.length === 1000);

  xhr = CouchDB.request("GET", url_pre + "fooIndex?bbox=" + bbox.join(","));
  resp = JSON.parse(xhr.responseText);
  T(resp.rows.length === 500);

  xhr = CouchDB.request("GET",
      '/test_suite_db/_design/compaction/_spatialinfo');
  resp = JSON.parse(xhr.responseText);
  T(resp.spatial_index.update_seq === 3001);
  T(resp.spatial_index.disk_size < disk_size_before_compact);
};
