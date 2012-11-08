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

couchTests.etags_spatial = function(debug) {
  var db = new CouchDB("test_suite_db", {"X-Couch-Full-Commit":"true"});
  db.deleteDb();
  db.createDb();
  if (debug) debugger;

  var designDoc = {
    _id:"_design/etags",
    language: "javascript",
    spatial: {
      basicIndex: stringFun(function(doc) {
        emit({
          type: "Point",
          coordinates: [doc.loc[0], doc.loc[1]]
        }, doc.string);
      }),
      fooIndex: stringFun(function(doc) {
        if (doc.foo) {
          emit({
            type: "Point",
            coordinates: [1, 2]
          }, 1);
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

  var xhr;
  var url_pre = '/test_suite_db/_design/etags/_spatial/';
  var docs = makeSpatialDocs(0, 10);
  db.bulkSave(docs);

  var bbox = [-180, -90, 180, 90];
  // verify get w/Etag on spatial index
  xhr = CouchDB.request("GET", url_pre + "basicIndex?bbox=" + bbox.join(","));
  T(xhr.status == 200);
  var etag = xhr.getResponseHeader("etag");
  xhr = CouchDB.request("GET", url_pre + "basicIndex?bbox=" + bbox.join(","), {
    headers: {"if-none-match": etag}
  });
  T(xhr.status == 304);

  // verify ETag doesn't change when an update
  // doesn't change the view group's index
  T(db.save({"_id":"doc1", "foo":"bar"}).ok);
  xhr = CouchDB.request("GET", url_pre + "basicIndex?bbox=" + bbox.join(","));
  var etag1 = xhr.getResponseHeader("etag");
  T(etag1 == etag);

/*
  // NOTE vmx (2011-01-13) purging is not yet implemented for the spatial index
  // Verify that purges affect etags
  xhr = CouchDB.request("GET", url_pre + "fooIndex?bbox=" + bbox.join(","));
  var foo_etag = xhr.getResponseHeader("etag");
  var doc1 = db.open("doc1");
  xhr = CouchDB.request("POST", "/test_suite_db/_purge", {
    body: JSON.stringify({"doc1":[doc1._rev]})
  });
  xhr = CouchDB.request("GET", url_pre + "fooIndex?bbox=" + bbox.join(","));
  etag1 = xhr.getResponseHeader("etag");
  T(etag1 != foo_etag);

  // Test that _purge didn't affect the other view etags.
  xhr = CouchDB.request("GET", "/test_suite_db/_design/etags/_view/basicView");
  etag1 = xhr.getResponseHeader("etag");
  T(etag1 == etag);
*/

  // verify different views in the same view group may have different ETags
  xhr = CouchDB.request("GET", url_pre + "fooIndex?bbox=" + bbox.join(","));
  etag1 = xhr.getResponseHeader("etag");
  xhr = CouchDB.request("GET", url_pre + "basicIndex?bbox=" + bbox.join(","));
  var etag2 = xhr.getResponseHeader("etag");
  T(etag1 != etag2);

  // verify ETag changes when an update changes the view group's index.
  db.bulkSave(makeSpatialDocs(10, 20));
  xhr = CouchDB.request("GET", url_pre + "basicIndex?bbox=" + bbox.join(","));
  etag1 = xhr.getResponseHeader("etag");
  T(etag1 != etag);

  // verify ETag is the same after a restart
  restartServer();
  xhr = CouchDB.request("GET", url_pre + "basicIndex?bbox=" + bbox.join(","));
  etag2 = xhr.getResponseHeader("etag");
  T(etag1 == etag2);
};
