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

// These tests are here to make sure bugs are fixed
couchTests.spatial_bugfixes = function(debug) {
  var db = new CouchDB("test_suite_db", {"X-Couch-Full-Commit":"true"});
  db.deleteDb();
  db.createDb();

  if (debug) debugger;


  var designDoc = {
    _id:"_design/spatial",
    language: "javascript",
    spatial : {
      points : (function(doc) {
        if (doc.loc) {
          emit({
            type: "Point",
            coordinates: [doc.loc[0], doc.loc[1]]
          }, doc._id);
        }
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

  // wait for a certain number of seconds
  function wait(secs) {
    var t0 = new Date(), t1;
    do {
      CouchDB.request("GET", "/");
      t1 = new Date();
    } while ((t1 - t0) < secs*1000);
  }

  var xhr;
  var url_pre = '/test_suite_db/_design/spatial/_spatial/';

  gc_1_spatial_index_loses_documents_after_restart();


  function gc_1_spatial_index_loses_documents_after_restart() {
    var bbox = [-180, -90, 180, 90];
    var docs = makeSpatialDocs(0, 2);
    db.bulkSave(docs);

    // build up the spatial index
    xhr = CouchDB.request("GET", url_pre + "points?bbox=" + bbox.join(","));
    TEquals(['0','1'], extract_ids(xhr.responseText),
            "Should return all inserted documents");
    // Wait that everything got commit before the restart
    wait(2);
    restartServer();

    docs = makeSpatialDocs(2, 3);
    db.save(docs[0]);

    // build up the spatial index again
    xhr = CouchDB.request("GET", url_pre + "points?bbox=" + bbox.join(","));
    TEquals(['0','1','2'], extract_ids(xhr.responseText),
          "Should return all inserted documents");
  }
};
