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

couchTests.spatial_design_docs = function(debug) {
  var db = new CouchDB("test_suite_db", {"X-Couch-Full-Commit":"false"});
  db.deleteDb();
  db.createDb();

  if (debug) debugger;

  var designDoc = {
    _id:"_design/spatial",
    language: "javascript",
    /* This is a 1.1.x feature, disable for now
    views: {
      lib: {
        geo: "exports.type = 'Point';"
      }
    }, */
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
      /* This is a 1.1.x feature, disable for now
      withCommonJs : stringFun(function(doc) {
        var lib = require('views/lib/geo');
        emit({
          type: lib.type,
          coordinates: [doc.loc[0], doc.loc[1]]
        }, doc.string);
      })*/
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
  var docs = makeSpatialDocs(0, 10);
  db.bulkSave(docs);
  var bbox = [-180, -90, 180, 90];

/* This is a 1.1.x feature, disable for now
  // test if CommonJS modules can be imported
  bbox = [-180, -90, 180, 90];
  xhr = CouchDB.request("GET", url_pre + "withCommonJs?bbox=" + bbox.join(","));
  T(xhr.status == 200);
  TEquals(['0','1','2','3','4','5','6','7','8','9'],
          extract_ids(xhr.responseText),
          "should return all geometries (test with CommonJS)");
*/


  // test that we get design doc info back
  xhr = CouchDB.request("GET", url_pre + '_info');
  var resp = JSON.parse(xhr.responseText);
  TEquals("spatial", resp.name);
  var sinfo = resp.spatial_index;
  TEquals(51, sinfo.disk_size);
  TEquals(false, sinfo.compact_running);
  // test that GET /db/_design/test/_info
  // hasn't triggered an update of the views
  wait(3);
  xhr = CouchDB.request("GET", url_pre + "basicIndex?bbox="
                        + bbox.join(",") + '&stale=ok');
  T(JSON.parse(xhr.responseText).rows.length === 0);

  // test that POST /db/_spatial_cleanup
  // doesn't trigger an update of the views
  xhr = CouchDB.request("GET", '/test_suite_db/_spatial_cleanup');
  wait(3);
  xhr = CouchDB.request("GET", url_pre + "basicIndex?bbox="
                        + bbox.join(",") + '&stale=ok');
  T(JSON.parse(xhr.responseText).rows.length === 0);
};
