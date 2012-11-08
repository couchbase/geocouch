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

couchTests.spatial_offsets = function(debug) {
  var db = new CouchDB("test_suite_db", {"X-Couch-Full-Commit":"false"});
  db.deleteDb();
  db.createDb();

  if (debug) debugger;

  var designDoc = {
    _id : "_design/spatial",
    spatial : {
      offset : (function(doc) {
        emit({
          type: "Point",
          coordinates: [doc.number, doc.number]
        }, doc);
      }).toString()
    }
  };
  T(db.save(designDoc).ok);

  var docs = [
    {_id : "a1", letter : "a", number : 1, foo : "bar"},
    {_id : "a2", letter : "a", number : 2, foo : "bar"},
    {_id : "a3", letter : "a", number : 3, foo : "bar"},
    {_id : "b1", letter : "b", number : 1, foo : "bar"},
    {_id : "b2", letter : "b", number : 2, foo : "bar"},
    {_id : "b3", letter : "b", number : 3, foo : "bar"},
    {_id : "b4", letter : "b", number : 4, foo : "bar"},
    {_id : "b5", letter : "b", number : 5, foo : "bar"},
    {_id : "c1", letter : "c", number : 1, foo : "bar"},
    {_id : "c2", letter : "c", number : 2, foo : "bar"}
  ];
  db.bulkSave(docs);

  var url = '/test_suite_db/_design/spatial/_spatial/offset?bbox=0,0,10,10';

  var xhr = CouchDB.request("GET", url);
  var resp = JSON.parse(xhr.responseText);
  TEquals(10, resp.rows.length, "return all docs");

  xhr = CouchDB.request("GET", url + "&limit=21");
  resp = JSON.parse(xhr.responseText);
  TEquals(10, resp.rows.length, "limit greater than all docs");

  xhr = CouchDB.request("GET", url + "&limit=1");
  resp = JSON.parse(xhr.responseText);
  TEquals(1, resp.rows.length, "limit is 1");

  xhr = CouchDB.request("GET", url + "&limit=6");
  resp = JSON.parse(xhr.responseText);
  TEquals(6, resp.rows.length, "limit is 6");

  xhr = CouchDB.request("GET", url + "&skip=3");
  resp = JSON.parse(xhr.responseText);
  TEquals(7, resp.rows.length, "skip 3");

  xhr = CouchDB.request("GET", url + "&skip=4");
  resp = JSON.parse(xhr.responseText);
  TEquals(6, resp.rows.length, "skip 4");

  xhr = CouchDB.request("GET", url + "&&skip=4&limit=3");
  resp = JSON.parse(xhr.responseText);
  TEquals(3, resp.rows.length, "skip 4, limit is 3");

  xhr = CouchDB.request("GET", url + "&skip=2&limit=3");
  var oldResp = resp;
  resp = JSON.parse(xhr.responseText);
  TEquals(3, resp.rows.length, "skip 2, limit is 3");
  TEquals(true, JSON.stringify(resp.rows)!==JSON.stringify(oldResp.rows),
    "response was different from previous");

  xhr = CouchDB.request("GET", url + "&skip=1&limit=4");
  resp = JSON.parse(xhr.responseText);
  TEquals(4, resp.rows.length, "skip 1, limit is 4");
  xhr = CouchDB.request("GET", url + "&skip=5&limit=3");
  oldResp = resp;
  resp = JSON.parse(xhr.responseText);
  TEquals(3, resp.rows.length, "skip 5, limit is 3");
  var rows = oldResp.rows.concat(resp.rows);
  xhr = CouchDB.request("GET", url + "&skip=1&limit=7");
  resp = JSON.parse(xhr.responseText);
  TEquals(7, resp.rows.length, "skip 1, limit is 7");
  TEquals(true, JSON.stringify(resp.rows)===JSON.stringify(rows),
    "two concatenated requests are the same as a single one");

  xhr = CouchDB.request("GET", url + "&skip=6&limit=5");
  resp = JSON.parse(xhr.responseText);
  TEquals(4, resp.rows.length, "skip + limit > total, but limit < total");
};
