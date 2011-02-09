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

couchTests.multiple_spatial_rows = function(debug) {
  var db = new CouchDB("test_suite_db", {"X-Couch-Full-Commit":"true"});
  db.deleteDb();
  db.createDb();
  if (debug) debugger;

  var designDoc = {
    _id: "_design/multirows",
    language: "javascript",
    spatial: {
      sameKey: (function(doc) {
        for (var i=0; i<doc.cities.length; i++) {
          emit({
            type: "Point",
            coordinates: doc.loc
          }, doc.cities[i] + ', ' + doc._id);
        }
      }).toString(),
      differentKey: (function(doc) {
        for (var i=0; i<doc.cities.length; i++) {
          emit({
            type: "Point",
            coordinates: [doc.loc[0]+i, doc.loc[1]+i]
          }, doc.cities[i] + ', ' + doc._id);
        }
      }).toString()
    }
  };
  T(db.save(designDoc).ok);

  function extract_values(str) {
    var json = JSON.parse(str);
    var res = [];
    for (var i=0; i<json.rows.length; i++) {
      res.push(json.rows[i].value);
    }
    return res.sort();
  }


  var nc = {_id: "NC", loc: [-80, 35], cities: ["Charlotte", "Raleigh"]};
  var ma = {_id: "MA", loc: [-72, 42], cities:
    ["Boston", "Lowell", "Worcester", "Cambridge", "Springfield"]};
  var fl = {_id: "FL", loc: [-82, 28], cities:
    ["Miami", "Tampa", "Orlando", "Springfield"]};

  T(db.save(nc).ok);
  T(db.save(ma).ok);
  T(db.save(fl).ok);

  var url_pre = '/test_suite_db/_design/multirows/_spatial/';
  var bbox = [-180, -90, 180, 90];

  var xhr = CouchDB.request("GET", url_pre + "sameKey?bbox=" + bbox.join(","));
  var rows = extract_values(xhr.responseText);
  T(rows[0] == "Boston, MA");
  T(rows[1] == "Cambridge, MA");
  T(rows[2] == "Charlotte, NC");
  T(rows[3] == "Lowell, MA");
  T(rows[4] == "Miami, FL");
  T(rows[5] == "Orlando, FL");
  T(rows[6] == "Raleigh, NC");
  T(rows[7] == "Springfield, FL");
  T(rows[8] == "Springfield, MA");
  T(rows[9] == "Tampa, FL");
  T(rows[10] == "Worcester, MA");

  // add another city to NC
  nc.cities.push("Wilmington");
  T(db.save(nc).ok);

  xhr = CouchDB.request("GET", url_pre + "sameKey?bbox=" + bbox.join(","));
  rows = extract_values(xhr.responseText);
  T(rows[0] == "Boston, MA");
  T(rows[1] == "Cambridge, MA");
  T(rows[2] == "Charlotte, NC");
  T(rows[3] == "Lowell, MA");
  T(rows[4] == "Miami, FL");
  T(rows[5] == "Orlando, FL");
  T(rows[6] == "Raleigh, NC");
  T(rows[7] == "Springfield, FL");
  T(rows[8] == "Springfield, MA");
  T(rows[9] == "Tampa, FL");
  T(rows[10] == "Wilmington, NC");
  T(rows[11] == "Worcester, MA");

  // now delete MA
  T(db.deleteDoc(ma).ok);

  xhr = CouchDB.request("GET", url_pre + "sameKey?bbox=" + bbox.join(","));
  rows = extract_values(xhr.responseText);
  T(rows[0] == "Charlotte, NC");
  T(rows[1] == "Miami, FL");
  T(rows[2] == "Orlando, FL");
  T(rows[3] == "Raleigh, NC");
  T(rows[4] == "Springfield, FL");
  T(rows[5] == "Tampa, FL");
  T(rows[6] == "Wilmington, NC");


  // and now with different keys
  T(db.deleteDoc(nc).ok);
  T(db.deleteDoc(fl).ok);
  nc.cities.pop("Wilmington");
  delete nc._deleted;
  delete ma._deleted;
  delete fl._deleted;
  T(db.save(nc).ok);
  T(db.save(ma).ok);
  T(db.save(fl).ok);

  xhr = CouchDB.request(
    "GET", url_pre + "differentKey?bbox=" + bbox.join(","));
  rows = extract_values(xhr.responseText);
  T(rows[0] == "Boston, MA");
  T(rows[1] == "Cambridge, MA");
  T(rows[2] == "Charlotte, NC");
  T(rows[3] == "Lowell, MA");
  T(rows[4] == "Miami, FL");
  T(rows[5] == "Orlando, FL");
  T(rows[6] == "Raleigh, NC");
  T(rows[7] == "Springfield, FL");
  T(rows[8] == "Springfield, MA");
  T(rows[9] == "Tampa, FL");
  T(rows[10] == "Worcester, MA");

  // add another city to NC
  nc.cities.push("Wilmington");
  T(db.save(nc).ok);

  xhr = CouchDB.request(
    "GET", url_pre + "differentKey?bbox=" + bbox.join(","));
  rows = extract_values(xhr.responseText);
  T(rows[0] == "Boston, MA");
  T(rows[1] == "Cambridge, MA");
  T(rows[2] == "Charlotte, NC");
  T(rows[3] == "Lowell, MA");
  T(rows[4] == "Miami, FL");
  T(rows[5] == "Orlando, FL");
  T(rows[6] == "Raleigh, NC");
  T(rows[7] == "Springfield, FL");
  T(rows[8] == "Springfield, MA");
  T(rows[9] == "Tampa, FL");
  T(rows[10] == "Wilmington, NC");
  T(rows[11] == "Worcester, MA");

  // now delete MA
  T(db.deleteDoc(ma).ok);

  xhr = CouchDB.request(
    "GET", url_pre + "differentKey?bbox=" + bbox.join(","));
  rows = extract_values(xhr.responseText);
  T(rows[0] == "Charlotte, NC");
  T(rows[1] == "Miami, FL");
  T(rows[2] == "Orlando, FL");
  T(rows[3] == "Raleigh, NC");
  T(rows[4] == "Springfield, FL");
  T(rows[5] == "Tampa, FL");
  T(rows[6] == "Wilmington, NC");
};
