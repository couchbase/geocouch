% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.


% The value a key can have. For the vtree that is either a number or
% (UTF-8) string. `nil` is used as a wildcard in queries, to match open or
% full ranges.
-type keyval() :: number() | string() | nil.

% The multidimensional bounding box
-type mbb() :: [{Min :: keyval(), Max :: keyval()}].

-type geom_type() :: 'Point' | 'LineString' | 'Polygon' | 'MultiPoint' |
                     'MultiLineString' | 'MultiPolygon' | 'GeometryCollection'.
-type geom_coords() :: [number()] | [geom_coords()].
-type geom() :: {Type :: geom_type(), Coordinates :: geom_coords()}.

% No idea what the json type will be yet
-type json() :: any().

-type kp_value() :: {PointerNode :: pos_integer(),
                     TreeSize :: non_neg_integer(), Reduce :: any()}.
-type kv_value() :: {DocId :: binary(), Geometry :: geom(), Body :: json()}.
%% The node format for the splits. It contains the MBB and in case of a:
%%  1. KV node: the pointer to the node in the file
%%  2. KP node: a list of pointers to its children
%%  3. abused: any value
%-type split_node() :: {Mbb :: mbb(),
%                       KvPosOrChildren :: integer() | [integer()]}.
%-type split_node() :: {Mbb :: mbb(), KpOrKv :: kp_value() | kp_value()}.
-type split_node() :: {Mbb :: mbb(), KpOrKv :: kp_value() | kp_value() | any()}.

-type candidate() :: {[split_node()], [split_node()]}.

% The less function compares two values and returns true if the former is
% less than the latter
-type lessfun() :: fun((keyval(), keyval()) -> boolean()).


-define(KP_NODE, 0).
-define(KV_NODE, 1).


-record(kv_node, {
          key = [] :: mbb(),
          docid = nil :: binary() | nil ,
          geometry = nil :: geom() | nil | pos_integer(),
          body = nil :: json() | nil | pos_integer()
}).

-record(kp_node, {
          key = [] :: mbb(),
          childpointer = [] :: non_neg_integer(),
          treesize = 0 :: non_neg_integer(),
          reduce = nil :: any(),
          mbb_orig = [] :: mbb()
}).

-record(vtree, {
          fd = nil :: file:io_device() | nil,
          % The root node of the tree
          %root = nil ::kp_value() | kv_value()
          root = nil :: #kp_node{} | nil,
          less = fun(A, B) -> A < B end,
          % `fill_min` and `fill_max` are normally set by vtree_state (which
          % is part of the Couchbase/Apache CouchDB API implementation)
          fill_min = nil,
          fill_max = nil,
          reduce = nil :: any(),
          chunk_threshold = 16#4ff
}).
