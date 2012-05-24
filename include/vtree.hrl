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
% (UTF-8) string
-type keyval() :: number() | string().

% The multidimensional bounding box
-type mbb() :: [{Min :: keyval(), Max :: keyval()}].
% The node format for the splits. It contains the MBB and in case of a:
%  1. KV node: the pointer to the node in the file
%  2. KP node: a list of pointers to its children
-type split_node() :: {Mbb :: mbb(),
                       KvPosOrChildren :: integer() | [integer()]}.

-type candidate() :: {[split_node()], [split_node()]}.

% The less function compares two values and returns true if the former is
% less than the latter
-type lessfun() :: fun((keyval(), keyval()) -> boolean()).
