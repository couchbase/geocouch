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

-module(run_vtreeviz).
-export([run/0]).

% Parameters are file (the filename) and pos (the position of the root node)
run() ->
    {ok, [[Filename]]} = init:get_argument(file),
    {ok, [[PosString]]} = init:get_argument(pos),
    {Pos, _} = string:to_integer(PosString),
    case couch_file:open(Filename, [read]) of
    {ok, Fd} ->
        vtreeviz:visualize(Fd, Pos),
        ok;
    {error, Reason} ->
        io:format("ERROR (~s): Couldn't open file (~s) for tree storage~n",
                  [Reason, Filename])
    end.
