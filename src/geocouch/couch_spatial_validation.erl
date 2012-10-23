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

-module(couch_spatial_validation).

-export([validate_ddoc_spatial/1]).

-include("couch_db.hrl").


validate_ddoc_spatial(#doc{body = {Body}}) ->
    Spatial = couch_util:get_value(<<"spatial">>, Body, {[]}),
    case Spatial of
    {L} when is_list(L) ->
        ok;
    _ ->
        throw({error, <<"The field `spatial' is not a json object.">>})
    end,
    lists:foreach(
        fun({SpatialName, Value}) ->
            validate_spatial_definition(SpatialName, Value)
        end,
        element(1, Spatial)).


validate_spatial_definition(<<"">>, _) ->
    throw({error, <<"Spatial view name cannot be an empty string">>});
validate_spatial_definition(SpatialName, SpatialDef) when
        is_binary(SpatialDef) ->
    validate_spatial_name(SpatialName, iolist_to_binary(io_lib:format(
        "Spatial view name `~s` cannot have leading or trailing whitespace",
        [SpatialName]))),
    validate_spatial_function(SpatialName, SpatialDef);
validate_spatial_definition(SpatialName, _) ->
    ErrorMsg = io_lib:format("Value for spatial view `~s' is not "
                             "a string.", [SpatialName]),
    throw({error, iolist_to_binary(ErrorMsg)}).


% Make sure the view name doesn't contain leading or trailing whitespace
% (space, tab, newline or carriage return)
validate_spatial_name(<<" ", _Rest/binary>>, ErrorMsg) ->
    throw({error, ErrorMsg});
validate_spatial_name(<<"\t", _Rest/binary>>, ErrorMsg) ->
    throw({error, ErrorMsg});
validate_spatial_name(<<"\n", _Rest/binary>>, ErrorMsg) ->
    throw({error, ErrorMsg});
validate_spatial_name(<<"\r", _Rest/binary>>, ErrorMsg) ->
    throw({error, ErrorMsg});
validate_spatial_name(Bin, ErrorMsg) when size(Bin) > 1 ->
    Size = size(Bin) - 1 ,
    <<_:Size/binary, Trailing/bits>> = Bin,
    % Check for trailing whitespace
    validate_spatial_name(Trailing, ErrorMsg);
validate_spatial_name(_, _) ->
    ok.


validate_spatial_function(SpatialName, SpatialDef) ->
    case mapreduce:start_map_context([SpatialDef]) of
    {ok, _Ctx} ->
        ok;
    {error, Reason} ->
        ErrorMsg = io_lib:format("Syntax error in the spatial function of"
                                 " the spatial view `~s': ~s",
                                 [SpatialName, Reason]),
        throw({error, iolist_to_binary(ErrorMsg)})
    end.
