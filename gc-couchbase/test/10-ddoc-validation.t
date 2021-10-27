#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable

% Copyright 2015-Present Couchbase, Inc.
%
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

test_set_name() -> <<"couch_test_spatial_ddoc_validation">>.
num_set_partitions() -> 4.
ddoc_id() -> <<"_design/test">>.


-define(etap_match(Got, Expected, Desc),
        etap:fun_is(fun(XXXXXX) ->
            case XXXXXX of Expected -> true; _ -> false end

        end, Got, Desc)).


main(_) ->
    test_util:init_code_path(),

    etap:plan(17),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail(Other)
    end,
    %init:stop(),
    %receive after infinity -> ok end,
    ok.


test() ->
    spatial_test_util:start_server(test_set_name()),

    etap:diag("Testing the design document validation of spatial views"),

    test_spatial_syntax_error(),
    test_spatial_name_error(),
    test_spatial_value_error(),


    spatial_test_util:stop_server(),
    ok.


test_spatial_syntax_error() ->
    etap:diag("Testing spatial function with invalid syntax"),
    setup_test(),
    ViewFun = <<"function(doc, meta) { emit([0, 1], 1); ">>,
    validate_ddoc(<<"syntaxerror">>, ViewFun),
    shutdown_test().


test_spatial_name_error() ->
    etap:diag("Testing spatial function with invalid name"),
    setup_test(),

    view_name_error(<<"  test  ">>),
    view_name_error(<<"test ">>),
    view_name_error(<<"   test ">>),
    view_name_error(<<"test\t">>),
    view_name_error(<<"\ttest\t">>),
    view_name_error(<<"\t\ttest">>),
    view_name_error(<<"\ntest">>),
    view_name_error(<<"test\n\n\n">>),
    view_name_error(<<"\n\ntest\n\n\n">>),
    view_name_error(<<"test\r\r">>),
    view_name_error(<<"\rtest">>),
    view_name_error(<<"\r\rtest\r">>),
    view_name_error(<<" \r\ntest">>),
    view_name_error(<<"test\t ">>),
    view_name_error(<<"\n  test\t ">>),

    shutdown_test().


view_name_error(ViewName) ->
    ViewFun = <<"function(doc, meta) { emit([0, 1], 1); }">>,
    validate_ddoc(ViewName, ViewFun).


test_spatial_value_error() ->
    etap:diag("Testing spatial function with wrong value"),
    setup_test(),
    ViewFun = {[]},
    validate_ddoc(<<"valueerror">>, ViewFun),
    shutdown_test().


setup_test() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(),
                                            num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(),
                                            num_set_partitions()),
    ok.


shutdown_test() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(),
                                            num_set_partitions()),
    ok.


create_ddoc(ViewName, ViewFun) ->
    DDoc = {[
             {<<"meta">>, {[{<<"id">>, ddoc_id()}]}},
             {<<"json">>, {[{<<"spatial">>, {[{ViewName, ViewFun}]}}]}}
            ]},
    ok = couch_set_view_test_util:update_ddoc(test_set_name(), DDoc).


validate_ddoc(ViewName, ViewFun) ->
    Result = try
                 create_ddoc(ViewName, ViewFun)
             catch throw:Error ->
                     Error
             end,
    ?etap_match(Result, {invalid_design_doc, _},
                "Design document creation got rejected"),
    {invalid_design_doc, Reason} = Result,
    etap:diag("Design document creation error reason: " ++
                  binary_to_list(Reason)).
