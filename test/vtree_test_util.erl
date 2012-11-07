-module(vtree_test_util).

-include_lib("../include/vtree.hrl").

-export([generate_kvnodes/1, generate_kpnodes/1, create_file/1,
         get_kvnodes/2]).


-spec generate_kvnodes(Num :: pos_integer()) -> [#kv_node{}].
generate_kvnodes(Num) ->
    [random_kvnode(I) || I <- lists:seq(1, Num)].

-spec random_kvnode(I :: pos_integer()) -> #kv_node{}.
random_kvnode(I) ->
    Max = 1000,
    {A, B, C, D, E, F, G, H} = {
      random:uniform(Max), random:uniform(Max), random:uniform(Max),
      random:uniform(Max), random:uniform(Max), random:uniform(Max),
      random:uniform(Max), random:uniform(Max)},
    Mbb = [
           {erlang:min(A, B), erlang:max(A, B)},
           {erlang:min(C, D), erlang:max(C, D)},
           {erlang:min(E, F), erlang:max(E, F)},
           {erlang:min(G, H), erlang:max(G, H)}],
    LineString = {'LineString', [[A, C, E, G],
                               [(A+B)/2, (C+D)/2, (E+F)/2, (G+H)/2],
                               [B, D, F, H]]},
    Id = list_to_binary("Node" ++ integer_to_list(I)),
    Value = list_to_binary("Value" ++ integer_to_list(I)),
    #kv_node {
               key = Mbb,
               docid = Id,
               geometry = LineString,
               body = Value
             }.


-spec generate_kpnodes(Num :: pos_integer()) -> [#kp_node{}].
generate_kpnodes(Num) ->
    [random_kpnode(I) || I <- lists:seq(1, Num)].

-spec random_kpnode(I :: pos_integer()) -> #kp_node{}.
random_kpnode(I) ->
    Max = 1000,
    {A, B, C, D, E, F, G, H} = {
      random:uniform(Max), random:uniform(Max), random:uniform(Max),
      random:uniform(Max), random:uniform(Max), random:uniform(Max),
      random:uniform(Max), random:uniform(Max)},
    Mbb = [
           {erlang:min(A, B), erlang:max(A, B)},
           {erlang:min(C, D), erlang:max(C, D)},
           {erlang:min(E, F), erlang:max(E, F)},
           {erlang:min(G, H), erlang:max(G, H)}],
    #kp_node {
               key = Mbb,
               childpointer = I,
               treesize = random:uniform(Max),
               reduce = nil,
               mbb_orig = Mbb
             }.


create_file(Filename) ->
    {ok, Fd} = case couch_file:open(Filename, [create, overwrite]) of
    {ok, Fd2} ->
        {ok, Fd2};
    {error, Reason} ->
        io:format("ERROR (~s): Couldn't open file (~s) for tree storage~n",
                  [Reason, Filename])
    end,
    Fd.


% Return a 2-tuple with a list of the depths of the KV-nodes and the
% KV-nodes themselves
get_kvnodes(Fd, RootPos) ->
    Children = vtree_io:read_node(Fd, RootPos),
    get_kvnodes(Fd, Children, 0, {[], []}).
get_kvnodes(_Fd, [], _Depth, Acc) ->
    Acc;
get_kvnodes(_Fd, [#kv_node{}|_]=Children, Depth, {Depths, Nodes}) ->
    {[Depth|Depths], Children ++ Nodes};
get_kvnodes(Fd, [#kp_node{}=Node|Rest], Depth, Acc) ->
    Children = vtree_io:read_node(Fd, Node#kp_node.childpointer),
    % Move down
    Acc2 = get_kvnodes(Fd, Children, Depth+1, Acc),
    % Move sideways
    get_kvnodes(Fd, Rest, Depth, Acc2).
