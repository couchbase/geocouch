% The counterpart to #spatial_query_args in the mapreduces views is
% #view_query_args
-record(spatial_query_args, {
    %bbox=nil,
    stale=update_after :: update_after | ok | false,
    %count=false,
    % Bounds of the cartesian plane
    %bounds=nil,
    limit = 10000000000, % Huge number to simplify logic
    skip = 0,
    include_docs = false :: boolean(),
    % a multidimensional bounding box
    range = [] :: [{number()|nil, number()|nil}],

    debug = false,
    % Whether to filter the passive/cleanup partitions out
    filter = true,
    % Whether to query the main or the replica index
    type = main,
    view_name = nil,

    % NOTE vmx 2013-07-09: Not sure if that's realle needed
    extra = nil
}).
