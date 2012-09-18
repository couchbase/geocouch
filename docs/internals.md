Vtree internals
===============


TODO/Think about
----------------


### Representation of nodes

Perhaps it would make sense to change the way the #kv_node and #kp_node records work. It might make sense to change to a 2-tuple with the MBB as key and the #kv_node{}/#kp_node{} as a value. This way they could be easily be passed on over the cut to the vtree_choose and vtree_split module.

On the other hand it's a single line to convert the nodes to be used on the other side of the cut:

    SplitNodes = [{Node#kv_node.key, Node} || Node <- Nodes]

And to convert back:

    {_, Nodes} = lists:unzip(SplitNodes)

At the moment I (vmx) think the current format works well, I'll keep it.


Cuts
----

There are several cuts in the internal code of the Vtree.


### Storing on disk

One cut is before the data is actually stored on disk (calls to the vtree_io module). Before the cut the nodes are splitted into the appropriate size. After the cut the nodes are encoded into some binary format and stored on disk. The #vtree{} record doesn't cross the cut.


### Split and choose algorthims

The split and choose algoithms shouldn't know about the internal things of the vtree. They are just operating on a key-value tuple, where the MBB is the key and the value doesn't really matter.

This means that there's a cut before calling to the vtree_choose and vtree_split module. Before the cut we have #kv_node{} and #kp_node{} record, but they don't cross the cut. They need to be converted into a 2-tuple with the MBB as the key.
