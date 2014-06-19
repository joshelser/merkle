merkle
======

A Merkle tree (http://en.wikipedia.org/wiki/Merkle_tree) is a hash tree and can be used to evaluate equality over large
files with the ability to ascertain what portions of the files differ. Each leaf of the Merkle tree is some hash of a
portion of the file, with each leaf corresponding to some "range" within the source file. As such, if all leaves are
considered as ranges of the source file, the "sum" of all leaves creates a contiguous range over the entire file.

The parent of any nodes (typically, a binary tree; however this is not required) is the concatenation of the hashes of
the children. We can construct a full tree by walking up the tree, creating parents from children, until we have a root
node. To check equality of two files that each have a merkle tree built, we can very easily compare the value of at the
root of the Merkle tree to know whether or not the files are the same.

Additionally, in the situation where we have two files with we expect to be the same but are not, we can walk back down
the tree, finding subtrees that are equal and subtrees that are not. Subtrees that are equal correspond to portions of
the files which are identical, where subtrees that are not equal correspond to discrepancies between the two files.

We can apply this concept to Accumulo, treating a table as a file, and ranges within a file as an Accumulo Range. We can
then compute the hashes over each of these Ranges and compute the entire Merkle tree to determine if two tables are
equivalent.

## Table Splits

While we can use any collection of contiguous Accumulo Ranges as the definition of the discrete pieces of an Accumulo
table, using the existing split points on the Accumulo table have some nice properties that are desirable. When we use
the table split points, we are, in essence calculating the hashes of each tablet. Thus, the leaves in our Merkle tree
are the tablets that make up our Accumulo table.

The performance gain is that we can push down the calculation of the hash over that Tablet to the Accumulo
TabletServers. For the same reasons that MapReduce is such a useful paradigm for computation over large datasets, we can
calculate the hashes for each table within the TabletServer itself by writing a custom Accumulo SortedKeyValueIterator.

By doing this, we can compute the hashes with orders of magnitude less overhead, due to of transferring the data to some
client, and avoid the pains of writing some distributed application to do this computation for us.

It is extremely important to note that the split points used *will* affect the Merkle tree that is generate. As such, it
is expected that two identical tables with non-identical table split points used to generate the Merkle tree will result
in two non-equal Merkle trees.

## Splits file

A file of split points to use when generating the Merkle tree is also possible by using the `-s` or `--splits` option.
This expects a file which is one split point per line and acts like the splits file which can be provided to Accumulo to
add splits to a table.

Using a file of split points is much less efficient that using the table split points as iterator pushdown cannot be
used. It is highly recommended to use the table split points when comparing two tables.

## Execution

To create a Merkle tree over an Accumulo table, we need to execute two steps.

### Generate the leaves

As mentioned, the leaves of our Merkle tree are best suited to be the Tablets that make up our Accumulo table. As such,
   we need to compute the hashes for each tablet and write them out to an Accumulo table.

```
accumulo org.apache.accumulo.test.merkle.cli.GenerateHashes -t source_table -o hashes -nt 8 -i merkle -u root -p password -z localhost -hash MD5 -iter
```

This command will execute the class that will create hashes over the table `source_table`, output them to the table
`hashes`, use 8 Scanners concurrently to fetch this data, use the MD5 algorithm to compute the Hash (any MessageDigest
algorithm is accepted). The `-iter` option will pushdown the computation of the hashes to the TabletServers
which can be very useful when the Accumulo instance has multiple nodes that can compute this data in parallel. The other
options are the typical required connection information: Accumulo username and password, ZooKeeper connection string and
Accumulo instance name.

### Computing the root Merkle tree node

Next, we use the hashes that we generated to compute the root of the Merkle tree.

```
accumulo org.apache.accumulo.test.merkle.cli.ComputeRootHash -t hashes -i merkle -u root -p password -z localhost -hash MD5
```

We provide the table that we want to read our hashes from (`hashes`) and the program will output the root of the Merkle
tree to STDOUT. This can then be used to determine if two tables are equal.

### Automatically check equality for multiple tables in the same Accumulo instance

A class is also exposed that takes a list of table and computes the Merkle trees for each of those tables. The output of
this program is a multiple space separated lines with two columns: the first column is the table name and the second is
the root Merkle tree node. This is a simple way to determine equality over two tables within the same instance in one
command.

```
accumulo org.apache.accumulo.test.merkle.cli.CompareTables --tables table1 table2 -i merkle -u root -p password -z localhost -hash MD5 -nt 8 -iter
```

The options here are very similar to the other commands, except the `--tables` option now accepts multiple Accumulo
tables.
