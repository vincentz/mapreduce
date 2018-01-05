# mapreduce

A MapReduce program to compute the distribution of a graph's node degree differences.

Each file stores a list of edges as tab-separated-values. Each line represents a single edge consisting of two columns: (Source, Target), each of which is separated by a tab. Node IDs are positive integers and the rows are already sorted by Source.

The code accept two arguments upon running. The first argument (args[0]) will be a path for the input graph file, and the second argument (args[1]) will be a path for output directory. The default output mechanism of Hadoop will create multiple files on the output directory such as part-00000, part-00001.
Output is of the formatdiff countwhere(1)    diff    is   the   difference   between   a   node’s   out-degree   and   in-degree   (out-degree   -   in-degree);   and (2)    count    is   the   number   of   nodes   that   have   the   value   of    difference    (specified   in   1).The out-degree of a node is the number of edges where that node is the Source. The in-degree of a node is the number of edges where that node is the Target.  diff and  count are separated by a tab (\t).