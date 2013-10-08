#
# Copyright 2013 Internet Archive
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License. You
# may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.
#

# Input: The set of dangling nodes (nodes with no outlinks), the number of dangling nodes,
# the total number of nodes in the graph,
# the dangling factor which is used in distributing the rank from the dangling nodes equally amongst every other node in the graph (at iteration i)
# Input: A web graph without timestamp information with each node and its Pagerank value (at iteration i)
# Output: A web graph without timestamp information with each node and its Pagerank (at iteration i+1)

#!/usr/bin/python
from org.apache.pig.scripting import *

P = Pig.compile("""

previousPagerankGraph = LOAD '$I_PR_ID_GRAPH_DIR' as (id:chararray, pagerank:float, links:{link:(id:chararray)});
danglingPagerankNodes = LOAD '$I_PR_DANGLING_NODES' as (id:chararray, pagerank:float);
previousDanglingFactor = LOAD '$I_PR_DANGLING_NODES_FACTOR' as (pagerank:float);
numGraphNodes = LOAD '$I_PR_GRAPH_NODES_COUNT' as (nodecount:long);
numDanglingNodes = LOAD '$I_PR_DANGLING_NODES_COUNT' as (nodecount:long);

outboundPagerank0 = FOREACH previousPagerankGraph GENERATE (pagerank/COUNT(links)) as pagerank, FLATTEN(links) as to_id;

joinedWithDangling = join outboundPagerank0 by to_id left, danglingPagerankNodes by id;
danglingRecords = FILTER joinedWithDangling by $2 is not null;
nondanglingRecords = FILTER joinedWithDangling by $2 is null;

danglingScoresNew = FOREACH danglingRecords GENERATE $0 as pagerank;
danglingScores = union danglingScoresNew, previousDanglingFactor;
danglingFactor = FOREACH (GROUP danglingScores ALL) GENERATE (SUM($1)/(numGraphNodes.$0));

outboundPagerank = FOREACH nondanglingRecords GENERATE $0 as pagerank, $1 as to_id;

pagerankGrouped = COGROUP outboundPagerank by to_id, previousPagerankGraph by id INNER;

newPagerank = FOREACH pagerankGrouped {
		GENERATE group as id, 
        	((1 - $d)) + $d * (SUM (outboundPagerank.pagerank) + danglingFactor.$0) as pagerank, 
		FLATTEN (previousPagerankGraph.links) as links,
                FLATTEN (previousPagerankGraph.pagerank) as previous_pagerank;
		};

-- to account for disconnected nodes / nodes with no incoming links
newPagerank = FOREACH newPagerank GENERATE id, (pagerank is null? ((1-$d) + $d * danglingFactor.$0):pagerank) as pagerank, links, previous_pagerank;

newDanglingFactor = FOREACH danglingFactor GENERATE ((1 - $d)) + $d * (danglingFactor.$0) as pagerank;
nextDanglingFactor = FOREACH newDanglingFactor GENERATE (numDanglingNodes.$0) * (newDanglingFactor.$0);
 
pagerankDiff = FOREACH newPagerank GENERATE ABS(previous_pagerank - pagerank);
maxDiff = FOREACH (GROUP pagerankDiff ALL) GENERATE MAX(pagerankDiff);

STORE newPagerank INTO '$O_PR_ID_GRAPH_DIR';
STORE nextDanglingFactor INTO '$O_PR_DANGLING_NODES_FACTOR';
STORE maxDiff INTO '$O_MAX_DIFF';

""")

I_PR_ID_GRAPH_DIR = "/search/nara/congress112th/pr-id.graph.gz"
I_PR_DANGLING_NODES = "/search/nara/congress112th/pr-dangling-nodes.gz"
I_PR_GRAPH_NODES_COUNT = "/search/nara/congress112th/pr-graph-nodes-count"
I_PR_DANGLING_NODES_COUNT = "/search/nara/congress112th/pr-dangling-nodes-count"
I_PR_DANGLING_NODES_FACTOR = "/search/nara/congress112th/pr-dangling-nodes-factor"
I_ITERATIONS_DIR = "/search/nara/congress112th/pr-iterations"

d = 0.85
MAXDIFF = 0.01
MAXITERATIONS = 10
Pig.fs("mkdir " + I_ITERATIONS_DIR)

for i in range(MAXITERATIONS):
        O_PR_ID_GRAPH_DIR = I_ITERATIONS_DIR + "/pr-id.graph_" + str(i + 1) + ".gz"
        O_PR_DANGLING_NODES_FACTOR = I_ITERATIONS_DIR + "/pr-dangling-nodes-factor_" + str(i + 1)
        O_MAX_DIFF = I_ITERATIONS_DIR + "/pr-max-diff_" + str(i + 1)
        Pig.fs("rmr " + O_PR_ID_GRAPH_DIR)
        Pig.fs("rmr " + O_PR_DANGLING_NODES_FACTOR)
        Pig.fs("rmr " + O_MAX_DIFF)
        stats = P.bind().runSingle()
        if not stats.isSuccessful():
                raise 'Pagerank Algorithm Failed!'
        maxDiffValue = float(str(stats.result("maxDiff").iterator().next().get(0)))
        print "Max Difference Value = " + str(maxDiffValue)
        if maxDiffValue < MAXDIFF:
                print "Pagerank computation completed at Iteration: " + str(i)
                break
        I_PR_ID_GRAPH_DIR = O_PR_ID_GRAPH_DIR
	I_PR_DANGLING_NODES_FACTOR = O_PR_DANGLING_NODES_FACTOR

