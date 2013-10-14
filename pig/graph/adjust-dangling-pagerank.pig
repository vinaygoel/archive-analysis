/*
 * Copyright 2013 Internet Archive
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

/* Input: The set of dangling nodes.
 * Input: A web graph without timestamp information with each node and its "final" Pagerank
 * Output: The set of dangling nodes and their respective PageRank scores (propagated from incoming links) 
 */

%default I_PR_ID_GRAPH_DIR '/search/nara/congress112th/pr-iterations/pr-id.graph_8.gz';
%default I_PR_DANGLING_NODES '/search/nara/congress112th/pr-dangling-nodes.gz';
%default O_PR_DANGLING_NODES_SCORES '/search/nara/congress112th/pr-dangling-nodes-adjusted-scores.gz';

previousPagerankGraph = LOAD '$I_PR_ID_GRAPH_DIR' as (id:chararray, pagerank:float, links:{link:(id:chararray)});
danglingPagerankNodes = LOAD '$I_PR_DANGLING_NODES' as (id:chararray);

outboundPagerank = FOREACH previousPagerankGraph GENERATE (pagerank/COUNT(links)) as pagerank, FLATTEN(links) as to_id;
danglingPagerankGrouped = COGROUP outboundPagerank by to_id, danglingPagerankNodes by id INNER;

newDanglingPagerank = FOREACH danglingPagerankGrouped GENERATE group as id, SUM(outboundPagerank.pagerank) as pagerank;

STORE newDanglingPagerank INTO '$O_PR_DANGLING_NODES_SCORES';
