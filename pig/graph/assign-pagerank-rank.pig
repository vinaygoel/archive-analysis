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

/* Input: The set of dangling nodes with their adjusted scores
 * Input: A web graph without timestamp information with each node and its "final" Pagerank
 * Output: The set of all nodes with their PageRank rank and their Pagerank score 
 */

%default I_PR_ID_GRAPH_DIR '/search/nara/congress112th/pr-iterations/pr-id.graph_8.gz';
%default I_PR_DANGLING_NODES_SCORES '/search/nara/congress112th/pr-dangling-nodes-adjusted-scores.gz';
%default O_PR_RANK_ALL_NODES '/search/nara/congress112th/pr-rank-nodeid-score-all-nodes.gz';

pagerankFromGraph = LOAD '$I_PR_ID_GRAPH_DIR' as (id:chararray, pagerank:float);
pagerankFromDangling = LOAD '$I_PR_DANGLING_NODES_SCORES' as (id:chararray, pagerank:float);

pagerankForAllNodes = UNION pagerankFromGraph, pagerankFromDangling;

prRanks = RANK pagerankForAllNodes by pagerank DESC;

STORE prRanks INTO '$O_PR_RANK_ALL_NODES';
