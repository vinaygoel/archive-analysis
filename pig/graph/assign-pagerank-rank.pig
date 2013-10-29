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

/* Input: The complete set of nodes with their PR scores
 * Output: The set of all nodes with their PageRank rank and their Pagerank score 
 */

%default I_ID_PRSCORE_DIR '/search/nara/congress112th/analysis/id.prscore/';
%default O_ID_PRRANK_DIR '/search/nara/congress112th/analysis/id.prrank/';

pagerankFromGraph = LOAD '$I_ID_PRSCORE_DIR' as (id:chararray, pagerank:double);
prRanks = RANK pagerankFromGraph by pagerank DESC;
-- id, rank, score
prRanks = FOREACH prRanks GENERATE $1, $0, $2; 
STORE prRanks INTO '$O_ID_PRRANK_DIR';
