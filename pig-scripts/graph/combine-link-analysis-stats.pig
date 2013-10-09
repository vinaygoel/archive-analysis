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

/* Input: Indegree counts, Outdegree counts and the PageRank ranks for each doc (id)
 * Input: The id.map data 
 * Output: Lines containing Outdegree, Indegree and Pagerank ranks for each doc (url)
 */

%default I_ID_MAP_DIR '/search/nara/congress112th/id.map.gz/';
%default I_OUTDEGREE_DIR '/search/nara/congress112th/degree-analysis.gz/id-numoutlinks.gz/';
%default I_INDEGREE_DIR '/search/nara/congress112th/degree-analysis.gz/id-numinlinks.gz/';
%default I_PR_RANK_ALL_NODES '/search/nara/congress112th/pr-rank-nodeid-score-all-nodes.gz';
%default O_OUTDEGREE_INDEGREE_PR_RANK '/search/nara/congress112th/canonurl-outDegree-inDegree-prRank.gz/';

idMap = LOAD '$I_ID_MAP_DIR' AS (id:chararray, url:chararray);
outDegrees = LOAD '$I_OUTDEGREE_DIR' as (id:chararray, outDegree:long);
inDegrees = LOAD '$I_INDEGREE_DIR' as (id:chararray, inDegree:long);

--note the change in order of fields
prRanks = LOAD '$I_PR_RANK_ALL_NODES' as (prRank:long, id:chararray);

outIds = FOREACH outDegrees GENERATE id;
inIds = FOREACH inDegrees GENERATE id;
prIds = FOREACH prRanks GENERATE id;

allIds = UNION outIds, inIds;
allIds = UNION allIds, prIds;
allIds = DISTINCT allIds;

outData = JOIN allIds by id left, outDegrees by id;
outData = FOREACH outData GENERATE allIds::id as id, (outDegrees::outDegree is null?0:outDegrees::outDegree) as outDegree;

outInData = JOIN outData by id left, inDegrees by id;
outInData = FOREACH outInData GENERATE outData::id as id, outData::outDegree as outDegree, (inDegrees::inDegree is null?0:inDegrees::inDegree) as inDegree; 

outInPrData = JOIN outInData by id left, prRanks by id;
outInPrData = FOREACH outInPrData GENERATE outInData::id as id, outInData::outDegree as outDegree, outInData::inDegree as inDegree, (prRanks::prRank is null?0:prRanks::prRank) as prRank;

result = JOIN idMap BY id, outInPrData by id;
result = FOREACH result GENERATE idMap::url, outInPrData::outDegree, outInPrData::inDegree, outInPrData::prRank;

STORE result INTO '$O_OUTDEGREE_INDEGREE_PR_RANK';
