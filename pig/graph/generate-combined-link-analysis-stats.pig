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

%default I_ID_MAP_DIR '/search/nara/congress112th/analysis/id.map/';
%default I_ID_OUTDEGREE_DIR '/search/nara/congress112th/analysis/id.outdegree/';
%default I_ID_INDEGREE_DIR '/search/nara/congress112th/analysis/id.indegree/';
%default I_ID_PRRANK_DIR '/search/nara/congress112th/analysis/id.prrank/';
%default O_URL_OUTDEGREE_INDEGREE_PRRANK_DIR '/search/nara/congress112th/analysis/url.outdegree-indegree-prrank';

%default PRRANK_FOR_UNKNOWN_PAGE '1000000000';

idMap = LOAD '$I_ID_MAP_DIR' AS (id:chararray, url:chararray);
outDegrees = LOAD '$I_ID_OUTDEGREE_DIR' as (id:chararray, outDegree:long);
inDegrees = LOAD '$I_ID_INDEGREE_DIR' as (id:chararray, inDegree:long);
prRanks = LOAD '$I_ID_PRRANK_DIR' as (id:chararray, prRank:long);

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
outInPrData = FOREACH outInPrData GENERATE outInData::id as id, outInData::outDegree as outDegree, outInData::inDegree as inDegree, (prRanks::prRank is null?$PRRANK_FOR_UNKNOWN_PAGE:prRanks::prRank) as prRank;
--PR Rank set to a very high value when unknown

result = JOIN idMap BY id, outInPrData by id;
result = FOREACH result GENERATE idMap::url as url, outInPrData::outDegree as outDegree, outInPrData::inDegree as inDegree, outInPrData::prRank as prRank;

STORE result INTO '$O_URL_OUTDEGREE_INDEGREE_PRRANK_DIR';
