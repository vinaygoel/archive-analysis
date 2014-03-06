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

/* Input: Lines containing the resources for co-citation analysis (source resources)
 * Input: The id.map data that maps a unique integer to each resource
 * Input: The id-sortedint.graph data that maps a set of sorted destination IDs to each source resource
 * Output: Lines containing the set of resources that are linked-to by all the given source resources
 */

%default I_SRC_RESOURCES_DIR '';
%default I_ID_MAP_DIR '';
%default I_ID_SORTEDINT_GRAPH_NO_TS_DIR '';
%default O_COMMON_LINKS_RESOURCES_DIR '';

REGISTER lib/ia-porky-jar-with-dependencies.jar;
DEFINE FindAndIntersectionsUsingPForDeltaDocIdSet org.archive.porky.FindAndIntersectionsUsingPForDeltaDocIdSetUDF();

--Load input
IDMap = LOAD '$I_ID_MAP_DIR' as (id:int, resource:chararray);
Graph = LOAD '$I_ID_SORTEDINT_GRAPH_NO_TS_DIR' as (src:int, dests:{d:(dst:int)});
SrcResources = LOAD '$I_SRC_RESOURCES_DIR' as (resource:chararray);

--Map IDs to Source Resources
Joined = JOIN SrcResources BY resource, IDMap BY resource;
SrcResourceIDs = FOREACH Joined GENERATE IDMap::id as id;
SrcResourceIDs = DISTINCT SrcResourceIDs;

--Find the links/citations for these resource IDs
Joined = JOIN SrcResourceIDs BY id, Graph BY src;
Citations = FOREACH Joined GENERATE Graph::dests as dests;

-- now to find co-citations
-- UDF approach:
-- make use of Kamikaze (http://data.linkedin.com/opensource/kamikaze)
-- to build docIdSets from the sorted integer arrays, and then perform efficient intersection of these sets
-- to find the integers in common to all the sets.
-- In effect, finding all the links common to all the given resources
-- Works only for integer IDs with value < Integer.MAX_VALUE (2^31 -1)
 
CitationsGrp = GROUP Citations ALL;
CoCitedIDs = FOREACH CitationsGrp GENERATE FLATTEN(FindAndIntersectionsUsingPForDeltaDocIdSet(Citations.dests)) as id;

--now to resolve these co-cited IDs to resources
Joined = JOIN CoCitedIDs BY id, IDMap BY id;
CoCitedResources = FOREACH Joined GENERATE IDMap::resource as resource;

STORE CoCitedResources into '$O_COMMON_LINKS_RESOURCES_DIR';

