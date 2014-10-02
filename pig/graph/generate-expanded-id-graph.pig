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

/* Input: ID-Map, ID-Graph, URL_TS_Checksum dataset (see pig/cdx/generate-urltschecksum-dataset-nonredirects.pig)
 * Output: Expanded ID-Graph (expands the ID graph to resolve revisits by comparing URL-checksums)
 */

%default I_ID_MAP_DIR '/dataset-derived/gov/link-analysis/id.map/';
%default I_ID_GRAPH_DIR '/dataset-derived/gov/link-analysis/id.graph/';
%default I_URL_TS_CHECKSUM_DIR '/dataset-derived/gov/link-analysis/url-ts-checksum';
%default O_EXPANDED_ID_GRAPH_DIR '/dataset-derived/gov/link-analysis/expanded.id.graph';

--SET mapreduce.job.queuename default
--SET mapreduce.reduce.memory.mb 8192
--SET default_parallel 100

IdMap = LOAD '$I_ID_MAP_DIR' AS (id:chararray, url:chararray);
IdGraph = LOAD '$I_ID_GRAPH_DIR' AS (id:chararray, ts:chararray, links:chararray);
UrlTsChecksum = LOAD '$I_URL_TS_CHECKSUM_DIR' AS (url:chararray, ts:chararray, checksum:chararray);

Joined = Join IdMap BY url, UrlTsChecksum BY url;
IdTsChecksum = FOREACH Joined GENERATE IdMap::id as id, UrlTsChecksum::ts as ts, UrlTsChecksum::checksum as checksum;

Joined = Join IdGraph BY (id,ts), IdTsChecksum BY (id,ts);
IdChecksumLinks = FOREACH Joined GENERATE IdTsChecksum::id as id, IdTsChecksum::checksum as checksum, IdGraph::links as links;
IdChecksumLinks = DISTINCT IdChecksumLinks;

Joined = Join IdTsChecksum BY (id,checksum), IdChecksumLinks BY (id,checksum);
ExpandedIdGraph = FOREACH Joined GENERATE IdTsChecksum::id as id, IdTsChecksum::ts as ts, IdChecksumLinks::links as links;
ExpandedIdGraph = DISTINCT ExpandedIdGraph;

STORE ExpandedIdGraph into '$O_EXPANDED_ID_GRAPH_DIR';

