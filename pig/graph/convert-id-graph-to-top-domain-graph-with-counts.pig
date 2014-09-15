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

/* Input: ID-Graph data
 * Input: ID-Map data
 * Output: top domain graph data - srcDomain, dstDomain, number of links
 */

%default I_ID_GRAPH_DIR '';
%default I_ID_MAP_DIR '';
%default O__GRAPH_DIR '';

REGISTER lib/getHostFromSurtUrl.py using jython as HOST;
REGISTER lib/ia-porky-jar-with-dependencies.jar;
DEFINE DOMAIN org.archive.porky.ExtractTopPrivateDomainFromHostNameUDF();

--SET mapreduce.job.queuename default
--SET mapreduce.reduce.memory.mb 8192
--SET default_parallel 100

IDMap = LOAD '$I_ID_MAP_DIR' as (id:chararray, url:chararray);
Graph = LOAD '$I_ID_GRAPH_DIR' as (src:chararray, timestamp:chararray, dests:{dest:(dst:chararray)});

HostIdMap = FOREACH IDMap GENERATE HOST.getHostFromSurtUrl(url) as host, id;
HostIdMap = FILTER HostIdMap BY host is not null and host != '';

--now, top domains
HostIdMap = FOREACH HostIdMap GENERATE DOMAIN(host) as host, id;

Links = FOREACH Graph GENERATE src, timestamp, FLATTEN(dests) as dst;
--skip/filter by timestamp, and then
Links = FOREACH Links GENERATE src, dst;

-- remove self loops
Links = FILTER Links BY src!=dst;
Links = DISTINCT Links;

--Replace srcids with corresponding host
SrcHostLinks = Join HostIdMap BY id, Links BY src;
SrcHostLinks = FOREACH SrcHostLinks GENERATE HostIdMap::host as srcHost, Links::dst as dst;

--Replace dstids with corresponding host
HostLinks = Join HostIdMap BY id, SrcHostLinks BY dst;
HostLinks = FOREACH HostLinks GENERATE SrcHostLinks::srcHost as srcHost, HostIdMap::host as dstHost;

HostLinksGrp = GROUP HostLinks BY (srcHost,dstHost);
HostLinksGrp = FOREACH HostLinksGrp GENERATE FLATTEN(group) as (srcHost,dstHost), COUNT(HostLinks) as count;

STORE HostLinksGrp into '$O_TOP_DOMAIN_GRAPH_DIR';
