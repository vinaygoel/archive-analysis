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

/* Input: A host/domain graph - srcHost, dstHost, number of links
 * Output: Number of outlinks and inlinks per host, and number of outhosts and inhosts per host
 */

%default I_HOST_GRAPH_DIR '/search/nara/congress112th/id.graph.gz';
%default O_HOST_DEGREE_STATS_DIR '/search/nara/congress112th/degree-analysis.gz';

Graph = LOAD '$I_HOST_GRAPH_DIR' as (srcHost:chararray, dstHost:chararray, numlinks:long);

Hosts = FOREACH Graph GENERATE srcHost, dstHost;
Hosts = DISTINCT Hosts;

OutHosts = GROUP Hosts BY srcHost;
OutHostsStats = FOREACH OutHosts GENERATE group as host, COUNT(Hosts) as outhosts;

InHosts = GROUP Hosts BY dstHost;
InHostsStats = FOREACH InHosts GENERATE group as host, COUNT(Hosts) as inhosts;

OutLinks = FOREACH Graph GENERATE srcHost, numlinks;
OutLinksGrp = GROUP OutLinks BY srcHost;
OutLinksStats = FOREACH OutLinksGrp GENERATE group as host, SUM(OutLinks.numlinks) as outlinks;

InLinks = FOREACH Graph GENERATE dstHost, numlinks;
InLinksGrp = GROUP InLinks BY dstHost;
InLinksStats = FOREACH InLinksGrp GENERATE group as host, SUM(InLinks.numlinks) as inlinks;

STORE OutLinksStats into '$O_HOST_DEGREE_STATS_DIR/host-numoutlinks.gz/';
STORE InLinksStats into '$O_HOST_DEGREE_STATS_DIR/host-numinlinks.gz/';

STORE OutHostsStats into '$O_HOST_DEGREE_STATS_DIR/host-numouthosts.gz/';
STORE InHostsStats into '$O_HOST_DEGREE_STATS_DIR/host-numinhosts.gz/';
