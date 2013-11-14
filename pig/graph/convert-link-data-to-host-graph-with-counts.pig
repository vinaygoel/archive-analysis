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

/* Input: Links with timestamp information
 * Output: host graph data - srcHost, dstHost, number of links
 */

%default I_LINKS_DATA_DIR '/search/nara/congress112th/analysis/links-from-*/';
%default O_HOST_GRAPH_DIR '/search/nara/congress112th/analysis/host.graph';

REGISTER lib/getHostFromSurtUrl.py using jython as HOST;

Links = LOAD '$I_LINKS_DATA_DIR' as (src:chararray, timestamp:chararray, dst:chararray);

--skip/filter by timestamp, and then

Links = FOREACH Links GENERATE src, dst;

-- remove self loops
Links = FILTER Links BY src!=dst;
Links = DISTINCT Links;

HostLinks = FOREACH Links GENERATE HOST.getHostFromSurtUrl(src) as srcHost, HOST.getHostFromSurtUrl(dst) as dstHost;
HostLinksGrp = GROUP HostLinks BY (srcHost,dstHost);
HostLinksGrp = FOREACH HostLinksGrp GENERATE FLATTEN(group) as (srcHost,dstHost), COUNT(HostLinks) as count;

STORE HostLinksGrp into '$O_HOST_GRAPH_DIR';
