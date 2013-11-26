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

/* Input: Original Links (not SURT) with timestamp information
 * Output: top-domain graph data - srcDomain, dstDomain, number of links. Uses public suffix to obtain top private domain. Domain is set to 'other' in the case of invalid domains.
 */

%default I_LINKS_DATA_DIR '';
%default O_TOP_DOMAIN_GRAPH_DIR '/search/nara/congress112th/analysis/top-domain.graph';

REGISTER lib/ia-porky-jar-with-dependencies-CDH4.jar;
DEFINE HOST org.archive.porky.ExtractHostFromOrigUrlUDF();
DEFINE DOMAIN org.archive.porky.ExtractTopPrivateDomainFromHostNameUDF();

Links = LOAD '$I_LINKS_DATA_DIR' as (src:chararray, timestamp:chararray, dst:chararray);

--skip/filter by timestamp, and then

Links = FOREACH Links GENERATE src, dst;

--since, brute approach, commenting out the following
--Links = FILTER Links BY src!=dst;
--Links = DISTINCT Links;

HostLinks = FOREACH Links GENERATE HOST(src) as srcHost, HOST(dst) as dstHost;
HostLinks = FILTER HostLinks BY srcHost is not null and dstHost is not null and srcHost != '' and dstHost != '';
DomainLinks = FOREACH HostLinks GENERATE DOMAIN(srcHost) as srcDomain, DOMAIN(dstHost) as dstDomain;

DomainLinksGrp = GROUP DomainLinks BY (srcDomain,dstDomain);
DomainLinksGrp = FOREACH DomainLinksGrp GENERATE FLATTEN(group) as (srcDomain,dstDomain), COUNT(DomainLinks) as count;

STORE DomainLinksGrp into '$O_TOP_DOMAIN_GRAPH_DIR';
