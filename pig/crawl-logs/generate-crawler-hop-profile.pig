/*
 * Copyright 2014 Internet Archive
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

/* Input: Heritrix generated Crawl Logs
 *
 * Output: Plain text files containing the following tab separated fields for each domain: 
 *
 * hopLengthOfFirstL: Minimum length of the Hop paths ending in 'L' for resources from the given domain
 * domain: the given domain
 * hopLengthOfLastL: Maximum length of the Hop paths ending in 'L' for resources from the given domain
 * maxLInHopPath: Maximum number of 'L' seen in any Hop path for resources from the given domain
 * numIntraDomainLinks: Number of resources crawled where both the crawled resource and the 'via' belong to the given domain
 * numInterDomainLinks: Number of resources crawled where the resource crawled belongs to a different domain to that of the 'via'. The 'via' belongs to the given domain.
 * numIntraDomainLLinks: Number of resources crawled where both the crawled resource and the 'via' belong to the given domain, and the hop path of the crawled resource ends in 'L'
 * numInterDomainLLinks: Number of resources crawled where the resource crawled belongs to a different domain to that of the 'via' and the hop path of the crawled resource ends in 'L' . The 'via' belongs to the given domain.
 * numHosts: Total number of hosts crawled from the given domain
 * numCaptures: Total number of captures from the given domain
 * numLCaptures: Total number of captures from the given domain with hop paths that end in 'L' 
 
 * For this study, we ignore Hop paths that ends in 'P' or 'I'. Also, the output is bucket-ized by the first field ('hopLengthOfFirstL')
 */

%default I_CRAWLLOG_DATA_DIR '';
%default O_HOP_PROFILE_DIR '';

REGISTER lib/ia-porky-jar-with-dependencies.jar;
REGISTER lib/piggybank.jar;
REGISTER lib/getHostFromSurtUrl.py using jython as HOST;

DEFINE SURTURL org.archive.porky.SurtUrlKey();
DEFINE DOMAIN org.archive.porky.ExtractTopPrivateDomainFromHostNameUDF();
SET mapreduce.job.queuename ait

Log = LOAD '$I_CRAWLLOG_DATA_DIR' USING PigStorage() AS (line:chararray);
Log = FOREACH Log GENERATE STRSPLIT(line,'\\s+') as cols;
Log = FOREACH Log GENERATE (chararray)cols.$0 as timestamp, 
                           (chararray)cols.$1 as status,
                           (chararray)cols.$2 as bytes,
                           (chararray)cols.$3 as url,
                           (chararray)cols.$4 as path,
                           (chararray)cols.$5 as via,
                           (chararray)cols.$6 as type,
                           (chararray)cols.$7 as thread,
                           (chararray)cols.$8 as elapsed,
                           (chararray)cols.$9 as digest,
                           (chararray)cols.$10 as source,
                           (chararray)cols.$11 as annotations;

Log = FILTER Log BY not path matches '^.*(P|I)$';
Log = FOREACH Log GENERATE url, (via == '-' ? 'http://internet-archive-wide-crawler.org/' : via) as viaUrl, path;

Log = FOREACH Log GENERATE SURTURL(url) as url, SURTURL(viaUrl) as viaUrl, path;
Log = FOREACH Log GENERATE HOST.getHostFromSurtUrl(url) as host, HOST.getHostFromSurtUrl(viaUrl) as viaHost, path;
Log = FOREACH Log GENERATE host, DOMAIN(host) as domain, viaHost, DOMAIN(viaHost) as viaDomain, path;

--filter out captures of 'other' type domains (IPs, invalid domains, etc.)
Log = FILTER Log BY domain != 'other';

Links = FOREACH Log GENERATE domain, viaDomain;
IntraLinks = FILTER Links BY domain == viaDomain;
InterLinks = FILTER Links BY domain != viaDomain;

IntraLinksGrp = GROUP IntraLinks BY viaDomain;
IntraLinksGrp2 = FOREACH IntraLinksGrp GENERATE group as domainToReport, COUNT(IntraLinks) as numIntraDomainLinks; 

InterLinksGrp = GROUP InterLinks BY viaDomain;
InterLinksGrp2 = FOREACH InterLinksGrp GENERATE group as domainToReport, COUNT(InterLinks) as numInterDomainLinks; 

LinkReport = Join IntraLinksGrp2 BY domainToReport, InterLinksGrp2 BY domainToReport;
LinkReport2 = FOREACH LinkReport GENERATE IntraLinksGrp2::domainToReport as domain, IntraLinksGrp2::numIntraDomainLinks as numIntraDomainLinks, InterLinksGrp2::numInterDomainLinks as numInterDomainLinks;

LLinks = FOREACH Log GENERATE domain, viaDomain, path;
LLinks = FILTER LLinks BY path matches '^.*L$';
IntraLLinks = FILTER LLinks BY domain == viaDomain;
InterLLinks = FILTER LLinks BY domain != viaDomain;

IntraLLinksGrp = GROUP IntraLLinks BY viaDomain;
IntraLLinksGrp2 = FOREACH IntraLLinksGrp GENERATE group as domainToReport, COUNT(IntraLLinks) as numIntraDomainLLinks; 

InterLLinksGrp = GROUP InterLLinks BY viaDomain;
InterLLinksGrp2 = FOREACH InterLLinksGrp GENERATE group as domainToReport, COUNT(InterLLinks) as numInterDomainLLinks; 

LLinkReport = Join IntraLLinksGrp2 BY domainToReport, InterLLinksGrp2 BY domainToReport;
LLinkReport2 = FOREACH LLinkReport GENERATE IntraLLinksGrp2::domainToReport as domain, IntraLLinksGrp2::numIntraDomainLLinks as numIntraDomainLLinks, InterLLinksGrp2::numInterDomainLLinks as numInterDomainLLinks;

DomainHosts = FOREACH Log GENERATE host, domain;
DomainHosts = DISTINCT DomainHosts;

DomainHosts1 = GROUP DomainHosts BY domain;
DomainHosts2 = FOREACH DomainHosts1 GENERATE group as domain, COUNT(DomainHosts) as numHosts;

DomainCaptures = FOREACH Log GENERATE domain;
DomainCaptures1 = GROUP DomainCaptures BY domain;
DomainCaptures2 = FOREACH DomainCaptures1 GENERATE group as domain, COUNT(DomainCaptures) as numCaptures;

LCaptures = FOREACH Log GENERATE domain, path;
LCaptures = FILTER LCaptures BY path matches '^.*L$';

LCaptures1 = GROUP LCaptures BY domain;
LCaptures2 = FOREACH LCaptures1 GENERATE group as domain, COUNT(LCaptures) as numLCaptures;

CaptureReport0 = Join DomainHosts2 BY domain, LCaptures2 BY domain;
CaptureReport0 = FOREACH CaptureReport0 GENERATE DomainHosts2::domain as domain, DomainHosts2::numHosts as numHosts, LCaptures2::numLCaptures as numLCaptures;

CaptureReport1 = Join CaptureReport0 BY domain, DomainCaptures2 BY domain;
CaptureReport2 = FOREACH CaptureReport1 GENERATE CaptureReport0::domain as domain, CaptureReport0::numHosts as numHosts, CaptureReport0::numLCaptures as numLCaptures, DomainCaptures2::numCaptures as numCaptures;

Paths = FOREACH LCaptures GENERATE domain, path, REPLACE(path,'[^L]','') as LString;
Paths = FOREACH Paths GENERATE domain, (int)SIZE(path) as hopLength, (int)SIZE(LString) as LStringLength;
 
DomainHop1 = GROUP Paths BY domain;
DomainHop2 = FOREACH DomainHop1 {
                        MinHops = ORDER Paths BY hopLength ASC;
			MaxHops = ORDER Paths BY hopLength DESC;
			MaxLInPath = ORDER Paths BY LStringLength DESC;
                        MinHops = LIMIT MinHops 1;
                        MaxHops = LIMIT MaxHops 1;
                        MaxLInPath = LIMIT MaxLInPath 1;
                        GENERATE group as domain, FLATTEN(MinHops.hopLength) as hopLengthOfFirstL, FLATTEN(MaxHops.hopLength) as hopLengthOfLastL, FLATTEN(MaxLInPath.LStringLength) as maxLInPath;
                };


Report = Join DomainHop2 BY domain, LinkReport2 BY domain;
Report = FOREACH Report GENERATE DomainHop2::hopLengthOfFirstL as hopLengthOfFirstL, DomainHop2::domain as domain, DomainHop2::hopLengthOfLastL as hopLengthOfLastL, DomainHop2::maxLInPath as maxLInPath, LinkReport2::numIntraDomainLinks as numIntraDomainLinks, LinkReport2::numInterDomainLinks as numInterDomainLinks;

Report1 = Join Report BY domain, LLinkReport2 BY domain;
Report1 = FOREACH Report1 GENERATE Report::hopLengthOfFirstL as hopLengthOfFirstL, Report::domain as domain, Report::hopLengthOfLastL as hopLengthOfLastL, Report::maxLInPath as maxLInPath, Report::numIntraDomainLinks as numIntraDomainLinks, Report::numInterDomainLinks as numInterDomainLinks, LLinkReport2::numIntraDomainLLinks as numIntraDomainLLinks, LLinkReport2::numInterDomainLLinks as numInterDomainLLinks;

Report2 = Join CaptureReport2 BY domain, Report1 BY domain;
Report2 = FOREACH Report2 GENERATE Report1::hopLengthOfFirstL as hopLengthOfFirstL, Report1::domain as domain, Report1::hopLengthOfLastL as hopLengthOfLastL, Report1::maxLInPath as maxLInPath, Report1::numIntraDomainLinks as numIntraDomainLinks, Report1::numInterDomainLinks as numInterDomainLinks, Report1::numIntraDomainLLinks as numIntraDomainLLinks, Report1::numInterDomainLLinks as numInterDomainLLinks, CaptureReport2::numHosts as numHosts, CaptureReport2::numCaptures as numCaptures, CaptureReport2::numLCaptures as numLCaptures;

Report2 = ORDER Report2 BY numHosts DESC;

Store Report2 into '$O_HOP_PROFILE_DIR' using org.apache.pig.piggybank.storage.MultiStorage('$O_HOP_PROFILE_DIR/','0');
