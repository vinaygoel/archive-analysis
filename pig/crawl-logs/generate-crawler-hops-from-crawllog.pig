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

/* Input: Heritrix generated Crawl Logs
 * Output: A mapping of the crawled URLs to unique integers IDs (crawllogid.map)
 * Output: A hop path file (src, -, set of destinations with the hop path type)
 * Output: Links dataset (source, timestamp, destination) - the links followed by the crawler
 */

%default I_CRAWLLOG_DATA_DIR '/user/adam/NARA-112TH-CONGRESS-2012.aggregate.crawl.log';
%default O_CRAWLLOG_ID_MAP_DIR '/search/nara/congress112th/analysis/crawllogid.map';
%default O_CRAWLLOG_ID_ONEHOP_DIR '/search/nara/congress112th/analysis/crawllogid.onehop';
%default O_CRAWLLOG_LINKS_DATA_DIR '/search/nara/congress112th/analysis/links-from-crawllog.gz';

--CDH4
--REGISTER lib/ia-web-commons-jar-with-dependencies-CDH4.jar;

--CDH3
REGISTER lib/ia-web-commons-jar-with-dependencies-CDH3.jar;

REGISTER lib/pigtools.jar;

DEFINE SURTURL pigtools.SurtUrlKey();

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

-- Store links data (can be combined with links from WAT files)
Links = FILTER Log BY via != '-';
Links = FOREACH Links GENERATE SURTURL(via) as src, ToDate(timestamp) as timestamp, SURTURL(url) as dst;
Links = FILTER Links by src is not null and dst is not null;
Links = FILTER Links by src!=dst;
Links = DISTINCT Links;

Log = FOREACH Log GENERATE (via == '-' ? '!CRAWLER!' : via) as src, url as dst, (SUBSTRING(path,((int)SIZE(path)-1),((int)SIZE(path)))) as hop;
Log = DISTINCT Log;

--crawllogid.map
sources = FOREACH Log GENERATE src as url;
destinations = FOREACH Log GENERATE dst as url;
allResources = UNION sources, destinations;
allResources = DISTINCT allResources;

crawlIdMap = RANK allResources BY url;
crawlIdMap = FOREACH crawlIdMap GENERATE $0 as id, $1 as url;

-- join with log data
srcIdResolvedLog = JOIN crawlIdMap BY url, Log BY src;
srcIdResolvedLog = FOREACH srcIdResolvedLog GENERATE crawlIdMap::id as srcId, Log::dst as dst, Log::hop as hop;

idLog = JOIN crawlIdMap BY url, srcIdResolvedLog BY dst;
idLog = FOREACH idLog GENERATE srcIdResolvedLog::srcId, crawlIdMap::id as dstId:chararray, srcIdResolvedLog::hop as hop;

idLog = FOREACH idLog GENERATE srcId, ((chararray)CONCAT(dstId,':')) as dstIdPrefix, hop;
idLog = FOREACH idLog GENERATE srcId, ((chararray)CONCAT(dstIdPrefix,hop)) as dstData;

idLogBySrc = GROUP idLog BY srcId;
idLogBySrc = FOREACH idLogBySrc {
		GENERATE group, '-' as misc, BagToString(idLog.dstData,'\t');  
	};

STORE crawlIdMap into '$O_CRAWLLOG_ID_MAP_DIR';
STORE idLogBySrc into '$O_CRAWLLOG_ID_ONEHOP_DIR'; 
STORE Links into '$O_CRAWLLOG_LINKS_DATA_DIR';
