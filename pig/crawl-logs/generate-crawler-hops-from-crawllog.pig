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
 * Output: A mapping of the crawled URLs to unique integers IDs (crawl.id.map)
 * Output: A hop path file (src, -, set of destinations with the hop path type)
 */

%default I_CRAWLLOG_DATA_DIR '/user/vinay/input/test.crawl.log';
%default O_CRAWL_ID_MAP_DIR '/user/vinay/input/crawl.id.map.gz';
%default O_CRAWL_ID_HOPPATH_DIR '/user/vinay/input/crawl.id.hoppath.gz';

REGISTER lib/collectBagElements.py using jython as COLLECTBAGELEMENTS;

log = LOAD '$I_CRAWLLOG_DATA_DIR' USING PigStorage() AS (line:chararray);

log = FOREACH log GENERATE STRSPLIT(line,'\\s+') as cols;
log = FOREACH log GENERATE (chararray)cols.$0 as timestamp, 
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

log = FOREACH log GENERATE (via == '-' ? '!CRAWLER!' : via) as src, url as dst, (SUBSTRING(path,((int)SIZE(path)-1),((int)SIZE(path)))) as hop;
log = DISTINCT log;

--crawl.id.map
sources = FOREACH log GENERATE src as url;
destinations = FOREACH log GENERATE dst as url;
allResources = UNION sources, destinations;
allResources = DISTINCT allResources;

crawlIdMap = RANK allResources BY url;
crawlIdMap = FOREACH crawlIdMap GENERATE $0 as id, $1 as url;

-- join with log data
srcIdResolvedLog = JOIN crawlIdMap BY url, log BY src;
srcIdResolvedLog = FOREACH srcIdResolvedLog GENERATE crawlIdMap::id as srcId, log::dst as dst, log::hop as hop;

idLog = JOIN crawlIdMap BY url, srcIdResolvedLog BY dst;
idLog = FOREACH idLog GENERATE srcIdResolvedLog::srcId, crawlIdMap::id as dstId:chararray, srcIdResolvedLog::hop as hop;

idLog = FOREACH idLog GENERATE srcId, ((chararray)CONCAT(dstId,':')) as dstIdPrefix, hop;
idLog = FOREACH idLog GENERATE srcId, ((chararray)CONCAT(dstIdPrefix,hop)) as dstData;

idLogBySrc = GROUP idLog BY srcId;
idLogBySrc = FOREACH idLogBySrc {
		GENERATE group, '-' as misc, COLLECTBAGELEMENTS.collectBagElements(idLog.dstData); 
	};

STORE crawlIdMap into '$O_CRAWL_ID_MAP_DIR';
STORE idLogBySrc into '$O_CRAWL_ID_HOPPATH_DIR';
