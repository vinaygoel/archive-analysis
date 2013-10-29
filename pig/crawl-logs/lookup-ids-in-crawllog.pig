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

/* Input: The set of IDs to look up in the Heritrix generated Crawl Logs
 * Input: Heritrix generated Crawl Logs
 * Input: A mapping of the crawled URLs to unique integers IDs (crawlid.map)
 * Output: Crawl Log Lines where the URLs/Vias have the given IDs
 */

%default I_CRAWLLOG_IDS_TO_LOOKUP_DIR '/search/nara/congress112th/giraph/ids-to-find';
%default I_CRAWLLOG_DATA_DIR '/user/adam/NARA-112TH-CONGRESS-2012.aggregate.crawl.log';
%default I_CRAWLLOG_ID_MAP_DIR '/search/nara/congress112th/giraph/crawllogid.map';
%default O_MATCHED_CRAWLLOG_DATA_DIR '/search/nara/congress112th/giraph/matched-logs';

idsToLookup = LOAD '$I_CRAWLLOG_IDS_TO_LOOKUP_DIR' AS (id:chararray);

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

idMap = LOAD '$I_CRAWLLOG_ID_MAP_DIR' AS (id:chararray, url:chararray);

urlsToLookup = JOIN idsToLookup BY id, idMap BY id;
urlsToLookup = FOREACH urlsToLookup GENERATE idMap::url as url;

matchedCrawlLogLinesUrls = JOIN urlsToLookup BY url, Log BY url;
matchedCrawlLogLinesUrls = FOREACH matchedCrawlLogLinesUrls GENERATE Log::timestamp, Log::status, Log::bytes, Log::url, Log::path, Log::via, Log::type, Log::thread, Log::elapsed, Log::digest, Log::source, Log::annotations;

matchedCrawlLogLinesVias = JOIN urlsToLookup BY url, Log BY via;
matchedCrawlLogLinesVias = FOREACH matchedCrawlLogLinesVias GENERATE Log::timestamp, Log::status, Log::bytes, Log::url, Log::path, Log::via, Log::type, Log::thread, Log::elapsed, Log::digest, Log::source, Log::annotations;

STORE urlsToLookup into '$O_MATCHED_CRAWLLOG_DATA_DIR/urls-lookup/';
STORE matchedCrawlLogLinesUrls into '$O_MATCHED_CRAWLLOG_DATA_DIR/matched-urls/' using PigStorage(' ');
STORE matchedCrawlLogLinesVias into '$O_MATCHED_CRAWLLOG_DATA_DIR/matched-vias/' using PigStorage(' ');
