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
 * Output: SURT-form crawled URLs (extracted from URL field)
 */

%default I_CRAWLLOG_DATA_DIR '';
%default O_CRAWLED_URLS_DIR '/search/nara/congress112th/analysis/video-urls/';

--CDH4
REGISTER lib/ia-web-commons-jar-with-dependencies-CDH4.jar;
--CDH3
--REGISTER lib/ia-web-commons-jar-with-dependencies-CDH3.jar;

REGISTER lib/ia-porky-jar-with-dependencies-CDH4.jar;

DEFINE SURTURL org.archive.porky.SurtUrlKey();

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

urls = FOREACH Log GENERATE SURTURL(url) as url;
urls = DISTINCT urls;
STORE urls into '$O_CRAWLED_URLS_DIR';
