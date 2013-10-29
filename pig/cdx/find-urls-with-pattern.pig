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

/* Input: CDX (wayback index files for the collection(s))
 * Output: URLs (SURT) with the provided pattern (see code for the pattern)
 */

%default I_CDX_DIR '/search/nara/congress112th/cdx/';
%default O_MATCHED_URLS_DIR '/search/nara/congress112th/analysis/youtube-watch-urls';

--CDH4
--REGISTER lib/ia-web-commons-jar-with-dependencies-CDH4.jar;

--CDH3
REGISTER lib/ia-web-commons-jar-with-dependencies-CDH3.jar;

REGISTER lib/pigtools.jar;
DEFINE SURTURL pigtools.SurtUrlKey();

CDXLines = LOAD '$I_CDX_DIR' using PigStorage(' ') AS (curl:chararray, ts:chararray, ourl:chararray);
CrawledUrls = foreach CDXLines GENERATE SURTURL(ourl) as url;
CrawledUrls = DISTINCT CrawledUrls;

MatchedUrls = FILTER CrawledUrls BY url matches '^.*youtube.*watch.*';

STORE MatchedUrls into '$O_MATCHED_URLS_DIR';
