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

/* Input: Lines containing Outdegree, Indegree and Pagerank ranks for each doc (url)
 * Input: Lines containing the URLs to find in the link stats dataset
 * Input: Lines containing the found URLs with the Outdegree, Indegree and Pagerank ranks (sorted by rank)
 */

%default I_URL_OUTDEGREE_INDEGREE_PRRANK_DIR '/search/nara/congress112th/analysis/url.outdegree-indegree-prrank';
%default I_ORIGURL_TO_FIND_DIR '/search/nara/congress112th/analysis/video-urls/';
%default O_FILTERED_ORIGURL_URL_OUTDEGREE_INDEGREE_PRRANK_DIR '/search/nara/congress112th/analysis/video.url.outdegree-indegree-prrank/';

--CDH4
REGISTER lib/webarchive-commons-jar-with-dependencies.jar;

--CDH3
--REGISTER lib/ia-web-commons-jar-with-dependencies-CDH3.jar;

REGISTER lib/ia-porky-jar-with-dependencies-CDH4.jar;
DEFINE SURTURL org.archive.porky.SurtUrlKey();

LinkStats = LOAD '$I_URL_OUTDEGREE_INDEGREE_PRRANK_DIR' AS (url:chararray, outDegree:long, inDegree:long, prRank:long);
Urls = LOAD '$I_ORIGURL_TO_FIND_DIR' AS (origurl:chararray);
Urls = FOREACH Urls GENERATE SURTURL(origurl) as url;
Urls = DISTINCT Urls;

Joined = JOIN Urls BY url, LinkStats BY url;
Joined = FOREACH Joined GENERATE Urls::origurl as origurl, LinkStats::url as url, LinkStats::outDegree as outDegree, LinkStats::inDegree as inDegree, LinkStats::prRank as prRank;
Sorted = ORDER Joined BY prRank;

STORE Sorted INTO '$O_FILTERED_ORIGURL_URL_OUTDEGREE_INDEGREE_PRRANK_DIR';
