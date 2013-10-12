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

/* Input: Lines containing the top 50 anchor-terms bag for each doc (url)
 * Input: Lines containing the top 50 metatext-terms bag for each doc (url)
 * Input: Lines containing Outdegree, Indegree and Pagerank ranks for each doc (url)
 * Output: Lines containing Outdegree, Indegree, Pagerank rank, anchor-terms bag, and metatext-terms bag
 */

%default I_ANCHOR_TOPN_TFIDF_DIR '/search/nara/congress112th/anchor-top-tfidf.gz/';
%default I_META_TEXT_TOPN_TFIDF_DIR '/search/nara/congress112th/meta-text-top-tfidf.gz/';
%default I_OUTDEGREE_INDEGREE_PR_RANK '/search/nara/congress112th/canonurl-outDegree-inDegree-prRank.gz/';
%default O_OUTDEGREE_INDEGREE_PR_RANK_ANCHORTOPTERMS_METATOPTERMS '/search/nara/congress112th/canonurl-outDegree-inDegree-prRank-anchortopterms-metatopterms.gz/';

%default PRRANK_FOR_UNKNOWN_PAGE '1000000000';

outInPrData = LOAD '$I_OUTDEGREE_INDEGREE_PR_RANK' AS (url:chararray, outDegree:long, inDegree:long, prRank:long);
--can read top terms in as chararray here instead of bag - topTerms:{termWithScores:(term:chararray,score:double)}
anchorTopTerms = LOAD '$I_ANCHOR_TOPN_TFIDF_DIR' AS (url:chararray, anchorTerms);
metaTopTerms = LOAD '$I_META_TEXT_TOPN_TFIDF_DIR' AS (url:chararray, metaTerms);

urls1 = FOREACH outInPrData GENERATE url;
urls2 = FOREACH anchorTopTerms GENERATE url;
urls3 = FOREACH metaTopTerms GENERATE url;

allUrls = UNION urls1, urls2;
allUrls = UNION allUrls, urls3;
allUrls = DISTINCT allUrls;

linkData = JOIN allUrls by url left, outInPrData by url;
linkData = FOREACH linkData GENERATE allUrls::url as url, (outInPrData::outDegree is null?0:outInPrData::outDegree) as outDegree, (outInPrData::inDegree is null?0:outInPrData::inDegree) as inDegree, (outInPrData::prRank is null?$PRRANK_FOR_UNKNOWN_PAGE:outInPrData::prRank) as prRank;

linkAnchorData = JOIN linkData BY url left, anchorTopTerms BY url;
linkAnchorData = FOREACH linkAnchorData GENERATE linkData::url as url, linkData::outDegree as outDegree, linkData::inDegree as inDegree, linkData::prRank as prRank, (anchorTopTerms::anchorTerms is null?'{}':anchorTopTerms::anchorTerms) as anchorTerms;

linkAnchorMetaTextData = JOIN linkAnchorData BY url left, metaTopTerms BY url;
linkAnchorMetaTextData = FOREACH linkAnchorMetaTextData GENERATE linkAnchorData::url as url, linkAnchorData::outDegree as outDegree, linkAnchorData::inDegree as inDegree, linkAnchorData::prRank as prRank, linkAnchorData::anchorTerms as anchorTerms, (metaTopTerms::metaTerms is null?'{}':metaTopTerms::metaTerms) as metaTerms;

linkAnchorMetaTextData = ORDER linkAnchorMetaTextData BY prRank;
STORE linkAnchorMetaTextData INTO '$O_OUTDEGREE_INDEGREE_PR_RANK_ANCHORTOPTERMS_METATOPTERMS';
