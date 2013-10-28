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

/* Input: Lines containing the top N anchor-terms bag for each doc (url)
 * Input: Lines containing the top N metatext-terms bag for each doc (url)
 * Output: Lines containing the url with the respective anchor-terms bag and metatext-terms bag
 */

%default I_ANCHORTEXT_TOPTERMS_DIR '/search/nara/congress112th/analysis/url.topanchortext.gz/';
%default I_METATEXT_TOPTERMS_DIR '/search/nara/congress112th/analysis/url.topmetatext.gz/';
%default O_URL_ANCHORTEXTTOPTERMS_METATEXTTOPTERMS '/search/nara/congress112th/analysis/url.topanchortext-topmetatext.gz';

--can read top terms in as chararray here instead of bag - topTerms:{termWithScores:(term:chararray,score:double)}
AnchorTopTerms = LOAD '$I_ANCHORTEXT_TOPTERMS_DIR' AS (url:chararray, anchorTerms);
MetaTopTerms = LOAD '$I_METATEXT_TOPTERMS_DIR' AS (url:chararray, metaTerms);

Urls1 = FOREACH AnchorTopTerms GENERATE url;
Urls2 = FOREACH MetaTopTerms GENERATE url;

AllUrls = UNION Urls1, Urls2;
AllUrls = DISTINCT AllUrls;

AnchorData = JOIN AllUrls BY url left, AnchorTopTerms BY url;
AnchorData = FOREACH AnchorData GENERATE AllUrls::url as url, (AnchorTopTerms::anchorTerms is null?'{}':AnchorTopTerms::anchorTerms) as anchorTerms;

AnchorAndMetaTextData = JOIN AnchorData BY url left, MetaTopTerms BY url;
AnchorAndMetaTextData = FOREACH AnchorAndMetaTextData GENERATE AnchorData::url as url, AnchorData::anchorTerms as anchorTerms, (MetaTopTerms::metaTerms is null?'{}':MetaTopTerms::metaTerms) as metaTerms;

STORE AnchorAndMetaTextData INTO '$O_URL_ANCHORTEXTTOPTERMS_METATEXTTOPTERMS';
