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


%default I_CRAWL_LOG_DIR '/user/vinay/nara-media/congress112th/congress112th_video_mime.txt';
%default I_OUTDEGREE_INDEGREE_PR_RANK_ANCHORTOPTERMS_METATOPTERMS '/search/nara/congress112th/canonurl-outDegree-inDegree-prRank-anchortopterms-metatopterms.gz/';

%default O_CANON_MAP_DIR '/user/vinay/nara-media/congress112th/canonurl-url/';
%default O_VIDEO_LINKTEXTDATA_DIR '/user/vinay/nara-media/congress112th/surtUrl-outDegree-inDegree-prRank-anchortopterms-metatopterms.gz';
%default O_VIDEO_REFERRER_LINKTEXTDATA_DIR '/user/vinay/nara-media/congress112th/surtReferrer-outDegree-inDegree-prRank-anchortopterms-metatopterms.gz';

REGISTER lib/ia-web-commons-jar-with-dependencies-CDH3.jar;
REGISTER lib/pigtools.jar;
DEFINE SURTURL pigtools.SurtUrlKey();

LogLines = LOAD '$I_CRAWL_LOG_DIR' as (origUrl:chararray,ts:chararray,response:chararray,mime:chararray,size:chararray,origReferrer:chararray);
LinkTextData = LOAD '$I_OUTDEGREE_INDEGREE_PR_RANK_ANCHORTOPTERMS_METATOPTERMS' as (url:chararray, outDegree:long, inDegree:long, prRank:long, anchorterms:chararray, metaterms:chararray);

LogLines = FOREACH LogLines GENERATE SURTURL(origUrl) as surtUrl, SURTURL(origReferrer) as surtReferrer, origUrl, origReferrer;
LogLines = DISTINCT LogLines;

CanonLines1 = FOREACH LogLines GENERATE surtUrl as url, origUrl as orig;
CanonLines2 = FOREACH LogLines GENERATE surtReferrer as url, origReferrer as orig;

CanonLines = UNION CanonLines1, CanonLines2;
CanonLines = DISTINCT CanonLines;

VideoUrls = FOREACH LogLines GENERATE surtUrl;
VideoUrls = DISTINCT VideoUrls;
ReferrerUrls = FOREACH LogLines GENERATE surtReferrer;
ReferrerUrls = DISTINCT ReferrerUrls;

VideoLinkTextData = JOIN VideoUrls BY surtUrl, LinkTextData BY url;
VideoReferrerLinkTextData = JOIN ReferrerUrls BY surtReferrer, LinkTextData BY url;

VideoLinkTextData = FOREACH VideoLinkTextData GENERATE LinkTextData::url, LinkTextData::outDegree, LinkTextData::inDegree, LinkTextData::prRank, LinkTextData::anchorterms, LinkTextData::metaterms;
VideoReferrerLinkTextData = FOREACH VideoReferrerLinkTextData GENERATE LinkTextData::url, LinkTextData::outDegree, LinkTextData::inDegree, LinkTextData::prRank, LinkTextData::anchorterms, LinkTextData::metaterms;

STORE CanonLines INTO '$O_CANON_MAP_DIR';
STORE VideoLinkTextData INTO '$O_VIDEO_LINKTEXTDATA_DIR';
STORE VideoReferrerLinkTextData INTO '$O_VIDEO_REFERRER_LINKTEXTDATA_DIR';

