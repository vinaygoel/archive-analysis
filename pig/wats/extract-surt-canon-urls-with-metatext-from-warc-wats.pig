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

/* Input: WAT files generated from WARC files
 * Output: Source URL (SURT), timestamp, metatext (from title/description/keywords)
 */

%default I_WATS_DIR '/search/nara/congress112th/wats/';
%default O_METATEXT_DATA_DIR '/search/nara/congress112th/analysis/metatext-from-wats.gz/';

SET pig.splitCombination 'false';
SET mapred.max.map.failures.percent 10;

REGISTER lib/tutorial.jar;
REGISTER lib/pigtools.jar;

--CDH4
REGISTER lib/ia-web-commons-jar-with-dependencies-CDH4.jar;

--CDH3
--REGISTER lib/ia-web-commons-jar-with-dependencies-CDH3.jar;

DEFINE SURTURL pigtools.SurtUrlKey();
DEFINE TOLOWER org.apache.pig.tutorial.ToLower();
DEFINE COMPRESSWHITESPACES pigtools.CompressWhiteSpacesUDF();

-- load data from I_WATS_DIR:
Orig = LOAD '$I_WATS_DIR' USING org.archive.hadoop.ArchiveJSONViewLoader('Envelope.WARC-Header-Metadata.WARC-Target-URI',
									 'Envelope.WARC-Header-Metadata.WARC-Date',
									 'Envelope.Payload-Metadata.HTTP-Response-Metadata.Response-Message.Status',
									 'Envelope.Payload-Metadata.HTTP-Response-Metadata.HTML-Metadata.Head.Title',
									 'Envelope.Payload-Metadata.HTTP-Response-Metadata.HTML-Metadata.Head.@Metas.{content,name}')
									 AS (src:chararray,timestamp:chararray,status:chararray,title:chararray,metacontent:chararray, metaname:chararray);

-- get meta text only from HTTP 200 response pages
Orig = FILTER Orig by status == '200';

-- SURT canonicalize the source URL
Orig = FOREACH Orig GENERATE SURTURL(src) as src, ToDate(timestamp) as timestamp, title, metacontent, metaname;

MetaLines = FOREACH Orig GENERATE src, timestamp, metacontent, metaname;
MetaLines = FILTER MetaLines BY metacontent != '' AND metaname != '';
MetaLines = FOREACH MetaLines GENERATE src, timestamp, metacontent, TOLOWER(metaname) as metaname;

TitleLines = FOREACH Orig GENERATE src, timestamp, title as metatext;
KeywordLines = FILTER MetaLines BY metaname == 'keywords';
KeywordLines = FOREACH KeywordLines GENERATE src, timestamp, metacontent as metatext;
DescriptionLines = FILTER MetaLines BY metaname == 'description';
DescriptionLines = FOREACH DescriptionLines GENERATE src, timestamp, metacontent as metatext;

-- let's combine all 3 sets (can be saved separately if needed)
AllLines = UNION TitleLines, KeywordLines, DescriptionLines;
AllLines = FILTER AllLines BY metatext != '';

AllLinesGrp = GROUP AllLines BY (src,timestamp);
AllLinesGrp = FOREACH AllLinesGrp GENERATE FLATTEN(group) as (src,timestamp), COMPRESSWHITESPACES(BagToString(AllLines.metatext, ' ')) as metatext;

STORE AllLinesGrp into '$O_METATEXT_DATA_DIR';
