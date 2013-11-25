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
 * Output: Links and Embeds from HTML pages (source, timestamp, destination, link type, and anchor text information)
 */

%default I_WATS_DIR '/search/nara/congress112th/wats/';
%default O_LINKS_DATA_DIR '/search/nara/congress112th/analysis/canon-wat-links.gz/'; 

SET pig.splitCombination 'false';
SET mapred.max.map.failures.percent 10;

--CDH4
REGISTER lib/ia-web-commons-jar-with-dependencies-CDH4.jar;

--CDH3
--REGISTER lib/ia-web-commons-jar-with-dependencies-CDH3.jar;

REGISTER lib/pigtools.jar;
DEFINE URLRESOLVE org.archive.hadoop.func.URLResolverFunc();
DEFINE SURTURL pigtools.SurtUrlKey();
DEFINE COMPRESSWHITESPACES pigtools.CompressWhiteSpacesUDF();

-- load data from I_WATS_DIR:
Orig = LOAD '$I_WATS_DIR' USING org.archive.hadoop.ArchiveJSONViewLoader('Envelope.WARC-Header-Metadata.WARC-Target-URI',
									 'Envelope.WARC-Header-Metadata.WARC-Date',
									 'Envelope.Payload-Metadata.HTTP-Response-Metadata.HTML-Metadata.Head.Base',
									 'Envelope.Payload-Metadata.HTTP-Response-Metadata.HTML-Metadata.@Links.{url,path,text,alt}')
									 as (src:chararray,timestamp:chararray,html_base:chararray,relative:chararray,path:chararray,text:chararray,alt:chararray);

-- discard lines without links
LinksOnly = FILTER Orig by relative != '';

-- Generate the resolved destination-URL
Links = FOREACH LinksOnly GENERATE src, timestamp, URLRESOLVE(src,html_base,relative) as dst, path, CONCAT(text,alt) as linktext;

-- canonicalize to SURT form
Links = FOREACH Links GENERATE SURTURL(src) as src, ToDate(timestamp) as timestamp, SURTURL(dst) as dst, path, COMPRESSWHITESPACES(linktext) as linktext;
Links = FILTER Links by src is not null and dst is not null;
Links = FILTER Links by src != '' and dst != '';

-- remove self links
Links = FILTER Links by src!=dst;
Links = DISTINCT Links;

--EmbedLinks = FILTER Links by (path != 'A@/href') AND (path != 'FORM@/action');
--OutLinks = FILTER Links by (path == 'A@/href') OR (path == 'FORM@/action');

STORE Links INTO '$O_LINKS_DATA_DIR';
