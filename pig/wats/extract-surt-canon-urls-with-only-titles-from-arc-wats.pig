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

/* Input: WAT files generated from ARC files
 * Output: Source URL (SURT), title text
 */

%default I_WATS_DIR '/search/nara/congress112th/wats/';
%default O_URL_TITLE_DIR '/search/nara/congress112th/analysis/url.title.gz/';

SET pig.splitCombination 'false';
SET mapred.max.map.failures.percent 10;

REGISTER lib/tutorial.jar;
REGISTER lib/pigtools.jar;

--CDH4
REGISTER lib/ia-web-commons-jar-with-dependencies-CDH4.jar;

--CDH3
--REGISTER lib/ia-web-commons-jar-with-dependencies-CDH3.jar;

DEFINE SURTURL pigtools.SurtUrlKey();
DEFINE COMPRESSWHITESPACES pigtools.CompressWhiteSpacesUDF();

-- load data from I_WATS_DIR:
Orig = LOAD '$I_WATS_DIR' USING org.archive.hadoop.ArchiveJSONViewLoader('Envelope.ARC-Header-Metadata.Target-URI',
									 'Envelope.Payload-Metadata.HTTP-Response-Metadata.HTML-Metadata.Head.Title')
									 AS (src:chararray,title:chararray);

-- SURT canonicalize the source URL
Orig = FOREACH Orig GENERATE SURTURL(src) as src, title;
Orig = FILTER Orig BY title != '';
Orig = FOREACH Orig GENERATE src, COMPRESSWHITESPACES(title) as title;

TitleLines = GROUP Orig BY src;
TitleLines = FOREACH TitleLines {
			Titles = Orig.title;
			Titles = LIMIT Titles 1;
			GENERATE group as url, FLATTEN(Titles) as title;
	     };

STORE TitleLines into '$O_URL_TITLE_DIR';
