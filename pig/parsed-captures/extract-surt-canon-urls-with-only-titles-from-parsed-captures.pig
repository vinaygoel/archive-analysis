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

/* Input: Parsed Text Captures generated from the 'internetarchive/waimea' project
 * Output: Source URL (SURT), title text
 */

%default I_PARSED_DATA_DIR '/search/nara/congress112th/parsed/';
%default O_URL_TITLE_DIR '/search/nara/congress112th/analysis/parsed-captures-url.title.gz/';

SET mapred.max.map.failures.percent 10;
SET mapred.reduce.slowstart.completed.maps 0.9

--CDH4
REGISTER lib/webarchive-commons-jar-with-dependencies.jar;

--CDH3
--REGISTER lib/ia-web-commons-jar-with-dependencies-CDH3.jar;

REGISTER lib/ia-porky-jar-with-dependencies-CDH4.jar;

DEFINE SURTURL org.archive.porky.SurtUrlKey();
DEFINE COMPRESSWHITESPACES org.archive.porky.CompressWhiteSpacesUDF();

DEFINE FROMJSON org.archive.porky.FromJSON();
DEFINE SequenceFileLoader org.archive.porky.SequenceFileLoader();

-- Load the metadata from the parsed data, which is JSON strings stored in a Hadoop SequenceFile.
Meta = LOAD '$I_PARSED_DATA_DIR' USING SequenceFileLoader() AS (key:chararray, value:chararray);

-- Convert the JSON strings into Pig Map objects.
Meta = FOREACH Meta GENERATE FROMJSON(value) AS m:[];

-- Only retain records where the errorMessage is not present.  Records
-- that failed to parse will be present in the input, but will have an
-- errorMessage property, so if it exists, skip the record.
Meta = FILTER Meta BY m#'errorMessage' is null;

-- Only retain the fields of interest.
Meta = FOREACH Meta GENERATE m#'url'           AS src:chararray,
			     m#'title'         AS title:chararray;

-- canonicalize the URL
Meta = FOREACH Meta GENERATE SURTURL(src) as src, 
			     (title is null?'':title) as title;

Meta = FOREACH Meta GENERATE src, 
			     COMPRESSWHITESPACES(title) as title;

TitleLines = GROUP Meta BY src;
TitleLines = FOREACH TitleLines {
                        Titles = Meta.title;
                        Titles = LIMIT Titles 1;
                        GENERATE group as url, FLATTEN(Titles) as title;
             };

Store TitleLines into '$O_URL_TITLE_DIR';
