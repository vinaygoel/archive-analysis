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
 * Output: Source URL (SURT), timestamp, metatext (from title/description/keywords)
 */

%default I_PARSED_DATA_DIR '/search/nara/congress112th/parsed/';
%default O_METATEXT_DATA_DIR '/search/nara/congress112th/analysis/metatext-from-parsed-captures.gz/';

SET mapred.max.map.failures.percent 10;
SET mapred.reduce.slowstart.completed.maps 0.9

--CDH4
--REGISTER lib/ia-web-commons-jar-with-dependencies-CDH4.jar;

--CDH3
REGISTER lib/ia-web-commons-jar-with-dependencies-CDH3.jar;

REGISTER lib/pigtools.jar;
REGISTER lib/bacon.jar
REGISTER lib/json.jar

DEFINE URLRESOLVE org.archive.hadoop.func.URLResolverFunc();
DEFINE SURTURL pigtools.SurtUrlKey();
DEFINE COMPRESSWHITESPACES pigtools.CompressWhiteSpacesUDF();

DEFINE FROMJSON org.archive.bacon.FromJSON();
DEFINE TOJSON   org.archive.bacon.ToJSON();

-- Load the metadata from the parsed data, which is JSON strings stored in a Hadoop SequenceFile.
Meta  = LOAD '$I_PARSED_DATA_DIR' USING org.archive.bacon.io.SequenceFileLoader() AS (key:chararray, value:chararray);

-- Convert the JSON strings into Pig Map objects.
Meta = FOREACH Meta GENERATE FROMJSON(value) AS m:[];

-- Only retain records where the errorMessage is not present.  Records
-- that failed to parse will be present in the input, but will have an
-- errorMessage property, so if it exists, skip the record.
Meta = FILTER Meta BY m#'errorMessage' is null;

-- Only retain the fields of interest.
Meta = FOREACH Meta GENERATE m#'url'           AS src:chararray,
			     m#'date'          AS timestamp:chararray,
			     m#'code'          AS code:chararray,
			     m#'title'         AS title:chararray,
			     m#'description'   AS description:chararray,
			     m#'keywords'      AS keywords:chararray;

-- get meta text only from HTTP 200 response pages
Meta = FILTER Meta BY code == '200';

-- canonicalize the URL
Meta = FOREACH Meta GENERATE SURTURL(src) as src, 
			     ToDate(timestamp,'yyyyMMddHHmmss') as timestamp,
			     (title is null?'':title) as title,
			     (description is null?'':description) as description,
			     (keywords is null?'':keywords) as keywords;

Meta = FOREACH Meta GENERATE src,
			     timestamp,
			     BagToString(TOBAG(title,description,keywords), ' ') as metatext;

Meta = FILTER Meta BY metatext is not null;

Meta = FOREACH Meta GENERATE src, 
			     timestamp,
			     COMPRESSWHITESPACES(metatext) as metatext;

Meta = FILTER Meta BY metatext != '' AND metatext != ' ';

Store Meta into '$O_METATEXT_DATA_DIR';
