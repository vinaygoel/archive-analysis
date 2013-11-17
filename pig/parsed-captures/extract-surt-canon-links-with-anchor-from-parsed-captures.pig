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
 * Output: Links and Embeds from the parsed captures (source, timestamp, destination, link type set to empty string, and anchor text information)
 */

%default I_PARSED_DATA_DIR '/search/nara/congress112th/parsed/';
%default O_LINKS_DATA_DIR '/search/nara/congress112th/analysis/canon-parsed-captures-links.gz/';

SET mapred.max.map.failures.percent 10;
SET mapred.reduce.slowstart.completed.maps 0.9

--CDH4
--REGISTER lib/ia-web-commons-jar-with-dependencies-CDH4.jar;

--CDH3
REGISTER lib/ia-web-commons-jar-with-dependencies-CDH3.jar;

REGISTER lib/pigtools.jar;
REGISTER lib/bacon.jar
REGISTER lib/json.jar

DEFINE SURTURL pigtools.SurtUrlKey();
DEFINE COMPRESSWHITESPACES pigtools.CompressWhiteSpacesUDF();

DEFINE FROMJSON org.archive.bacon.FromJSON();

-- Load the metadata from the parsed data, which is JSON strings stored in a Hadoop SequenceFile.
Meta  = LOAD '$I_PARSED_DATA_DIR' USING org.archive.bacon.io.SequenceFileLoader() AS (key:chararray, value:chararray);

-- Convert the JSON strings into Pig Map objects.
Meta = FOREACH Meta GENERATE FROMJSON(value) AS m:[];

-- Only retain records where the errorMessage is not present.  Records
-- that failed to parse will be present in the input, but will have an
-- errorMessage property, so if it exists, skip the record.
Meta = FILTER Meta BY m#'errorMessage' is null;

-- Only retain the fields of interest.
Meta = FOREACH Meta GENERATE m#'url'          AS src:chararray,
			     m#'date'         AS timestamp:chararray,
                             m#'outlinks'     AS links:{tuple(link:[])};

Links = FOREACH Meta { 
         LinkData = FOREACH links GENERATE link#'url' AS dst:chararray, link#'text' AS linktext:chararray;
         LinkData = FILTER LinkData BY dst != '';
         LinkData = DISTINCT LinkData;
         GENERATE src, timestamp, FLATTEN(LinkData) as (dst, linktext);
       }

-- canonicalize to SURT form
Links = FOREACH Links GENERATE SURTURL(src) as src, 
			       ToDate(timestamp,'yyyyMMddHHmmss') as timestamp, 
			       SURTURL(dst) as dst, 
			       '' as path, -- since missing
			       COMPRESSWHITESPACES(linktext) as linktext;

Links = FILTER Links by src is not null and dst is not null;
Links = FILTER Links by src != '' and dst != '';

-- remove self links
Links = FILTER Links by src!=dst;
Links = DISTINCT Links;

STORE Links INTO '$O_LINKS_DATA_DIR';
