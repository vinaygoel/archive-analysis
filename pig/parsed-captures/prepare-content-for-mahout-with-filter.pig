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
 * Output: Sequence files containing the source URL (SURT) and the content field (to be used to generate document vectors in Mahout)
 */

--grunt-0.11.sh -Dmapred.cache.files="/user/vinay/stop-words.txt#stop-words.txt" -Dmapred.create.symlink=yes -p I_STOP_WORDS_FILE=stop-words.txt
%default I_PARSED_DATA_DIR '/search/nara/congress112th/parsed/';
%default O_URL_CONTENT_SEQ_DIR '/search/nara/congress112th/analysis/parsed-captures-senategovurls.content.seq/';
%default I_URL_PREFIX_FILTER '^.*$';
%default I_STOP_WORDS_FILE 'pig/text/stop-words.txt';

SET mapred.max.map.failures.percent 10;
SET mapred.reduce.slowstart.completed.maps 0.9

--CDH4
--REGISTER lib/webarchive-commons-jar-with-dependencies.jar;

--CDH3
--REGISTER lib/ia-web-commons-jar-with-dependencies-CDH3.jar;

REGISTER lib/ia-porky-jar-with-dependencies.jar;
REGISTER lib/tutorial.jar;
REGISTER lib/json-simple-1.1.1.jar;
REGISTER lib/elephant-bird-hadoop-compat-4.1.jar;
REGISTER lib/elephant-bird-pig-4.1.jar;
REGISTER lib/piggybank-0.10.jar;
--REGISTER lib/tokenize.py using jython as TOKENIZE;

DEFINE TOLOWER org.apache.pig.tutorial.ToLower();
DEFINE FROMJSON com.twitter.elephantbird.pig.piggybank.JsonStringToMap();
DEFINE SequenceFileLoader org.apache.pig.piggybank.storage.SequenceFileLoader();
DEFINE SequenceFileStorage com.twitter.elephantbird.pig.store.SequenceFileStorage();
DEFINE SURTURL org.archive.porky.SurtUrlKey();
DEFINE TOKENIZETEXT org.archive.porky.TokenizeTextUDF('$I_STOP_WORDS_FILE');

-- Load the metadata from the parsed data, which is JSON strings stored in a Hadoop SequenceFile.
Meta  = LOAD '$I_PARSED_DATA_DIR' USING SequenceFileLoader() AS (key:chararray, value:chararray);

-- Convert the JSON strings into Pig Map objects.
Meta = FOREACH Meta GENERATE FROMJSON(value) AS m:[];

-- Only retain records where the errorMessage is not present.  Records
-- that failed to parse will be present in the input, but will have an
-- errorMessage property, so if it exists, skip the record.
Meta = FILTER Meta BY m#'errorMessage' is null;

-- Only retain the fields of interest.
Meta = FOREACH Meta GENERATE m#'url'           AS src:chararray,
			     m#'code'          AS code:chararray,
			     m#'content'       AS content:chararray;

-- Only extract content from HTTP-200 responses
Meta = FILTER Meta BY code == '200';

-- canonicalize the URL
Meta = FOREACH Meta GENERATE SURTURL(src) as src, content;

--filter out robots.txt captures
Meta = FILTER Meta BY not src matches '.*robots.txt$';

--FILTER by url prefix
Meta = FILTER Meta BY src matches '$I_URL_PREFIX_FILTER';

ContentLines = GROUP Meta BY src;
ContentLines = FOREACH ContentLines {
                        Content = Meta.content;
                        Content = LIMIT Content 1;
                        GENERATE group as key, FLATTEN(Content) as value;
             };

ContentLines = FILTER ContentLines BY value is not null;

--remove stop words and punctuation
ContentLines = FOREACH ContentLines GENERATE key, TOKENIZETEXT(value) as value;
ContentLines = FILTER ContentLines BY value != '';

STORE ContentLines into '$O_URL_CONTENT_SEQ_DIR' using SequenceFileStorage('-c com.twitter.elephantbird.pig.util.TextConverter',
									   '-c com.twitter.elephantbird.pig.util.TextConverter');
