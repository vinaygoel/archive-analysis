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

/* Input: Key TextContent
 * Output: Sequence files containing the Key and the TextContent (to be used to generate document vectors in Mahout)
 */

--grunt-0.11.sh -Dmapred.cache.files="/user/vinay/stop-words.txt#stop-words.txt" -Dmapred.create.symlink=yes -p I_STOP_WORDS_FILE=stop-words.txt

%default I_KEY_CONTENT_DIR '/search/nara/congress112th/parsed/';
%default O_KEY_CONTENT_SEQ_DIR '/search/nara/congress112th/analysis/parsed-captures-senategovurls.content.seq/';
%default I_STOP_WORDS_FILE 'pig/text/stop-words.txt';

SET mapred.max.map.failures.percent 10;
SET mapred.reduce.slowstart.completed.maps 0.9;
SET mapred.task.timeout 80000000;

REGISTER lib/tutorial.jar;
REGISTER lib/elephant-bird-hadoop-compat-4.1.jar;
REGISTER lib/elephant-bird-pig-4.1.jar;

DEFINE TOKENIZETEXT org.archive.porky.TokenizeTextUDF('stop-words.txt');
DEFINE TOLOWER org.apache.pig.tutorial.ToLower();
DEFINE SequenceFileStorage com.twitter.elephantbird.pig.store.SequenceFileStorage();

Lines  = LOAD '$I_KEY_CONTENT_DIR' AS (key:chararray, value:chararray);

ContentLines = GROUP Lines BY key;
ContentLines = FOREACH ContentLines {
                        Content = Lines.value;
                        Content = LIMIT Content 1;
                        GENERATE group as key, FLATTEN(Content) as value;
             };

ContentLines = FILTER ContentLines BY value is not null;

--remove stop words and punctuation
ContentLines = FOREACH ContentLines GENERATE key, TOKENIZETEXT(value) as value;
ContentLines = FILTER ContentLines BY value != '';

STORE ContentLines into '$O_KEY_CONTENT_SEQ_DIR' using SequenceFileStorage('-c com.twitter.elephantbird.pig.util.TextConverter',
									   '-c com.twitter.elephantbird.pig.util.TextConverter');
