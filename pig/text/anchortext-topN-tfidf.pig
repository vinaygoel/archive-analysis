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

/* Input: Links (src, ts, dst, path, linktext)
 * Output: dst -> top N TF-IDF terms 
 */

-- make sure to supply the stop words list as a distributed cache!!!!
--grunt-0.11.sh -Dmapred.cache.files="/user/vinay/stop-words.txt#stop-words.txt" -Dmapred.create.symlink=yes -p I_STOP_WORDS_FILE=stop-words.txt

%default I_LINKS_DATA_DIR '/search/nara/congress112th/analysis/links-from-wats-only-crawled-resources.gz/';
%default O_URL_ANCHORTEXT_TOPTERMS_DIR '/search/nara/congress112th/analysis/url.topanchortext.gz/';

%default N '50';
%default I_STOP_WORDS_FILE 'pig/text/stop-words.txt';

import 'pig/text/tfidf.macro';
import 'pig/text/topN.macro';
REGISTER lib/tutorial.jar;
REGISTER lib/ia-porky-jar-with-dependencies.jar;
--REGISTER lib/tokenize.py using jython as TOKENIZE;
DEFINE TOLOWER org.apache.pig.tutorial.ToLower();
--DEFINE COMPRESSWHITESPACES org.archive.porky.CompressWhiteSpacesUDF();
DEFINE TOKENIZETEXT org.archive.porky.TokenizeTextUDF('stop-words.txt');

Links = LOAD '$I_LINKS_DATA_DIR' as (src:chararray, timestamp:chararray, dst:chararray, path:chararray, linktext:chararray);
Links = FILTER Links BY linktext is not null and linktext != '';
Links = DISTINCT Links;

-- Extract records and fields of interest
Links = FOREACH Links GENERATE dst as doc, linktext as text;

--remove stop words and punctuation
--Docs = FOREACH Links GENERATE doc, TOKENIZE.tokenize(text,'$I_STOP_WORDS_FILE') as text;
--Docs = FOREACH Docs GENERATE doc, COMPRESSWHITESPACES(text) as text;
Docs = FOREACH Links GENERATE doc, TOKENIZETEXT(text) as text;
Docs = FILTER Docs BY text != '';

-- Use TF-IDF Macro, returns fields: doc, term, tfidf
tfIdfScores = TF_IDF(Docs,'doc','text');

tfIdfScores = FOREACH tfIdfScores GENERATE doc, term, (double)tfidf as tfidf;

-- Use TOP_N macro to get Top N TF-IDF terms per Doc, returns doc, bag of (term,score) tuples
TopNTfIdfScores = TOP_N(tfIdfScores,'doc','term','tfidf',$N);

STORE TopNTfIdfScores into '$O_URL_ANCHORTEXT_TOPTERMS_DIR'; 

