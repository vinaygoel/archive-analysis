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
urlTsCodeChecksum
/* Input: The URL-TS-Rescode-Checksum dataset 
 * Output: A distinct set of URL-Timestamp-Checksum from non redirect pages
 */

%default I_REVISIT_DATA_DIR '';
%default O_URL_TS_CHECKSUM_DIR '';

REGISTER lib/tutorial.jar;
REGISTER lib/ia-porky-jar-with-dependencies.jar;

DEFINE EXTRACTYEARFROMDATE org.archive.porky.ExtractYearFromDate();
DEFINE COMPRESSWHITESPACES org.archive.porky.CompressWhiteSpacesUDF();

--Load SURT Revisit lines (space separated)
RevisitLines = LOAD '$I_REVISIT_DATA_DIR' AS (url:chararray,
                                          timestamp:chararray,
                                          rescode:chararray,
                                          checksum:chararray);

Lines = FILTER RevisitLines BY not rescode matches '^3.*$';
Lines = FOREACH Lines GENERATE url, timestamp, checksum;
Lines = DISTINCT Lines;

STORE Lines into '$O_URL_TS_CHECKSUM_DIR';
