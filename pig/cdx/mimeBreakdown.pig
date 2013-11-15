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

/* Input: CDX (wayback index files for the collection(s))
 * Output: Breakdown of MIME types per year
 */

%default I_CDX_DIR '/user/vinay/gov-cdx-1995-fy2013/';
%default O_MIME_BREAKDOWN_DIR '/user/vinay/gov-cdx-1995-fy2013-stats/';

REGISTER lib/tutorial.jar;
REGISTER lib/pigtools.jar;

DEFINE EXTRACTYEARFROMDATE pigtools.ExtractYearFromDate();
DEFINE COMPRESSWHITESPACES pigtools.CompressWhiteSpacesUDF();

--Load SURT CDX lines (space separated)
CDXLines = LOAD '$I_CDX_DIR' USING PigStorage(' ') AS (url:chararray,
                                                       timestamp:chararray,
                                                       orig_url:chararray,
						       mime:chararray,
                                                       rescode:chararray,
                                                       checksum:chararray,
                                                       redirect_url:chararray,
                                                       meta:chararray,
                                                       compressed_size:chararray,
                                                       offset:chararray,
                                                       filename:chararray);

Lines = FOREACH CDXLines GENERATE url, checksum, timestamp, COMPRESSWHITESPACES(org.apache.pig.tutorial.ToLower(mime)) as mime, (compressed_size == '-'?'0':compressed_size) as compressed_size, offset, filename;
Lines = DISTINCT Lines;

--STORE Lines into '$O_MIME_BREAKDOWN_DIR/all-lines-url-checksum-timestamp-mime-size-offset-filename-distinct.gz';

MimeLines = FOREACH Lines GENERATE EXTRACTYEARFROMDATE(timestamp) as year, mime, (long)compressed_size as compressed_size;
MimesGrp = GROUP MimeLines BY (year,mime);
MimesGrp2 = FOREACH MimesGrp GENERATE FLATTEN(group) as (year,mime), COUNT(MimeLines) as count, SUM(MimeLines.compressed_size) as totalSize;

STORE MimesGrp2 into '$O_MIME_BREAKDOWN_DIR/all-urls-year-mime-count-totalsize.gz';

BaseUrlChecksums = FOREACH Lines GENERATE url, checksum, mime, (long)compressed_size as compressed_size;
BaseUrlChecksumsGrp = GROUP BaseUrlChecksums BY (url,checksum);
BaseUrlChecksumsGrp = FOREACH BaseUrlChecksumsGrp {
                                Res = LIMIT BaseUrlChecksums 1;
                                GENERATE FLATTEN(group) as (url,checksum), FLATTEN(Res.mime) as mime, FLATTEN(Res.compressed_size) as compressed_size; 
                        };

--STORE BaseUrlChecksumsGrp into '$O_MIME_BREAKDOWN_DIR/base-urls-url-checksum-mime-size.gz';

BaseUrlsMimes = GROUP BaseUrlChecksumsGrp BY mime;
BaseUrlsMimes = FOREACH BaseUrlsMimes GENERATE group as mime, COUNT(BaseUrlChecksumsGrp) as count, SUM(BaseUrlChecksumsGrp.compressed_size) as totalSize;
STORE BaseUrlsMimes into '$O_MIME_BREAKDOWN_DIR/base-urls-mime-count-totalsize.gz';
