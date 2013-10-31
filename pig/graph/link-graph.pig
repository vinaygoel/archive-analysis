/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

--
-- Build a graph as an adjacency-list from the outlinks in the parsed
-- data.
--
%default INPUT  ''
%default OUTPUT ''

SET job.name '$JOBNAME'
SET mapred.output.compress 'true'
SET mapred.job.reuse.jvm.num.tasks 1
SET mapred.reduce.slowstart.completed.maps 0.9

REGISTER lib/bacon.jar
REGISTER lib/json.jar

DEFINE FROMJSON org.archive.bacon.FromJSON();
DEFINE TOJSON   org.archive.bacon.ToJSON();
DEFINE HOST     org.archive.bacon.url.Host();
DEFINE DOMAIN   org.archive.bacon.url.Domain();

-- Load the metadata from the parsed data, which is JSON strings stored in a Hadoop SequenceFile.
meta  = LOAD '$INPUT' 
        USING org.archive.bacon.io.SequenceFileLoader()
        AS ( key:chararray, value:chararray );

-- Convert the JSON strings into Pig Map objects.
meta = FOREACH meta GENERATE FROMJSON( value ) AS m:[];

-- Only retain records where the errorMessage is not present.  Records
-- that failed to parse will be present in the input, but will have an
-- errorMessage property, so if it exists, skip the record.
meta = FILTER meta BY m#'errorMessage' is null;

-- Only retain the fields needed for our graph analysis.
meta = FOREACH meta GENERATE m#'url'          AS src:chararray,
                             HOST(m#'url')    AS src_host:chararray,
                             m#'outlinks'     AS links:{ tuple(link:[]) };

-- The host should not be empty.
meta = FILTER meta BY src_host != '';

-- Compute the source domain.
meta = FOREACH meta GENERATE src, 
                             src_host,
                             DOMAIN(src_host) AS src_domain:chararray,
                             links;

-- If the source domain cannot be computed, use the hostname.
meta = FOREACH meta GENERATE src,
                             src_host,
                             (( src_domain != '') ? src_domain :  src_host) AS  src_domain,
                             links;

-- For each capture, get the set of unique outlinks, then
-- compute the domain and host for each link destination.
-- Drop outlink types we don't care about: mailto:, etc.
meta = FOREACH meta 
       {
         -- Get the URL from the link, ignore link text.
         links = FOREACH links GENERATE link#'url' AS dest:chararray;

         -- Drop any non-http(s) links.  There are often lots of mailto
         -- and other types of links we don't care about.
         links = FILTER links BY (dest matches '^[hH][tT][tT][pP][sS]?[:]//.*');

         -- Get the hostname from the URL, and drop any that are empty
         links = FOREACH links GENERATE dest, HOST(dest) AS dest_host:chararray;
         links = FILTER links BY dest_host != '';

         -- Drop duplicate outlinks.
         links = DISTINCT links;

         -- Compute the dest domain.  If we cannot compute the domain, use the hostname.
         links = FOREACH links GENERATE dest, dest_host, DOMAIN(dest_host) AS dest_domain:chararray;
         links = FOREACH links GENERATE dest, dest_host, ((dest_domain != '') ? dest_domain : dest_host) AS dest_domain;

         GENERATE src, src_host, src_domain, links;
       }

-- Flatten out the links and zap any intra-host links.
links = FOREACH meta GENERATE src, src_host, src_domain, FLATTEN(links) AS (dest:chararray,dest_host:chararray, dest_domain:chararray);
links = FILTER links BY (src_host != dest_host);

-- Drop intra-domain links.
links = FILTER links BY (src_domain != dest_domain);

-- Drop duplicate links.
links = DISTINCT links;

-- Save the unique inter-domain links.
links = FOREACH links GENERATE TOJSON( TOMAP( 'src',         src,
                                              'src_host',    src_host,
                                              'src_domain',  src_domain,
                                              'dest',        dest,
                                              'dest_host',   dest_host,
                                              'dest_domain', dest_domain ) );

STORE links INTO '$OUTPUT' USING org.archive.bacon.io.SequenceFileStorage();
