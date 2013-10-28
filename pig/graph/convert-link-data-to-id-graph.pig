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

/* Input: Links with timestamp information
 * Input: The id.map generated from the Links
 * Output: id.graph data - srcid, timestamp, {set of destination ids} (using the IDs specified by the id.map)  
 */

--links from the WATs and (optionally) from the crawl.log data
%default I_LINKS_DATA_DIR '/search/nara/congress112th/analysis/links-from-*/';
%default I_ID_MAP_DIR '/search/nara/congress112th/analysis/id.map';
%default O_ID_GRAPH_DIR '/search/nara/congress112th/analysis/id.graph';

REGISTER lib/datafu-0.0.10.jar;
DEFINE MD5 datafu.pig.hash.MD5();

Links = LOAD '$I_LINKS_DATA_DIR' as (src:chararray, timestamp:chararray, dst:chararray);
IDMap = LOAD '$I_ID_MAP_DIR' as (id:chararray, url:chararray);

-- remove self loops
Links = FILTER Links BY src!=dst;
Links = DISTINCT Links;

-- id relation
IDR = FOREACH IDMap GENERATE url as key, 'm' as type, id as value;
IDR = DISTINCT IDR;

Links = FOREACH Links GENERATE src, timestamp, (chararray)CONCAT(src,timestamp) as srcTs, dst;
Links = FILTER Links BY srcTs is not null;

Links = FOREACH Links GENERATE src, timestamp, MD5(srcTs) as capid, dst;

-- time relation
TR = FOREACH Links GENERATE timestamp as key, 't' as type, capid as value;
TR = DISTINCT TR;

-- sources relation
SR = FOREACH Links GENERATE src as key, 's' as type, capid as value;
SR = DISTINCT SR;

-- destinations relation
DR = FOREACH Links GENERATE dst as key, 'd' as type, capid as value;
DR = DISTINCT DR;

IDSR = UNION IDR, SR;
IDSDR = UNION IDSR, DR;
IDSDR = DISTINCT IDSDR;

-- Resolve IDs
L1 = GROUP IDSDR by key;
L2 = FOREACH L1 {
		
		IDLine = FILTER IDSDR by type == 'm';
		GENERATE FLATTEN(IDLine.value) as newkey, FLATTEN(IDSDR) as (key,type,value);
	};

-- 'm' type no longer needed
L2 = FILTER L2 by type != 'm';
L2 = FOREACH L2 GENERATE newkey as key, type, value;
 
-- now combine L2 with TR
L3 = UNION L2, TR;

-- group by value / capid
-- use Capid to resolve

M1 = GROUP L3 by value;

M2 = FOREACH M1 {
		SLine = FILTER L3 by type == 's'; 
		TLine = FILTER L3 by type == 't';
		GENERATE FLATTEN(SLine.key) as srcid, FLATTEN(TLine.key) as timestamp, FLATTEN(L3) as (key,type,value);
	};

--only need destination type lines
M2 = FILTER M2 by type == 'd';

IDLinks = FOREACH M2 GENERATE srcid, timestamp, key as destid;

--group by source and timestamp
IDLinks2 = GROUP IDLinks by (srcid,timestamp);
IDGraph = FOREACH IDLinks2 {
		dests = DISTINCT IDLinks;
		GENERATE FLATTEN(group) as (src,timestamp), dests.destid;
	};

STORE IDGraph into '$O_ID_GRAPH_DIR';
