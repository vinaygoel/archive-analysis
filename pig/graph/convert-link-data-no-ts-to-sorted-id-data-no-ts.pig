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

/* Input: Links without timestamp information
 * Output: The id.map generated from the Links
 * Output: id.graph data - srcid, {set of sorted destination ids} (using the IDs specified by the id.map)  
 */

-- lines containing src<tab>dst lines
%default I_LINKS_DATA_NO_TS_DIR '/tmp/hosts.txt';
%default O_ID_MAP_DIR '/tmp/hosts-id.map';
%default O_ID_SORTEDINT_GRAPH_NO_TS_DIR '/tmp/hosts-id.graph-no-ts';

REGISTER lib/datafu-0.0.10.jar;
DEFINE MD5 datafu.pig.hash.MD5();

Links = LOAD '$I_LINKS_DATA_NO_TS_DIR' as (src:chararray, dst:chararray);

--keep self loops?
Links = FILTER Links BY src is not null and src!= '' and dst is not null and dst!='';
Links = DISTINCT Links;

-- generate ID-MAP
S = FOREACH Links GENERATE src as url;
D = FOREACH Links GENERATE dst as url;
A = UNION S, D;
A = DISTINCT A;
Ranked = RANK A by url;
IDMap = FOREACH Ranked GENERATE (chararray)$0 as id, (chararray)$1 as url;

-- id relation
IDR = FOREACH IDMap GENERATE url as key, 'm' as type, id as value;
IDR = DISTINCT IDR;

-- sources relation
SR = FOREACH Links GENERATE src as key, 's' as type, src as value;
SR = DISTINCT SR;

-- destinations relation
DR = FOREACH Links GENERATE dst as key, 'd' as type, src as value;
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

-- group by value / src
M1 = GROUP L2 by value;

M2 = FOREACH M1 {
		SLine = FILTER L2 by type == 's'; 
		GENERATE FLATTEN(SLine.key) as srcid, FLATTEN(L2) as (key,type,value);
	};

--only need destination type lines
M2 = FILTER M2 by type == 'd';

IDLinks = FOREACH M2 GENERATE srcid, key as destid;

LongIDLinks = FOREACH IDLinks GENERATE (long)srcid as srcid, (long)destid as destid;
LongIDLinks = DISTINCT LongIDLinks;

--group by source
IDLinks = GROUP LongIDLinks by srcid;
IDGraph = FOREACH IDLinks {
		dests = ORDER LongIDLinks BY destid;
		GENERATE FLATTEN(group) as srcid, dests.destid;
	};
STORE IDMap into '$O_ID_MAP_DIR';
STORE IDGraph into '$O_ID_SORTEDINT_GRAPH_NO_TS_DIR';
