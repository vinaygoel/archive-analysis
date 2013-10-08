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

/* Input: An archival web graph - src, timestamp, {set of destinations}
 * Output: Number of outlinks and inlinks per node, in-degree and out-degree distributions.
 */

%default I_ID_GRAPH_DIR '/search/nara/congress112th/id.graph.gz';
%default O_DEGREE_ANALYSIS_DIR '/search/nara/congress112th/degree-analysis.gz';

Graph = LOAD '$I_ID_GRAPH_DIR' as (src:chararray, timestamp:chararray, dests:{d:(dst:chararray)});

--filter out timestamps as needed.
-- then, produce link data

Links = FOREACH Graph GENERATE src, FLATTEN(dests) as dst;
Links = DISTINCT Links;

Out = GROUP Links by src;
Out = FOREACH Out {
		GENERATE group as id, COUNT(Links) as numOutLinks:long;
	};

In = GROUP Links by dst;
In = FOREACH In {
		GENERATE group as id, COUNT(Links) as numInLinks:long;
	};

OutDegreeDistribution = GROUP Out by numOutLinks;
OutDegreeDistribution = FOREACH OutDegreeDistribution GENERATE group as numOutLinks, COUNT(Out) as numnodes;

InDegreeDistribution = GROUP In by numInLinks;
InDegreeDistribution = FOREACH InDegreeDistribution GENERATE group as numInLinks, COUNT(In) as numnodes;

STORE Out into '$O_DEGREE_ANALYSIS_DIR/id-numoutlinks.gz/';
STORE In into '$O_DEGREE_ANALYSIS_DIR/id-numinlinks.gz/';

STORE OutDegreeDistribution into '$O_DEGREE_ANALYSIS_DIR/numoutlinks-numnodes.gz/';
STORE InDegreeDistribution into '$O_DEGREE_ANALYSIS_DIR/numinlinks-numnodes.gz/';
