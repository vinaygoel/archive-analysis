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
 * Output: Web graph - The source id (src) followed by an initial PR value(1) and a list of tab separated destinations (no timestamp information)
 */


%default I_ID_GRAPH_DIR '/search/nara/congress112th/analysis/id.graph';
%default O_PR_TAB_ID_GRAPH_DIR '/search/nara/congress112th/analysis/pr-id.graph';
%default PR_INIT_SCORE '1';

REGISTER lib/collectBagElements.py using jython as COLLECTBAGELEMENTS;

Graph = LOAD '$I_ID_GRAPH_DIR' as (src:chararray, timestamp:chararray, dests:{d:(dst:chararray)});
Links = foreach Graph generate src, FLATTEN(dests) as dst;
Links = DISTINCT Links;
GraphWithoutTimestamps = GROUP Links by src;
Graph = FOREACH GraphWithoutTimestamps GENERATE group as src, Links.dst as dests;
PRIDGraph = FOREACH Graph GENERATE src, $PR_INIT_SCORE as pagerank, COLLECTBAGELEMENTS.collectBagElements(dests) as destString;
STORE PRIDGraph into '$O_PR_TAB_ID_GRAPH_DIR';
