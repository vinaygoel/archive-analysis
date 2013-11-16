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

/* Input: A host/domain graph - srcHostHost, dstHostHost, number of links
 * Output: Weighted host/domain PR graph - srcHostHost followed by an initial PR value(1) 
 * and a list of tab separated destinations along with their weights (number of links)
 */

%default I_HOST_GRAPH_DIR '';
%default O_PR_TAB_HOST_GRAPH_DIR '';
%default PR_INIT_SCORE '1';

Graph = LOAD '$I_HOST_GRAPH_DIR' as (srcHost:chararray, dstHost:chararray, count:long);
Graph = FOREACH Graph GENERATE srcHost, BagToString(TOBAG(dstHost,':',count),'') as weightedDstHost;

GraphGrp = GROUP Graph BY srcHost;
GraphGrp = FOREACH GraphGrp GENERATE group as srcHost, Graph.weightedDstHost as weightedDests;

PRHostGraph = FOREACH GraphGrp GENERATE srcHost, $PR_INIT_SCORE as pagerank, BagToString(weightedDests,'\t') as destString;

STORE PRHostGraph into '$O_PR_TAB_HOST_GRAPH_DIR';
