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

/* Input: A set of archival web graphs to merge - src, timestamp, {set of destinations}
 * Output: Links with timestamp information. The source and destination node IDs are combined with the unique filenames of the graph they belong to (links.translate)
 */

%default I_ID_GRAPH_DIRS 'merge-graphs/*sample*-id.graph.gz';
%default O_TOMERGE_LINKS_DIR 'merge-graphs/link-data.links.translate.gz';

-- set this flag to we can use tagsource (prevent Pig from merging input files)
SET pig.splitCombination 'false';

Graph = LOAD '$I_ID_GRAPH_DIRS' using PigStorage ('\t','-tagsource') as (filename:chararray, src:chararray, timestamp:chararray, dests:{dest:(dst:chararray)});

Graph = FOREACH Graph GENERATE filename, src, timestamp, FLATTEN(dests) as dst;

-- match the filenames to that of the corresponding id.map (replace graph with map in filename)

Graph = FOREACH Graph GENERATE REPLACE((CONCAT(filename, src)), '.graph.gz','.map.gz') as src, timestamp, REPLACE((CONCAT(filename, dst)), '.graph.gz','.map.gz') as dst;

STORE Graph into '$O_TOMERGE_LINKS_DIR';
