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

/* Input: Archival web graphs - src, timestamp, {set of destinations}
 * Output: Links with timestamp information - src, timestamp, destination.
 */

%default I_ID_GRAPH_DIR 'congress109th-sample/id.graph.gz';
%default O_ID_LINKS_DIR 'congress109th-sample/id-links.gz';

Graph = LOAD '$I_ID_GRAPH_DIR' as (src:chararray, timestamp:chararray, dests:{dest:(dst:chararray)});
Links = FOREACH Graph GENERATE src, timestamp, FLATTEN(dests) as dst;
STORE Links into '$O_ID_LINKS_DIR';
