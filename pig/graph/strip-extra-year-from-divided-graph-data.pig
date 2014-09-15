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

/* Input: Id-Graph from a per year bucket (with extra year field)
 * Output: ID-Graph with the extra year field stripped out
 */


%default I_ID_GRAPH_BY_YEAR_EXTRAYEAR_DIR '/dataset-derived/gov/link-analysis/expanded.id.graph-by-year-TEMP/1995/';
%default O_ID_GRAPH_BY_YEAR_DIR '/dataset-derived/gov/link-analysis/expanded.id.graph-by-year/1995/';

--SET mapreduce.job.queuename default
--SET mapreduce.reduce.memory.mb 8192
--SET default_parallel 1000

IdGraph = LOAD '$I_ID_GRAPH_BY_YEAR_EXTRAYEAR_DIR' as (year:chararray, id:chararray, timestamp:chararray, links:chararray);
IdGraph = FOREACH IdGraph GENERATE id, timestamp, links;
STORE IdGraph into '$O_ID_GRAPH_BY_YEAR_DIR';
