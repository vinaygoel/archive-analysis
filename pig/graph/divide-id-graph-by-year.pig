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

/* Input: Id-Graph 
 * Output: Directories containing ID-Graph divided by year 
 */

%default I_ID_GRAPH_DIR '/dataset-derived/gov/link-analysis/expanded.id.graph';
%default O_ID_GRAPH_BY_YEAR_DIR '/dataset-derived/gov/link-analysis/expanded.id.graph-by-year-TEMP';

REGISTER lib/ia-porky-jar-with-dependencies.jar;
--REGISTER /opt/pig/contrib/piggybank/java/piggybank.jar;
REGISTER lib/piggybank-0.12.0.jar;

DEFINE YEAR org.archive.porky.ExtractYearFromDate();

--SET mapreduce.job.queuename default
--SET mapreduce.reduce.memory.mb 8192
--SET default_parallel 1000

IdGraph = LOAD '$I_ID_GRAPH_DIR' as (id:chararray, timestamp:chararray, links:chararray);
IdGraph = DISTINCT IdGraph;

IdGraph = FOREACH IdGraph GENERATE YEAR(timestamp) as year, id, timestamp, links;
IdGraph = FILTER IdGraph BY year is not null;

Store IdGraph into '$O_ID_GRAPH_BY_YEAR_DIR' using org.apache.pig.piggybank.storage.MultiStorage('$O_ID_GRAPH_BY_YEAR_DIR','0');
