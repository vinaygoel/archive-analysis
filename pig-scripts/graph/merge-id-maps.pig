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

/* Input: Set of ID-Map data from the graphs to merge
 * Output: A mapping of the merged set of link URLs/IDs to unique integers IDs (id.map) 
 * Output: A mapping of the old node IDs using the unique filenames of the graphs to be merged to the new integer IDs (id.map.translate)
 */


%default I_ID_MAP_DIRS 'merge-graphs/*sample*-id.map.gz';
%default O_NEW_ID_MAP_DIR 'merge-graphs/merged-id.map.gz';
%default O_TRANSLATE_ID_MAP_DIR 'merge-graphs/id.map.translate.gz';

-- set this flag to we can use tagsource (prevent Pig from merging input files)
SET pig.splitCombination 'false';

IDMap = LOAD '$I_ID_MAP_DIRS' using PigStorage ('\t','-tagsource') as (filename:chararray, id:chararray, url:chararray);

MergedIDMap = FOREACH IDMap GENERATE CONCAT(filename,id) as oldid, url;

Ranked = RANK MergedIDMap by url;

NewIDMap = FOREACH Ranked GENERATE $0 as id, url;
NewIDMap = DISTINCT NewIDMap;

TranslateIDMap = FOREACH Ranked GENERATE $0 as id, oldid;

Store NewIDMap into '$O_NEW_ID_MAP_DIR';
Store TranslateIDMap into '$O_TRANSLATE_ID_MAP_DIR';
