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

/* Input: The file prefix to use for the extracted files
 * Input: The max number of records to be stored in each extracted file
 * Input: The manifest file containing the offset and http/hdfs location to the source (W)ARC file
 * Output: The extraction task file containing the file prefix and the locations of the records to be stored in the file
 */

%default I_EXTRACTED_FILE_PREFIX 'DATA-EXTRACTION';
%default I_RECORDS_PER_EXTRACTED_FILE '10000';
%default I_OFFSET_SRCFILEPATH '';
%default O_TASK_FILE_FOR_EXTRACTION '';

--Load input
OffPathLines = LOAD '$I_OFFSET_SRCFILEPATH' as (offset:long, srcfilepath:chararray);
OffPathLines = DISTINCT OffPathLines;

--sort
OffPathLines = FOREACH OffPathLines GENERATE CONCAT((chararray)offset,srcfilepath) as resource, offset, srcfilepath;
OffPathLines = RANK OffPathLines BY resource;
OffPathLines = FOREACH OffPathLines GENERATE $0 as index, $2 as offset:long, $3 as srcfilepath;

--bucketize
Tasks = FOREACH OffPathLines GENERATE (int)(index/(int)'$I_RECORDS_PER_EXTRACTED_FILE') as partid, offset, srcfilepath;

Tasks = FOREACH Tasks GENERATE BagToString(TOBAG('$I_EXTRACTED_FILE_PREFIX','PART',partid), '-') as taskid, offset, srcfilepath;

TasksGrp = GROUP Tasks BY taskid;
TasksGrp = FOREACH TasksGrp {
		RecordLocation = FOREACH Tasks GENERATE (offset,srcfilepath) as loc;
		GENERATE group as taskid, RecordLocation;
	   }
STORE TasksGrp into '$O_TASK_FILE_FOR_EXTRACTION';

