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

/* Input: ID-Map data
 * Output: ID-Map that maps a unique integer ID to each hostname in the graph.
 * Output: A Two Level ID-Map with each old ID prefixed by a unique Host id. Each hostname in the graph is assigned a unique integer. The new id is of the form hostid:urlid (id.map) 
 * Output: A mapping of the old node IDs to the new hostid:urlid IDs (id.map.translate)
 */

%default I_ID_MAP_DIR 'congress109th-sample/id.map.gz';
%default O_HOSTNAME_ID_MAP_DIR 'congress109th-sample/hostname-id.map.gz';
%default O_TWO_LEVEL_ID_MAP_DIR 'congress109th-sample/twolevel-hosturl-id.map.gz';
%default O_TRANSLATE_ID_MAP_DIR 'congress109th-sample/twolevel-hosturl-id.map.translate.gz';

REGISTER lib/pigtools.jar;
DEFINE HOSTNAME pigtools.ExtractHostNameFromCanonUrlUDF();

IDMap = LOAD '$I_ID_MAP_DIR' as (id:chararray, url:chararray);

IDMapWithHostInfo = FOREACH IDMap GENERATE id, url, HOSTNAME(url) as hostname;

Ranked = RANK IDMapWithHostInfo by hostname DENSE;

Ranked = FOREACH Ranked GENERATE ((chararray)CONCAT((chararray)$0,':')) as hostidprefix, id, url, hostname;

TwoLevelIDMap = FOREACH Ranked GENERATE (chararray)CONCAT(hostidprefix,id) as twolevelid, id, url;
NewTwoLevelIDMap = FOREACH TwoLevelIDMap GENERATE twolevelid as id, url as url;
TranslateIDMap = FOREACH TwoLevelIDMap GENERATE twolevelid as newid, id as oldid;

HostnameIDMap = FOREACH Ranked GENERATE hostidprefix, hostname;
HostnameIDMap = DISTINCT HostnameIDMap;

Store HostnameIDMap into '$O_HOSTNAME_ID_MAP_DIR';
Store NewTwoLevelIDMap into '$O_TWO_LEVEL_ID_MAP_DIR';
Store TranslateIDMap into '$O_TRANSLATE_ID_MAP_DIR';
