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

/* Input: Links And/Or Embeds (source, timestamp, destination, linktype, linktext)
 * Output: SURT canonicalized links (canon-source, timestamp, canon-destination, linktype, linktext) 
 */

%default I_LINKS_DIR 'congress109th-sample/*-from-wats.gz/';
%default O_CANON_LINKS_DIR 'congress109th-sample/canonicalized-link-data.gz';

--CDH4
--REGISTER lib/ia-web-commons-jar-with-dependencies-CDH4.jar;

--CDH3
REGISTER lib/ia-web-commons-jar-with-dependencies-CDH3.jar;

REGISTER lib/pigtools.jar;
DEFINE SURTURL pigtools.SurtUrlKey();

Links = LOAD '$I_LINKS_DIR' as (src:chararray, timestamp:chararray, dst:chararray, linktype:chararray, linktext:chararray);
Links = FOREACH Links GENERATE SURTURL(src) as src, ToDate(timestamp) as timestamp, SURTURL(dst) as dst, linktype, linktext;

Links = FILTER Links by src is not null and dst is not null;
Links = FILTER Links by src != '' and dst != '';
Links = FILTER Links by src!=dst;
Links = DISTINCT Links;

STORE Links into '$O_CANON_LINKS_DIR';
