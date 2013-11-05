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

/* Input: Lines containing the SURT form hosts/URLs with the respective number of captures 
 * Output: Lines containing the top private domain (using public suffix list) and the number of captures. Domain is set to 'other' in the case of invalid domains.
 */

%default I_INPUT_DIR '';
%default O_OUTPUT_DIR '';

REGISTER lib/getHostFromSurtUrl.py using jython as HOST;
--REGISTER lib/getPublicSuffixDomain.py using jython as DOMAIN;
REGISTER lib/guava-13.0.1.jar;
REGISTER lib/pigtools.jar;
DEFINE DOMAIN pigtools.ExtractTopPrivateDomainFromHostNameUDF();

-- can be surtUrl as well
Lines = LOAD '$I_INPUT_DIR' AS (surthost:chararray, captures:long);
Lines = FOREACH Lines GENERATE DOMAIN(HOST.getHostFromSurtUrl(surthost)) as domain, captures;
Lines2 = GROUP Lines BY domain;
Lines2 = FOREACH Lines2 GENERATE group as domain, SUM(Lines.captures) as captures;
STORE Lines2 into '$O_OUTPUT_DIR';
