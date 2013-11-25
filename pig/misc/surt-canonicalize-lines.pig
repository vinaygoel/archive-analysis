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

/* Input: URLs to be SURT canonicalized
 * Output: SURT canonicalized URLs
 */

%default I_URLS_DIR '';
%default I_SURT_URLS_DIR '';

--CDH4
REGISTER lib/ia-web-commons-jar-with-dependencies-CDH4.jar;

--CDH3
--REGISTER lib/ia-web-commons-jar-with-dependencies-CDH3.jar;

REGISTER lib/pigtools.jar;
DEFINE SURTURL pigtools.SurtUrlKey();

Urls = LOAD '$I_URLS_DIR' AS (ourl:chararray);
Urls = FOREACH Urls GENERATE SURTURL(ourl) as url;
STORE Urls into '$O_SURT_URLS_DIR';
