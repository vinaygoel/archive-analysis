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

package org.archive.porky;
import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.archive.url.URLKeyMaker;
import org.archive.url.WaybackURLKeyMaker;


/**
 * PIG UDF that converts a URL into a SURT canconicalized URL 
 * 
 * @author vinay
 */

public class SurtUrlKey extends EvalFunc<String> {
	public String exec(Tuple input) throws IOException {
        	if (input == null || input.size() == 0)
            		return null;
        	try {
            		String url = (String)input.get(0);
			URLKeyMaker keyMaker = new WaybackURLKeyMaker();
			return keyMaker.makeKey(url);
        	} catch(Exception e){
            		return null;
        	}
    	}
}
