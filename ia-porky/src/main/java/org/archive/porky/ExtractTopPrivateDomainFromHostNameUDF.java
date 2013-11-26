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
import java.util.regex.*;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;
import com.google.common.net.InternetDomainName;

/**
 * PIG UDF that extracts the top-private-domain from a host
 * returns "other" if not a valid tpd 
 * @author vinay
 */


public class ExtractTopPrivateDomainFromHostNameUDF extends EvalFunc<String> {
	public String exec(Tuple input) throws IOException {
        	if (input == null || input.size() == 0)
            	return null;
		String str = null;
        	try {
            		str = (String)input.get(0);
			InternetDomainName domainName = InternetDomainName.from(str);
	    		return (domainName.topPrivateDomain().name());		
        	} catch(Exception e){
			return "other";
		}
    	}
}
