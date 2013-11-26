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
import org.apache.pig.impl.util.WrappedIOException;
import java.util.regex.*;

/**
 * PIG UDF that extracts the year from a timestamp
 * 
 * @author vinay
 */

public class ExtractYearFromDate extends EvalFunc<String> {
	public String exec(Tuple input) throws IOException {
        	if (input == null || input.size() == 0)
            	return null;
        	try {
            		String s = (String)input.get(0);
			int year = 2000;
			s = s.substring(0,4);
			year = Integer.parseInt(s);
	    		return (Integer.toString(year));		
        	} catch(Exception e){
        		return null;
		}
    	}
}
