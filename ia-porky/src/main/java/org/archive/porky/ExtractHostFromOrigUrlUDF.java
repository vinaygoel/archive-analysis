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
import java.io.*;
import java.net.*;

/**
 * UDF which reads in the Original URL and returns the host name.
 */ 

public class ExtractHostFromOrigUrlUDF extends EvalFunc<String>
{
  public String exec( Tuple input ) throws IOException  {
    if ( input == null || input.size() == 0 ) return null;
    try {
        URL u = new URL( (String) input.get(0) );
        String host = u.getHost( );
        // If host cannot be determined, return empty string.
        if ( host == null ) host = "";
        // Ensure i18n hosts are in Unicode format.
        host = java.net.IDN.toUnicode( host, java.net.IDN.ALLOW_UNASSIGNED );
        return host;
      } catch ( MalformedURLException mue ) {
        // If not a valid URL, just return an empty string.
        return "";
      } catch ( Exception e ) {
        throw WrappedIOException.wrap("Caught exception processing input row ", e);
      }
  }
}
