/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.archive.porky;

import java.io.IOException;
import org.json.JSONObject;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * EvalFunc that converts a Pig object (typically a Map) into a JSON String.
 */
public class ToJSON extends EvalFunc<String>
{

  public String exec( Tuple input )
    throws IOException
  {
    if ( input == null || input.size() < 1 ) return null;

    try
      {
        JSONObject json;
        // If the tuple to serialize as JSON has one field which is a
        // Map, then serialize that Map, not the tuple.  Otherwise,
        // serialize the tuple.
        if ( input.size() == 1 && DataType.findType( input.get(0) ) == DataType.MAP )
          {
            json = (JSONObject) JSON.toJSON( input.get(0) );
          }
        else
          {
            json = (JSONObject) JSON.toJSON( input );
          }
        
        String jstring = json.toString();
        
        return jstring;
      }
    catch ( Exception e )
      {
        throw new RuntimeException( e );
      }
  }

  @Override
  public Schema outputSchema(Schema input) 
    {
      return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
    }

}
