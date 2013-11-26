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

import java.io.*;
import java.util.*;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONException;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.FuncSpec;


/**
 * EvalFunc that converts a JSON String into a Pig Map
 */
public class FromJSON extends EvalFunc<Map>
{
  @Override
  public Map exec( Tuple input )
    throws IOException
  {
    if ( input == null || input.size() < 1 )
      {
        return null;
      }

    try
      {
        String jsonString = (String) input.get(0);

        JSONObject json = new JSONObject( jsonString );

        Object o = JSON.fromJSON( json );

        return (Map) o;
      }
    catch ( Exception e )
      {
        throw new RuntimeException("Error while creating a map", e);
      }
  }

  @Override
  public Schema outputSchema(Schema input) 
    {
      return new Schema(new Schema.FieldSchema(null, DataType.MAP));
    }

}
