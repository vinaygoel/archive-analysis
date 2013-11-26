/*
 * Copyright 2012 Internet Archive
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

import java.io.*;
import java.util.*;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONException;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.BagFactory;

/**
 * Utility class for converting between JSON strings and Pig objects
 */
public class JSON
{
  /**
   * Convert the given Pig object into a JSON object, recursively
   * convert child objects as well.
   */
  public static Object toJSON( Object o )
    throws JSONException, IOException
  {
    switch ( DataType.findType( o ) )
      {
      case DataType.NULL:
        return JSONObject.NULL;

      case DataType.BOOLEAN:
      case DataType.INTEGER:
      case DataType.LONG:
      case DataType.DOUBLE:
        return o;
        
      case DataType.FLOAT:
        return Double.valueOf( ((Float)o).floatValue() );
        
      case DataType.CHARARRAY:
        return o.toString( );
        
      case DataType.MAP:
        {
          Map<String,Object> m = (Map<String,Object>) o;
          JSONObject json = new JSONObject();
          for( Map.Entry<String, Object> e: m.entrySet( ) )
            {
              String key   = e.getKey();
              Object value = toJSON( e.getValue() );
              
              json.put( key, value );
            }
          return json;
        }

      case DataType.TUPLE:
        {
          JSONObject json = new JSONObject( );

          Tuple t = (Tuple) o;
          for ( int i = 0; i < t.size(); ++i ) 
            {
              Object value = toJSON( t.get(i) );
              
              json.put( "$" + i , value );
            }

          return json;
        }

      case DataType.BAG:
        {
          JSONArray values = new JSONArray();

          for ( Tuple t : ((DataBag) o) )
            {
              switch ( t.size() )
                {
                case 0:
                  continue ;

                case 1:
                  {
                    Object innerObject = toJSON( t.get(0) );

                    values.put( innerObject );
                  }
                  break;
                  
                default:
                  JSONArray innerList = new JSONArray();
                  for ( int i = 0; i < t.size(); ++i ) 
                    {
                      Object innerObject = toJSON( t.get(i) );

                      innerList.put( innerObject );
                    }
                  
                  values.put( innerList );
                  break;
                }
              
            }

          return values;
        }

      case DataType.BYTEARRAY:
        // FIXME?  What else can we do?  base-64 encoded string?
        System.err.println( "Pig BYTEARRAY not supported for JSONStorage" );
        return null;
        
      default:
        System.out.println( "unknown type: " + DataType.findType( o ) + " value: " + o.toString( ) );
        return null;
      }
  }

  /**
   * Convert JSON object into a Pig object, recursively convert
   * children as well.
   */
  public static Object fromJSON( Object o ) 
    throws IOException, JSONException
  {
    if ( o instanceof String  ||
         o instanceof Long    ||
         o instanceof Double  ||
         o instanceof Integer )
      {
        return o;
      }
    else if ( JSONObject.NULL.equals(o) )
      {
        return null;
      }
    else if ( o instanceof JSONObject )
      {
        JSONObject json = (JSONObject) o;

        Map<String,Object> map = new HashMap<String,Object>( json.length() );

        // If getNames() returns null, then it's  an empty JSON object.
        String[] names = JSONObject.getNames( json );

        if ( names == null ) return map;

        for ( String key : JSONObject.getNames( json ) )
          {
            Object value = json.get( key );
            
            // Recurse the value
            map.put( key, fromJSON( value ) );
          }
        
        // Now, check to see if the map keys match the formula for
        // a Tuple, that is if they are: "$0", "$1", "$2", ...
        
        // First, peek to see if there is a "$0" key, if so, then 
        // start moving the map entries into a Tuple.
        if ( map.containsKey( "$0" ) )
          {
            Tuple tuple = TupleFactory.getInstance().newTuple( map.size() );

            for ( int i = 0 ; i < map.size() ; i++ )
              {
                // If any of the expected $N keys is not found, give
                // up and return the map.
                if ( ! map.containsKey( "$" + i ) ) return map;
                
                tuple.set( i, map.get( "$" + i ) );
              }

            return tuple;
          }

        return map;
      }
    else if ( o instanceof JSONArray )
      {
        JSONArray json = (JSONArray) o;
        
        List<Tuple> tuples = new ArrayList<Tuple>( json.length() );

        for ( int i = 0; i < json.length() ; i++ )
          {
            tuples.add( TupleFactory.getInstance().newTuple( fromJSON( json.get(i) ) ) );
          }

        DataBag bag = BagFactory.getInstance().newDefaultBag( tuples );

        return bag;
      }
    else if ( o instanceof Boolean )
      {
        // Since Pig doesn't have a true boolean data type, we map it to
        // String values "true" and "false".
        if ( ((Boolean) o).booleanValue() )
          {
            return "true";
          }
        return "false";
      }
    else
      {
        // FIXME: What to do here?
        throw new IOException( "Unknown data-type serializing from JSON: " + o );
      }
  }

}
