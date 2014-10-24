/*
 * Copyright 2014 Internet Archive
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
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

/**
 * Output Format to support multiplexing
 *
 * @author vinay
 */

public class KeyBasedMultipleTextOutputFormat<K, V> extends MultipleTextOutputFormat<K, V> {
 
  @Override
  protected String generateFileNameForKeyValue (K key, V value, String name) {
    return key.toString();
  }

  @Override
  protected K generateActualKey(K key, V value) {
    return null;
  }
}
