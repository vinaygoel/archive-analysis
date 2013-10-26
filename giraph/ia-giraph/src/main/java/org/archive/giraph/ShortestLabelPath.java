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

package org.archive.giraph;

import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

/**
 * Find the shortest labelled path from a selected vertex.
 */
@Algorithm(
    name = "Shortest paths (labels)",
    description = "Find the shortest labelled path from a selected vertex"
)
public class ShortestLabelPath extends
    Vertex<LongWritable, Text,
    Text, Text> {
  /** The shortest paths id */
  public static final LongConfOption SOURCE_ID =
      new LongConfOption("ShortestLabelPath.sourceId", 1);
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(ShortestLabelPath.class);

  // 100 hops
  public static final String HOP = "-";
  public static final String MAXHOPPATH = "---------------------------------------------------------------------------------------------------";
  public static final int MAX_SUPERSTEPS = 30;

  /**
   * Is this vertex the source id?
   *
   * @return True if the source id
   */
  private boolean isSource() {
    return getId().get() == SOURCE_ID.get(getConf());
  }

  private int findNumberOfOccurrences(String string, String subString) {
    return string.length() - string.replace(subString, "").length();
  }


  @Override
  public void compute(Iterable<Text> messages) {
    // superstep0 initialization, also handle dangling nodes case
    if (getValue().toString().equals("") || getSuperstep() == 0) {
      setValue(new Text(MAXHOPPATH));
    }

    String minHopPath = isSource() ? HOP + "0:CRAWLER" : MAXHOPPATH;
    int minDist = isSource() ? 1 : findNumberOfOccurrences(MAXHOPPATH,HOP);

    for (Text message : messages) {
      String messageString = message.toString();
      //find number of hops in the received message
      int numHops = findNumberOfOccurrences(messageString,HOP);
      if(numHops < minDist) {
        minDist = numHops;
	minHopPath = messageString;
      }
    }
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Vertex " + getId() + " got minHopPath = " + minHopPath +
          " vertex value = " + getValue());
    }
  
    String currentVertexValueString = getValue().toString();
    int currentDist = findNumberOfOccurrences(currentVertexValueString,HOP);
    
    if (minDist < currentDist && getSuperstep() < MAX_SUPERSTEPS)  {
      setValue(new Text(minHopPath));
      for (Edge<LongWritable, Text> edge : getEdges()) {
        String path = minHopPath + HOP + edge.getTargetVertexId() + ":" + edge.getValue().toString();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Vertex " + getId() + " sent to " +
              edge.getTargetVertexId() + " = " + path);
        }
        sendMessage(edge.getTargetVertexId(), new Text(path));
      }
    }
    voteToHalt();
  }
}
