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

import org.apache.giraph.graph.BasicComputation;
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
public class ShortestLabelPathComputation extends
    BasicComputation<LongWritable, Text, Text, Text> {
  /** The shortest paths id */
  private static final LongConfOption SOURCE_ID =
      new LongConfOption(
      "ShortestLabelPathComputation.sourceId", 1, "Source Vertex ID");
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(ShortestLabelPathComputation.class);

  /** HOP character */
  private static final String HOP = "-";
  /** default hop string */
  private static final String MAXHOPPATH =
    "------------------------------------------" +
    "---------------------------------------------------------";

/** Max supersteps */
  private static final int MAX_SUPERSTEPS = 30;

  /**
   * Is this vertex the source id?
   * @param vertex vertex object
   * @return True if the source id
   */
  private boolean isSource(Vertex<LongWritable, Text, Text> vertex) {
    return vertex.getId().get() == SOURCE_ID.get(getConf());
  }

  /**
   * Find number of occurrences of substring in string
   * @param string basestring
   * @param subString substring
   * @return num of occurrences
   */

  private int findNumberOfOccurrences(String string, String subString) {
    return string.length() - string.replace(subString, "").length();
  }


  @Override
  public void compute(
      Vertex<LongWritable, Text, Text> vertex,
      Iterable<Text> messages) {
    // superstep0 initialization, also handle dangling nodes case
    if (vertex.getValue().toString().equals("") ||
      getSuperstep() == 0) {
      vertex.setValue(new Text(MAXHOPPATH));
    }

    String minHopPath = isSource(vertex) ? HOP + "1:CRAWLER" : MAXHOPPATH;
    int minDist = isSource(vertex) ?
       1 : findNumberOfOccurrences(MAXHOPPATH, HOP);

    for (Text message : messages) {
      String messageString = message.toString();
      //find number of hops in the received message
      int numHops = findNumberOfOccurrences(messageString, HOP);
      if (numHops < minDist) {
        minDist = numHops;
        minHopPath = messageString;
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Vertex " + vertex.getId() + " got minHopPath = " + minHopPath +
          " vertex value = " + vertex.getValue());
    }
    String currentVertexValueString = vertex.getValue().toString();
    int currentDist = findNumberOfOccurrences(currentVertexValueString, HOP);
    if (minDist < currentDist && getSuperstep() < MAX_SUPERSTEPS)  {
      vertex.setValue(new Text(minHopPath));
      for (Edge<LongWritable, Text> edge : vertex.getEdges()) {
        String path = minHopPath + HOP +
           edge.getTargetVertexId() + ":" + edge.getValue().toString();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Vertex " + vertex.getId() + " sent to " +
              edge.getTargetVertexId() + " = " + path);
        }
        sendMessage(edge.getTargetVertexId(), new Text(path));
      }
    }
    vertex.voteToHalt();
  }
}
