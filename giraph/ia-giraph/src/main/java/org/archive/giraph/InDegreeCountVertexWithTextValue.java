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

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * Simple function to return the in degree for each vertex.
 */
@Algorithm(
    name = "Indegree Count"
)
public class InDegreeCountVertexWithTextValue extends Vertex<
  LongWritable, Text, Text, Text> {

  @Override
  public void compute(Iterable<Text> messages) {
    if (getSuperstep() == 0) {
      Iterable<Edge<LongWritable, Text>> edges = getEdges();
      for (Edge<LongWritable, Text> edge : edges) {
        sendMessage(edge.getTargetVertexId(), new Text("1"));
      }
    } else {
      long sum = 0;
      for (Text message : messages) {
        sum++;
      }
      Text vertexValue = getValue();
      vertexValue.set(Double.toString(sum));
      setValue(vertexValue);
      voteToHalt();
    }
  }
}
