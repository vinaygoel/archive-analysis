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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.giraph.graph.Vertex;


/**
 * Simple function to return the out degree for each vertex.
 */
@Algorithm(
    name = "Outdegree Count for vertices with text value"
)
public class OutDegreeCountComputationVertexWithTextValue
  extends BasicComputation<LongWritable, Text, Text, Text> {

  @Override
  public void compute(
      Vertex<LongWritable, Text, Text> vertex,
      Iterable<Text> messages) {
    Text vertexValue = vertex.getValue();
    vertexValue.set(Double.toString(vertex.getNumEdges()));
    vertex.setValue(vertexValue);
    vertex.voteToHalt();
  }
}
