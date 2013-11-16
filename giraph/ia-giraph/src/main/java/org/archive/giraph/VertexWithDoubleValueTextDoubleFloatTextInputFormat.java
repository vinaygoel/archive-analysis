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

import com.google.common.collect.Lists;
import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Text-based {@link org.apache.giraph.io.VertexInputFormat} for
 * weighted graphs with text ids (and vertex weight)
 * Each line consists of: vertex vertex-weight neighbor1:weight1, neighbor2:weight2
 */
public class VertexWithDoubleValueTextDoubleFloatTextInputFormat
    extends TextVertexInputFormat<Text, DoubleWritable,
    FloatWritable>
    implements ImmutableClassesGiraphConfigurable<Text, DoubleWritable,
    FloatWritable, Writable> {
  /** Configuration. */
  private ImmutableClassesGiraphConfiguration<Text, DoubleWritable,
      FloatWritable, Writable> conf;

  @Override
  public TextVertexReader createVertexReader(InputSplit split,
      TaskAttemptContext context)
    throws IOException {
    return new TextDoubleFloatDoubleVertexReader();
  }

  @Override
  public void setConf(ImmutableClassesGiraphConfiguration<Text,
      DoubleWritable, FloatWritable, Writable> configuration) {
    this.conf = configuration;
  }

  @Override
  public ImmutableClassesGiraphConfiguration<Text, DoubleWritable,
      FloatWritable, Writable> getConf() {
    return conf;
  }

  /**
   * Vertex reader associated with
   * {@link VertexWithDoubleValueTextDoubleFloatTextInputFormat}.
   */
  public class TextDoubleFloatDoubleVertexReader extends
    TextVertexInputFormat<Text, DoubleWritable,
        FloatWritable>.TextVertexReader {
    /** Separator of the vertex and neighbors */
    private final Pattern separator = Pattern.compile("[\t ]");

    @Override
    public Vertex<Text, DoubleWritable, FloatWritable, ?>
    getCurrentVertex() throws IOException, InterruptedException {
      Vertex<Text, DoubleWritable, FloatWritable, ?>
        vertex = conf.createVertex();

      String[] tokens =
          separator.split(getRecordReader().getCurrentValue().toString());

      List<Edge<Text, FloatWritable>> edges =
          Lists.newArrayListWithCapacity(tokens.length - 2);

      Text vertexId = new Text(tokens[0]);
      double vertexWeight = Double.parseDouble(tokens[1]);
      
      for (int n = 2; n < tokens.length; n++) {
        String[] splits = tokens[n].split(":");
        if(splits.length == 2) {
          edges.add(EdgeFactory.create(
              new Text(splits[0]),
              new FloatWritable(Float.parseFloat(splits[1]))));
        }
       }
      
      vertex.initialize(vertexId, new DoubleWritable(vertexWeight), edges);
      return vertex;
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
      return getRecordReader().nextKeyValue();
    }
  }
}
