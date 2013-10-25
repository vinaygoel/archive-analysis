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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Simple text-based {@link org.apache.giraph.io.VertexInputFormat} for
 * text-labeled graphs with long ids
 * Each line consists of: vertex vertexLabel neighbor1:label
 * neighbor2:label ...
 */
public class VertexWithTextValueLongTextTextTextInputFormat
    extends TextVertexInputFormat<LongWritable, Text,
    Text>
    implements ImmutableClassesGiraphConfigurable<LongWritable, Text,
    Text, Writable> {
  /** Configuration. */
  private ImmutableClassesGiraphConfiguration<LongWritable, Text,
      Text, Writable> conf;

  @Override
  public TextVertexReader createVertexReader(InputSplit split,
      TaskAttemptContext context)
    throws IOException {
    return new LongTextTextTextVertexReader();
  }

  @Override
  public void setConf(ImmutableClassesGiraphConfiguration<LongWritable,
      Text, Text, Writable> configuration) {
    this.conf = configuration;
  }

  @Override
  public ImmutableClassesGiraphConfiguration<LongWritable, Text,
      Text, Writable> getConf() {
    return conf;
  }

  /**
   * Vertex reader associated with
   * {@link VertexWithTextValueLongTextTextTextInputFormat}.
   */
  public class LongTextTextTextVertexReader extends
    TextVertexInputFormat<LongWritable, Text,
        Text>.TextVertexReader {
    /** Separator of the vertex and neighbors */
    private final Pattern separator = Pattern.compile("[\t ]");
    
    @Override
    public Vertex<LongWritable, Text, Text, ?>
    getCurrentVertex() throws IOException, InterruptedException {
      Vertex<LongWritable, Text, Text, ?>
        vertex = conf.createVertex();

      String[] tokens =
          separator.split(getRecordReader().getCurrentValue().toString());

      //double vertexWeight = Double.parseDouble(tokens[1]);
      Text vertexWeight = new Text(tokens[1]);

      List<Edge<LongWritable, Text>> edges =
          Lists.newArrayListWithCapacity(tokens.length - 2);

      //float weight = 1.0f / (tokens.length - 1);
      for (int n = 2; n < tokens.length; n++) {
        String[] splits = tokens[n].split(":");
        edges.add(EdgeFactory.create(
            new LongWritable(Long.parseLong(splits[0])),
            new Text(splits[1])));
      }

      LongWritable vertexId = new LongWritable(Long.parseLong(tokens[0]));
      vertex.initialize(vertexId, vertexWeight, edges);

      return vertex;
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
      return getRecordReader().nextKeyValue();
    }
  }
}
