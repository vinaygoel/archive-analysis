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
import org.apache.giraph.conf.FloatConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.log4j.Logger;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.worker.WorkerContext;

/**
 * WeightedPageRank Algorithm
 */
@Algorithm(
    name = "WeightedPageRank",
    description = "WeightedPageRank algorithm - (handles dangling nodes)"
)
public class WeightedPageRankVertex extends
    Vertex<Text, DoubleWritable,
    FloatWritable, DoubleWritable> {

  /** Number of supersteps */
  public static final LongConfOption MAX_SUPERSTEPS =
      new LongConfOption("WeightedPageRankVertex.max_supersteps", 30);

  /** Jump probability */
  public static final FloatConfOption JUMP_PROBABILITY =
      new FloatConfOption("WeightedPageRankVertex.jump_probability", 0.15f);

  //public static final int MAX_SUPERSTEPS = 30;
  //public static final float JUMP_PROBABILITY = 0.15f;
  
  /** Logger */
  private static final Logger LOG =
      Logger.getLogger(WeightedPageRankVertex.class);

  /** Sum aggregator names */
  private static String DANGLING_SUM_AGG = "danglingsum";
  private static String NUMVERTICES_SUM_AGG = "numvertices";

  public void compute(Iterable<DoubleWritable> messages) {
    if (getSuperstep() >= 1) {
      double sum = 0;

      for (DoubleWritable message : messages) {
        sum += message.get();
      }
      // add in the dangling factor
      sum+=this.<DoubleWritable>getAggregatedValue(DANGLING_SUM_AGG).get();
      float jump_probability = JUMP_PROBABILITY.get(getConf());
      DoubleWritable vertexValue = new DoubleWritable(jump_probability + (1-jump_probability) * sum);
      setValue(vertexValue);
      aggregate(NUMVERTICES_SUM_AGG, new LongWritable(1));
    }

    if (getSuperstep() < MAX_SUPERSTEPS.get(getConf())) {
      long edges = getNumEdges();
      double vertexValue = getValue().get();
      //dangling nodes -- transfer score evenly to all nodes
      if(0 == edges)
         aggregate(DANGLING_SUM_AGG, new DoubleWritable(vertexValue / getTotalNumVertices()));
      else {
	 //Pass 1: Sum up all neighbor weights
         float totalEdgeWeight = 0;
	 for(Edge<Text, FloatWritable> edge : getEdges())
	    totalEdgeWeight+=edge.getValue().get(); 
	 //Pass 2: send weighted PR value to each neighbor 
	 if(totalEdgeWeight > 0) { 
	    for(Edge<Text, FloatWritable> edge : getEdges()) {
	       sendMessage(edge.getTargetVertexId(), new DoubleWritable((vertexValue * edge.getValue().get()) / totalEdgeWeight));
	    }
         }
      } 
    } else {
      voteToHalt();
    }
  }

  /**
   * Worker context used with {@link WeightedPageRankVertex}.
   */
  public static class WeightedPageRankVertexWorkerContext extends
      WorkerContext {
    /** Final sum value for verification for local jobs */
    private static long FINAL_SUM;

    public static long getFinalSum() {
      return FINAL_SUM;
    }

    @Override
    public void preApplication()
      throws InstantiationException, IllegalAccessException {
    }

    @Override
    public void postApplication() {
      FINAL_SUM = this.<LongWritable>getAggregatedValue(NUMVERTICES_SUM_AGG).get();
      LOG.info("aggregatedNumVertices=" + FINAL_SUM);
    }

    @Override
    public void preSuperstep() {
      if (getSuperstep() >= 3) {
        LOG.info("aggregatedNumVertices=" +
            getAggregatedValue(NUMVERTICES_SUM_AGG) +
            " NumVertices=" + getTotalNumVertices());
        if (this.<LongWritable>getAggregatedValue(NUMVERTICES_SUM_AGG).get() !=
            getTotalNumVertices()) {
          throw new RuntimeException("wrong value of SumAggregator(numvertices): " +
              getAggregatedValue(NUMVERTICES_SUM_AGG) + ", should be: " +
              getTotalNumVertices());
        }
      }
    }

    @Override
    public void postSuperstep() { }
  }

  /**
   * Master compute associated with {@link WeightedPageRankVertex}.
   * It registers required aggregators.
   */
  public static class WeightedPageRankVertexMasterCompute extends
      DefaultMasterCompute {
    @Override
    public void initialize() throws InstantiationException,
        IllegalAccessException {
      registerAggregator(DANGLING_SUM_AGG, DoubleSumAggregator.class);
      registerAggregator(NUMVERTICES_SUM_AGG, LongSumAggregator.class);
    }
  }
}
