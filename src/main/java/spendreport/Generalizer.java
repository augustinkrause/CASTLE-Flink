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

package spendreport;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

import java.util.ArrayList;

/**
 *
 */
public class Generalizer extends KeyedProcessFunction<Long, Transaction, Alert> {

	private ArrayList<Cluster> clusters;
	private double globLowerBound = Double.POSITIVE_INFINITY;
	private double globUpperBound = Double.NEGATIVE_INFINITY;

	private long delayConstraint;
	private double threshold;
	private int k;

	final OutputTag<GeneralizedTransaction> outputTag;

	public Generalizer(int k, long delayConstraint, double threshold, OutputTag<GeneralizedTransaction> outputTag){
		this.clusters = new ArrayList<Cluster>();
		this.delayConstraint = delayConstraint;
		this.threshold = threshold;
		this.k = k;
		this.outputTag = outputTag;
	}
	// This hook is executed before the processing starts, kind of as a set-up
	@Override
	public void open(Configuration parameters){

	}

	// This hook is executed on each element of the data stream
	@Override
	public void processElement(
			Transaction transaction,
			Context context,
			Collector<Alert> collector) throws Exception {

		//update global bounds
		if(this.globLowerBound > transaction.getAmount()) this.globLowerBound = transaction.getAmount();
		if(this.globUpperBound < transaction.getAmount()) this.globUpperBound = transaction.getAmount();

		//Add tuple to best-fitting cluster or create new one
		if(this.clusters.size() == 0){
			Cluster c = new Cluster(transaction);
			this.clusters.add(c);
		}else{
			//find the cluster with lowest information loss due to enlargement with new tuple
			double minimum = Double.POSITIVE_INFINITY;
			int minIndex = 0;
			for(int i = 0; i < this.clusters.size(); i++){
				if(this.clusters.get(i).lossDueToEnlargement(transaction, globLowerBound, globUpperBound) < minimum){
					minimum = this.clusters.get(i).lossDueToEnlargement(transaction, globLowerBound, globUpperBound);
					minIndex = i;
				}
			}

			//only add the new element to the found cluster if it satisfies our information loss constraint
			//otherwise create a new cluster around it
			if(this.clusters.get(minIndex).testEnlargement(transaction, threshold, globLowerBound, globUpperBound)){
				this.clusters.get(minIndex).addTuple(transaction);
			}else{
				Cluster c = new Cluster(transaction);
				this.clusters.add(c);
			}
		}

		//release tuples that are older than the delay constraint and their corresponding clusters
		ArrayList<Cluster> newClusters = new ArrayList<Cluster>();
		//since there are multiple cases where "release" removes clusters from the cluster list, we can't use an ordinary loop
		while(!this.clusters.isEmpty()){
			if(clusters.get(0).tuples.peek()._2.longValue() + this.delayConstraint <= System.currentTimeMillis()){
				this.release(this.clusters.get(0), context);
			}else{
				newClusters.add(this.clusters.get(0));
				this.clusters.remove(0);
			}
		}
		//instead we remove all the clusters after they have been processed in one iteration and in the end only add the ones that are kept back in
		this.clusters = newClusters;
	}

	//releases a cluster and if necessary k-anonymizes it first
	private void release(Cluster cluster, Context ctx){

		System.out.println(cluster);
		while(cluster.tuples.size() < this.k && this.clusters.size() > 1){
			//merge with cluster that requires minimal enlargement
			double minimum = Double.POSITIVE_INFINITY;
			int minIndex = 0;
			for(int i = 1; i < this.clusters.size(); i++){
				if(cluster.lossDueToMerge(this.clusters.get(i), globLowerBound, globUpperBound) < minimum){
					minimum = cluster.lossDueToMerge(this.clusters.get(i), globLowerBound, globUpperBound);
					minIndex = i;
				}
			}
			cluster.merge(this.clusters.get(minIndex));
			this.clusters.remove(minIndex);

		}
		/*
		while(cluster.tuples.size() >= 2 * this.k){
			//split the cluster
		}*/

		System.out.println(cluster);
		//release all the created clusters
		while(!cluster.tuples.isEmpty()){
			GeneralizedTransaction newTransaction = new GeneralizedTransaction(cluster.tuples.poll()._1, cluster.lowerBound, cluster.upperBound);
			//ctx.output(outputTag, newTransaction);
			//System.out.println(newTransaction);
		}
		this.clusters.remove(0);
	}

}
