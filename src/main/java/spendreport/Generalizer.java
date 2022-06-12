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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import scala.Tuple2;

import java.util.*;

/**
 *
 */
public class Generalizer<T> extends KeyedProcessFunction<Long, T, Alert> {

	private ArrayList<Cluster<T>> clusters;
	private double globLowerBound = Double.POSITIVE_INFINITY;
	private double globUpperBound = Double.NEGATIVE_INFINITY;

	private long delayConstraint;
	private double threshold;
	private int k;

	private String methodName;

	public Generalizer(int k, long delayConstraint, double threshold, String methodName){
		this.clusters = new ArrayList<>();
		this.delayConstraint = delayConstraint; //when a new element comes in, any tuple older than this constraint should be released
		this.threshold = threshold;	//the aim is to not create clusters with more information loss than this
		this.k = k;
		this.methodName = methodName;
	}
	// This hook is executed before the processing starts, kind of as a set-up
	@Override
	public void open(Configuration parameters){

	}

	// This hook is executed on each element of the data stream
	@Override
	public void processElement(
			T element,
			Context context,
			Collector<Alert> collector) throws Exception {

		System.out.println(element.getClass().getMethod("getAmount"));
		System.out.println(element.getClass().getMethod("getAmount").invoke(element));

		//update global bounds (upper and lower bound over the whole processing period)
		if(this.globLowerBound > ((Number) element.getClass().getMethod(this.methodName).invoke(element)).doubleValue()) this.globLowerBound = ((Number) element.getClass().getMethod(this.methodName).invoke(element)).doubleValue();
		if(this.globUpperBound < ((Number) element.getClass().getMethod(this.methodName).invoke(element)).doubleValue()) this.globUpperBound = ((Number) element.getClass().getMethod(this.methodName).invoke(element)).doubleValue();

		//Add tuple to best-fitting cluster or create new one
		if(this.clusters.size() == 0){
			Cluster c = new Cluster<T>(element, this.methodName);
			this.clusters.add(c);
		}else{
			//find the cluster with lowest information loss due to enlargement with new tuple
			double minimum = Double.POSITIVE_INFINITY;
			int minIndex = 0;
			for(int i = 0; i < this.clusters.size(); i++){
				if(this.clusters.get(i).lossDueToEnlargement(element, globLowerBound, globUpperBound) < minimum){
					minimum = this.clusters.get(i).lossDueToEnlargement(element, globLowerBound, globUpperBound);
					minIndex = i;
				}
			}

			//only add the new element to the found cluster if it satisfies our information loss constraint
			//otherwise create a new cluster around it
			if(this.clusters.get(minIndex).testEnlargement(element, threshold, globLowerBound, globUpperBound)){
				this.clusters.get(minIndex).addTuple(element);
			}else{
				Cluster c = new Cluster<T>(element, this.methodName);
				this.clusters.add(c);
			}
		}

		//release tuples that are older than the delay constraint and their corresponding clusters
		ArrayList<Cluster<T>> newClusters = new ArrayList<>();
		//since there are multiple cases where "release" removes clusters from the cluster list, we can't use an ordinary loop
		while(!this.clusters.isEmpty()){
			if(this.clusters.get(0).elements.peek()._2.longValue() + this.delayConstraint <= System.currentTimeMillis()){
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
	private void release(Cluster<T> cluster, Context ctx){

		while(cluster.elements.size() < this.k && this.clusters.size() > 1){
			//in this case the cluster is not yet "k-anonymous"
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
			this.clusters.remove(minIndex); //since we merged the two clusters, they both in the end need to get removed from our cluster list

		}

		//splits become possible if the cluster is of size at least 2k
		ArrayList<Cluster<T>> splitClusters;
		if(cluster.elements.size() >= 2 * this.k){
			//split the cluster
			splitClusters = this.split(cluster);
		}else{
			//if it doesn't get split we just create a 1-element list of clusters to be released
			splitClusters = new ArrayList<>();
			splitClusters.add(cluster);
		}

		//release all the created clusters
		for(Cluster<T> c : splitClusters){
			while(!c.elements.isEmpty()){
				//GeneralizedTransaction newTransaction = new GeneralizedTransaction(c.elements.poll()._1, c.lowerBound, c.upperBound);
				//ctx.output(outputTag, newTransaction);
				//System.out.println(newTransaction);
			}
		}

		//we always process the first cluster in the cluster list -> that cluster will have definitely been released and thus needs to be removed
		//TODO: Cluster reuse
		this.clusters.remove(0);
	}

	//this performs the splitting specified in the CASTLE algorithm WITHOUT adhering to l-diversity principle
	//it is a variant of a KNN algorithm
	public ArrayList<Cluster<T>> split(Cluster<T> c){
		ArrayList<Cluster<T>> newClusters = new ArrayList<>(); //will hold all the newly generated clusters

		while(c.elements.size() >= this.k){
			T t = c.elements.poll()._1; //TODO: In the original Algorithm they select a tuple randomly
			Cluster<T> newCluster = new Cluster<T>(t, this.methodName); //form a new cluster over the randomly picked element

			//find k-1 NNs
			PriorityQueue<Tuple2<Tuple2<T, Long>, Double>> sortedElements = new PriorityQueue<>(new ElementComparator()); //used for sorting by distance to t
			Iterator<Tuple2<T, Long>> it = c.elements.iterator();
			//calculate each element's distance to t
			while(it.hasNext()){
				Tuple2<T, Long> t_i = it.next(); //the transaction along with its timestamp (which is needed later)
				double infoLoss_i = newCluster.lossDueToEnlargement(t_i._1, this.globLowerBound, this.globUpperBound);
				sortedElements.add(new Tuple2<>(t_i, infoLoss_i));
			}
			//pick only the first k-1 elements in sortedElements (since c.size() was >= k we are guaranteed to find k-1 elements)
			for(int counter = 0; counter < this.k - 1; counter++){
				Tuple2<T, Long> t_i = sortedElements.poll()._1;
				newCluster.addTuple(t_i._1);
				c.elements.remove(t_i); //the current element has been assigned to a new cluster, so remove it from the old one
			}

			newClusters.add(newCluster);
		}

		//add remaining elements to their respective cluster that requires minimal enlargement
		while(!c.elements.isEmpty()){
			Cluster minC = newClusters.get(0);
			for(Cluster c_i : newClusters){
				if(c_i.lossDueToEnlargement(c.elements.peek()._1, this.globLowerBound, this.globUpperBound) < minC.lossDueToEnlargement(c.elements.peek()._1, this.globLowerBound, this.globUpperBound)){
					minC = c_i;
				}
			}
			minC.addTuple(c.elements.poll()._1);
		}

		return newClusters;
	}

	//needed for sorting the groups that are created in "split"
	static class ElementComparator implements Comparator<Tuple2<?, Double>> {

		public int compare(Tuple2<?, Double> o1, Tuple2<?, Double> o2) {
			return o1._2.doubleValue() > o2._2.doubleValue() ? 1 : -1;
		}
	}

}
