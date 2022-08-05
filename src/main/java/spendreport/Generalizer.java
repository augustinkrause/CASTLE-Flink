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
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.*;

/**
 *
 */
public class Generalizer extends ProcessFunction<Tuple2<Tuple, Long>, Tuple> implements Serializable {

	private ArrayList<Cluster> clusters;
	private ArrayList<Cluster> reuseClusters; //these clusters will have been previously published and thus at some point have been k-anonymous
	private double[] globLowerBounds;
	private double[] globUpperBounds;

	private long delayConstraint;
	private double threshold;
	private int k;

	private int[] keys;

	public Generalizer(int k, long delayConstraint, double threshold, int[] keys){
		this.clusters = new ArrayList<>();
		this.reuseClusters = new ArrayList<>();
		this.delayConstraint = delayConstraint; //when a new element comes in, any tuple older than this constraint should be released
		this.threshold = threshold;	//the aim is to not create clusters with more information loss than this
		this.k = k;
		this.keys = keys;

		//initialize one global bound per key
		this.globLowerBounds = new double[keys.length];
		this.globUpperBounds = new double[keys.length];
		for(int i = 0; i < keys.length; i++){
			this.globLowerBounds[i] = Double.POSITIVE_INFINITY;
			this.globUpperBounds[i] = Double.NEGATIVE_INFINITY;
		}
	}
	// This hook is executed before the processing starts, kind of as a set-up
	@Override
	public void open(Configuration parameters){

	}

	// This hook is executed on each element of the data stream
	@Override
	public void processElement(
			Tuple2<Tuple, Long> element,
			Context context,
			Collector<Tuple> collector) throws Exception {
		
		//update ALL global bounds
		for(int i = 0; i < this.keys.length; i++){
			//update global bounds (upper and lower bound over the whole processing period)
			if(this.globLowerBounds[i] > ((Number) element.f0.getField(this.keys[i])).doubleValue()) this.globLowerBounds[i] = ((Number) element.f0.getField(this.keys[i])).doubleValue();
			if(this.globUpperBounds[i] < ((Number) element.f0.getField(this.keys[i])).doubleValue()) this.globUpperBounds[i] = ((Number) element.f0.getField(this.keys[i])).doubleValue();
		}

		//Add tuple to best-fitting cluster or create new one
		if(this.clusters.size() == 0){
			Cluster c = new Cluster(element, this.keys);
			this.clusters.add(c);
		}else{
			//find the cluster with lowest information loss due to enlargement with new tuple
			double minimum = Double.POSITIVE_INFINITY;
			int minIndex = 0;
			for(int i = 0; i < this.clusters.size(); i++){
				double lossDueToEnlargement = this.clusters.get(i).lossDueToEnlargement(element.f0, this.globLowerBounds, this.globUpperBounds);
				if(lossDueToEnlargement < minimum){
					minimum = lossDueToEnlargement;
					minIndex = i;
				}
			}

			//only add the new element to the found cluster if it satisfies our information loss constraint
			//otherwise create a new cluster around it
			if(this.clusters.get(minIndex).testEnlargement(element.f0, this.threshold, this.globLowerBounds, this.globUpperBounds)){
				this.clusters.get(minIndex).addTuple(element, this.globLowerBounds, this.globUpperBounds);
			}else{
				Cluster c = new Cluster(element, this.keys);
				this.clusters.add(c);
			}
		}

		//release tuples that are older than the delay constraint and their corresponding clusters
		ArrayList<Cluster> newClusters = new ArrayList<>();
		//since there are multiple cases where "release" removes clusters from the cluster list, we can't use an ordinary loop
		while(!this.clusters.isEmpty()){

			if(this.clusters.get(0).elements.peek().f1 + this.delayConstraint <= System.currentTimeMillis()){
				newClusters = this.release(this.clusters.get(0), newClusters, collector);
			}else{
				newClusters.add(this.clusters.get(0));
				this.clusters.remove(0);
			}
		}
		//instead we remove all the clusters after they have been processed in one iteration and in the end only add the ones that are kept back in
		this.clusters = newClusters;
	}

	//releases a cluster and if necessary k-anonymizes it first
	private ArrayList<Cluster> release(Cluster cluster, ArrayList<Cluster> newClusters, Collector<Tuple> collector){

		//check if reuse FOR THE OLDEST TUPLE IN THE CLUSTER is possible (only if the to-be-released cluster is not itself k-anonymous)
		//in this case we know the oldest tuple in the cluster has expired and should be released
		if(cluster.elements.size() < this.k){
			//find fitting reuseClusters
			ArrayList<Cluster> fittingReuseClusters = new ArrayList<>();

			for(int i = 0; i < this.reuseClusters.size(); i++){
				if(this.reuseClusters.get(i).fits(cluster.elements.peek().f0)) fittingReuseClusters.add(this.reuseClusters.get(i));
			}
			if(fittingReuseClusters.size() > 0){
				//randomly select reuseCluster
				int index = new Random().nextInt(fittingReuseClusters.size());
				Tuple generalizedElement = fittingReuseClusters.get(index).generalize(cluster.elements.poll().f0);
				collector.collect(generalizedElement);

				if(cluster.elements.size() == 0) this.clusters.remove(0);

				System.out.print("Reused: ");
				System.out.println(fittingReuseClusters.get(index));
				//we now return just the old list of already processed clusters and don't remove any cluster, since no whole cluster has been released
				return newClusters;
			}
		}

		while(cluster.elements.size() < this.k && this.clusters.size() > 1){
			//in this case the cluster is not yet "k-anonymous"
			//merge with cluster that requires minimal enlargement
			double minimum = Double.POSITIVE_INFINITY;
			int minIndex = 0;
			boolean minNewCluster = false; //indicates if the minIndex is relative to this.clusters or newClusters
			for(int i = 1; i < this.clusters.size(); i++){
				if(cluster.lossDueToMerge(this.clusters.get(i), this.globLowerBounds, this.globUpperBounds) < minimum){
					minimum = cluster.lossDueToMerge(this.clusters.get(i), this.globLowerBounds, this.globUpperBounds);
					minIndex = i;
				}
			}
			//we need to loop through previously processed clusters as well
			for(int i = 0; i < newClusters.size(); i++){
				if(cluster.lossDueToMerge(newClusters.get(i), this.globLowerBounds, this.globUpperBounds) < minimum){
					minimum = cluster.lossDueToMerge(newClusters.get(i), this.globLowerBounds, this.globUpperBounds);
					minIndex = i;
					minNewCluster = true;
				}
			}
			if(minNewCluster){
				cluster.merge(newClusters.get(minIndex), this.globLowerBounds, this.globUpperBounds);
				newClusters.remove(minIndex); //since we merged the two clusters, they both in the end need to get removed from our cluster list
			}else{
				cluster.merge(this.clusters.get(minIndex), this.globLowerBounds, this.globUpperBounds);
				this.clusters.remove(minIndex); //since we merged the two clusters, they both in the end need to get removed from our cluster list
			}

		}

		//splits become possible if the cluster is of size at least 2k
		ArrayList<Cluster> splitClusters;
		if(cluster.elements.size() >= 2 * this.k){
			//split the cluster
			splitClusters = this.split(cluster);
		}else{
			//if it doesn't get split we just create a 1-element list of clusters to be released
			splitClusters = new ArrayList<>();
			splitClusters.add(cluster);
		}

		//release all the created clusters
		for(Cluster c : splitClusters){

			while(!c.elements.isEmpty()){
				Tuple generalizedElement = c.generalize(c.elements.poll().f0);

				//output the generalized element
				System.out.println(generalizedElement);
				collector.collect(generalizedElement);
			}
			this.reuseClusters.add(c); // buffer EMPTY cluster for reuse
		}

		//we always process the first cluster in the cluster list -> that cluster will have definitely been released and thus needs to be removed
		this.clusters.remove(0);
		return newClusters;
	}

	//this performs the splitting specified in the CASTLE algorithm WITHOUT adhering to l-diversity principle
	//it is a variant of a KNN algorithm
	public ArrayList<Cluster> split(Cluster c){
		ArrayList<Cluster> newClusters = new ArrayList<>(); //will hold all the newly generated clusters

		while(c.elements.size() >= this.k){
			int index = new Random().nextInt(c.elements.size());
			Tuple2<Tuple, Long>[] elementsArray = new Tuple2[c.elements.size()];
			c.elements.toArray(elementsArray);
			Tuple2<Tuple, Long> t = elementsArray[index];
			Cluster newCluster = new Cluster(t, this.keys); //form a new cluster over the randomly picked element

			//find k-1 NNs
			PriorityQueue<Tuple2<Tuple2<Tuple, Long>, Double>> sortedElements = new PriorityQueue<>(new ElementComparator()); //used for sorting by distance to t
			Iterator<Tuple2<Tuple, Long>> it = c.elements.iterator();
			//calculate each element's distance to t
			while(it.hasNext()){
				Tuple2<Tuple, Long> t_i = it.next(); //the transaction along with its timestamp (which is needed later)
				double infoLoss_i = newCluster.lossDueToEnlargement(t_i.f0, this.globLowerBounds, this.globUpperBounds);
				sortedElements.add(new Tuple2<>(t_i, infoLoss_i));
			}
			//pick only the first k-1 elements in sortedElements (since c.size() was >= k we are guaranteed to find k-1 elements)
			for(int counter = 0; counter < this.k - 1; counter++){
				Tuple2<Tuple, Long> t_i = sortedElements.poll().f0;
				newCluster.addTuple(t_i, this.globLowerBounds, this.globUpperBounds);
				c.elements.remove(t_i); //the current element has been assigned to a new cluster, so remove it from the old one
			}

			newClusters.add(newCluster);
		}

		//add remaining elements to their respective cluster that requires minimal enlargement
		while(!c.elements.isEmpty()){
			Cluster minC = newClusters.get(0);
			for(Cluster c_i : newClusters){
				if(c_i.lossDueToEnlargement(c.elements.peek().f0, this.globLowerBounds, this.globUpperBounds) < minC.lossDueToEnlargement(c.elements.peek().f0, this.globLowerBounds, this.globUpperBounds)){
					minC = c_i;
				}
			}
			minC.addTuple(c.elements.poll(), this.globLowerBounds, this.globUpperBounds);
		}

		return newClusters;
	}

	//needed for sorting the groups that are created in "split"
	static class ElementComparator implements Comparator<Tuple2<?, Double>> {

		public int compare(Tuple2<?, Double> o1, Tuple2<?, Double> o2) {
			return o1.f1.doubleValue() > o2.f1.doubleValue() ? 1 : -1;
		}
	}

}
