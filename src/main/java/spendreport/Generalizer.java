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
import scala.Tuple2;

import java.util.*;

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
		this.delayConstraint = delayConstraint; //when a new element comes in, any tuple older than this constraint should be released
		this.threshold = threshold;	//the aim is to not create clusters with more information loss than this
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

		//update global bounds (upper and lower bound over the whole processing period)
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

		while(cluster.tuples.size() < this.k && this.clusters.size() > 1){
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
		ArrayList<Cluster> splitClusters;
		if(cluster.tuples.size() >= 2 * this.k){
			//split the cluster
			splitClusters = this.split(cluster);
		}else{
			//if it doesn't get split we just create a 1-element list of clusters to be released
			splitClusters = new ArrayList<>();
			splitClusters.add(cluster);
		}
		System.out.println(splitClusters);

		//release all the created clusters
		for(Cluster c : splitClusters){
			while(!c.tuples.isEmpty()){
				GeneralizedTransaction newTransaction = new GeneralizedTransaction(c.tuples.poll()._1, c.lowerBound, c.upperBound);
				//ctx.output(outputTag, newTransaction);
				//System.out.println(newTransaction);
			}
		}

		//we always process the first cluster in the cluster list -> that cluster will have definitely been released and thus needs to be removed
		//TODO: Cluster reuse
		this.clusters.remove(0);
	}

	//this performs the splitting specified in the CASTLE algorithm
	//it is a variant of a KNN algorithm
	public ArrayList<Cluster> split(Cluster c){
		ArrayList<Cluster> newClusters = new ArrayList<Cluster>(); //will hold all the newly generated clusters

		while(c.tuples.size() >= k){
			Transaction t = c.tuples.poll()._1; //TODO: In the original Algorithm they select a tuple randomly
			Cluster newCluster = new Cluster(t); //form a new cluster over the randomly picked element

			//this hash map will contain the so called groups
			//in the algorithm it is assumed, that all elements with the same pid (i.e. accountid) have the same distance to t
			//these elements form a group, along with each group (keyed by accountid) we store their distance to t
			HashMap<Long, Tuple2<ArrayList<Tuple2<Transaction, Long>>, Double>> groups = new HashMap<>();

			Iterator<Tuple2<Transaction, Long>> it = c.tuples.iterator();
			//calculate each groups distance to t
			while(it.hasNext()){
				Tuple2<Transaction, Long> t_i = it.next(); //the transaction along with its timestamp (which is needed later)
				if(groups.containsKey(t_i._1.getAccountId())){
					//in this case don't update the loss, as its the same for all tuples in a group
					Tuple2<ArrayList<Tuple2<Transaction, Long>>, Double> group_i = groups.get(t_i._1.getAccountId());
					group_i._1.add(t_i); //we only need t add the <Transaction, Timestamp> to the group
					groups.put(t_i._1.getAccountId(), group_i);
				}else{
					//initialize the group
					double loss_i = newCluster.lossDueToEnlargement(t_i._1, this.globLowerBound, this.globUpperBound);
					ArrayList<Tuple2<Transaction, Long>> ts = new ArrayList<>(); //these are the <Transaction, Timestamp> Tuples
					ts.add(t_i);
					Tuple2<ArrayList<Tuple2<Transaction, Long>>, Double> group_i = new Tuple2<>(ts, loss_i); //store the (for now) 1-element list + calculated loss
					groups.put(t_i._1.getAccountId(), group_i);
				}
			}

			//sort by loss
			//this PriorityQueue is used for sorting and has the same "Typing" as the previous HashMap
			PriorityQueue<Tuple2<ArrayList<Tuple2<Transaction, Long>>, Double>> sortedGroups = new PriorityQueue<>(new GroupComparator());
			for (Long i : groups.keySet()) {
				//By just adding all the groups into the PQ one after another they get sorted
				Tuple2<ArrayList<Tuple2<Transaction, Long>>, Double> group_i = groups.get(i);
				sortedGroups.add(group_i);
			}

			//add k-1 tuples to new_cluster
			//this is different to the specified CASTLE-algorithm, since there it is implicitly assumed we have k-1 groups at least
			while(newCluster.tuples.size() < k){
				//we need to check all elements of the sortedGroups PQ, for this we remove every group and store some of them in this "new" PQ
				PriorityQueue<Tuple2<ArrayList<Tuple2<Transaction, Long>>, Double>> newSortedGroups = new PriorityQueue<>(new GroupComparator());
				//loop through all groups
				while(!sortedGroups.isEmpty()){
					Tuple2<ArrayList<Tuple2<Transaction, Long>>, Double> group_i = sortedGroups.poll();
					//we always take one tuple from each group, in the order of ascending distance to t
					//during this process some groups might become completely empty
					if(!group_i._1.isEmpty()){
						Tuple2<Transaction, Long> t_i = group_i._1.get(0); //get the element that expires the soonest from the group
						group_i._1.remove(0); //also remove it, since we now add it to a new cluster
						newSortedGroups.add(group_i);
						newCluster.addTuple(t_i._1);
						c.tuples.remove(t_i); //the current element has been assigned to a new cluster, so remove it from the old one
						if(newCluster.tuples.size() == k){
							//at the beginning of the loop we're only guaranteed to have at least k tuples in c, so we NEED to stop here
							break;
						}
					}
				}
				sortedGroups = newSortedGroups;
			}

			newClusters.add(newCluster);
		}

		//add remaining elements to their respective cluster that requires minimal enlargement
		while(!c.tuples.isEmpty()){
			Cluster minC = newClusters.get(0);
			for(Cluster c_i : newClusters){
				if(c_i.lossDueToEnlargement(c.tuples.peek()._1, this.globLowerBound, this.globUpperBound) < minC.lossDueToEnlargement(c.tuples.peek()._1, this.globLowerBound, this.globUpperBound)){
					minC = c_i;
				}
			}
			minC.addTuple(c.tuples.poll()._1);
		}

		return newClusters;
	}

	//needed for sorting the groups that are created in "split"
	static class GroupComparator implements Comparator<Tuple2<ArrayList<Tuple2<Transaction, Long>>, Double>> {

		public int compare(Tuple2<ArrayList<Tuple2<Transaction, Long>>, Double> o1, Tuple2<ArrayList<Tuple2<Transaction, Long>>, Double> o2) {
			return o1._2.doubleValue() > o2._2.doubleValue() ? 1 : -1;
		}
	}

}
