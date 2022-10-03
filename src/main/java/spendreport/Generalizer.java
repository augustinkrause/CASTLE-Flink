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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.*;

/**
 *
 */
public class Generalizer extends ProcessFunction<Tuple2<Tuple, Long>, Tuple> implements Serializable, ResultTypeQueryable {

	private ArrayList<Cluster> clusters;
	private PriorityQueue<Cluster> reuseClusters; //these clusters will have been previously published and thus at some point have been k-anonymous
	private double[] globLowerBounds;
	private double[] globUpperBounds;

	private long delayConstraint;
	private double threshold;
	private int k;
	private int mu;
	private int maxClusters;

	private int[] keys;

	private Collector<Tuple> collector; //TODO: For now only a test
	private TypeInformation[] types;

	public Generalizer(int k, long delayConstraint, int mu, int maxClusters, int[] keys, TypeInformation[] types){
		this.clusters = new ArrayList<>();
		this.reuseClusters = new PriorityQueue<>(new ClusterComparator());
		this.delayConstraint = delayConstraint; //when a new element comes in, any tuple older than this constraint should be released
		this.threshold = 0;	//the aim is to not create clusters with more information loss than this
		this.k = k;
		this.keys = keys;
		this.mu = mu;
		this.maxClusters = maxClusters;

		//initialize one global bound per key
		this.globLowerBounds = new double[keys.length];
		this.globUpperBounds = new double[keys.length];
		for(int i = 0; i < keys.length; i++){
			this.globLowerBounds[i] = Double.POSITIVE_INFINITY;
			this.globUpperBounds[i] = Double.NEGATIVE_INFINITY;
		}

		this.types = types;
	}
	// This hook is executed before the processing starts, kind of as a set-up
	@Override
	public void open(Configuration parameters){

	}

	@Override
	public void close(){
		ArrayList<Cluster> newClusters = new ArrayList<>();

		//release remaining clusters
		while(!this.clusters.isEmpty()){
			newClusters = this.release(this.clusters.get(0), newClusters, this.collector);
		}

	}

	// This hook is executed on each element of the data stream
	@Override
	public void processElement(
			Tuple2<Tuple, Long> element,
			Context context,
			Collector<Tuple> collector) throws Exception {

		this.collector = collector;
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
			if(this.clusters.get(minIndex).testEnlargement(element.f0, this.threshold, this.globLowerBounds, this.globUpperBounds) || this.clusters.size() >= this.maxClusters){
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

		if(cluster.elements.size() >= k){
			this.publish(cluster, collector);
		}else{

			//check if reuse FOR THE OLDEST TUPLE IN THE CLUSTER is possible (only if the to-be-released cluster is not itself k-anonymous)
			//in this case we know the oldest tuple in the cluster has expired and should be released
			ArrayList<Cluster> fittingReuseClusters = new ArrayList<>();

			Cluster[] reuseClustersArr = new Cluster[this.reuseClusters.size()];
			reuseClustersArr = this.reuseClusters.toArray(reuseClustersArr);
			for(int i = 0; i < reuseClustersArr.length; i++){
				if(reuseClustersArr[i].fits(cluster.elements.peek().f0)) fittingReuseClusters.add(reuseClustersArr[i]);
			}
			if(fittingReuseClusters.size() > 0){
				//randomly select reuseCluster
				int index = new Random().nextInt(fittingReuseClusters.size());
				Tuple generalizedElement = fittingReuseClusters.get(index).generalize(cluster.elements.poll().f0);
				System.out.println(generalizedElement);
				collector.collect(generalizedElement);

				if(cluster.elements.size() == 0) this.clusters.remove(0);

				//we now return just the old list of already processed clusters and don't remove any cluster, since no whole cluster has been released
				return newClusters;
			}

			//release oldest tuple in cluster if it "is an outlier"
			int m = 0;
			int nElements = cluster.elements.size();
			for(int i = 1; i < this.clusters.size(); i++){
				if(cluster.elements.size() < this.clusters.get(i).elements.size()){
					m++;
				}
				nElements += this.clusters.get(i).elements.size();
			}
			if(m > 0.5 * this.clusters.size() || nElements < this.k){
				Tuple generalizedElement = this.suppress(cluster.elements.poll().f0);
				System.out.println(generalizedElement);
				collector.collect(generalizedElement);

				if(cluster.elements.size() == 0) this.clusters.remove(0);

				return newClusters;
			}

			while(cluster.elements.size() < this.k && (this.clusters.size() > 1 || newClusters.size() > 0)){
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

			this.publish(cluster, collector);
		}

		//we always process the first cluster in the cluster list -> that cluster will have definitely been released and thus needs to be removed
		this.clusters.remove(0);
		return newClusters;
	}

	public void publish(Cluster cluster, Collector<Tuple> collector){
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

			boolean kAnonymous = c.elements.size() >= k;
			while(!c.elements.isEmpty()){
				Tuple generalizedElement;
				if(kAnonymous) {
					generalizedElement = c.generalize(c.elements.poll().f0);
				}else{
					generalizedElement = this.suppress(c.elements.poll().f0);
				}

				//output the generalized element
				System.out.println(generalizedElement);
				collector.collect(generalizedElement);
			}

			if(kAnonymous) this.reuseClusters.add(c); // buffer EMPTY, k-anonymous cluster for reuse

			//update adaptive infoloss
			Cluster[] reuseClustersArr = new Cluster[this.reuseClusters.size()];
			double infolossSum = 0;
			int numSummed = 0;
			for(int i = 0; i < reuseClustersArr.length && i < this.mu; i++){
				reuseClustersArr[i] = this.reuseClusters.poll();
				infolossSum += reuseClustersArr[i].oldInfoLoss;
				numSummed++;
			}
			for(int i = 0; i < reuseClustersArr.length && i < this.mu; i++){
				this.reuseClusters.add(reuseClustersArr[i]);
			}
			this.threshold = infolossSum / numSummed;
			if(c.oldInfoLoss >= this.threshold && kAnonymous) this.reuseClusters.remove(c);
		}
	}

	//this performs the splitting specified in the CASTLE algorithm WITHOUT adhering to l-diversity principle
	//it is a variant of a KNN algorithm
	public ArrayList<Cluster> split(Cluster c){
		ArrayList<Cluster> newClusters = new ArrayList<>(); //will hold all the newly generated clusters

		while(c.elements.size() >= this.k){
			int index = new Random().nextInt(c.elements.size());
			Tuple2<Tuple, Long>[] elementsArray = new Tuple2[c.elements.size()];
			elementsArray = c.elements.toArray(elementsArray);
			Tuple2<Tuple, Long> t = elementsArray[index];
			Cluster newCluster = new Cluster(t, this.keys); //form a new cluster over the randomly picked element
			c.elements.remove(t);

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

	//returns a tuple with the global bounds as its entries
	public Tuple suppress(Tuple t){
		Tuple newT = Tuple.newInstance(t.getArity());

		for(int i = 0; i < this.keys.length; i++){
			newT.setField(new Tuple2<>(this.globLowerBounds[i], this.globUpperBounds[i]), this.keys[i]);
		}

		for(int i = 0; i < t.getArity(); i++){
			if(newT.getField(i) == null) newT.setField(t.getField(i) ,i);
		}

		return newT;
	}

	@Override
	public TypeInformation getProducedType() {
		TypeInformation[] outputTypes = new TypeInformation[this.types.length];
		for(int i = 0; i < this.keys.length; i++){
			outputTypes[this.keys[i]] = Types.TUPLE(Types.DOUBLE, Types.DOUBLE);
		}
		for(int i = 0; i < this.types.length; i++){
			if(outputTypes[i] == null) outputTypes[i] = this.types[i];
		}
		return Types.TUPLE(outputTypes);
	}

	//readObject, writeObject, readObjectNoData are functions that are called during serialization of instances of this class
	private void writeObject(java.io.ObjectOutputStream out) throws IOException {

		out.writeObject(this.clusters);
		out.writeObject(this.reuseClusters);

		out.writeInt(this.globLowerBounds.length);
		for(int i = 0; i < this.globLowerBounds.length; i++){
			out.writeDouble(this.globLowerBounds[i]);
		}
		out.writeInt(this.globUpperBounds.length);
		for(int i = 0; i < this.globUpperBounds.length; i++){
			out.writeDouble(this.globUpperBounds[i]);
		}

		out.writeLong(this.delayConstraint);
		out.writeDouble(this.threshold);
		out.writeInt(this.k);
		out.writeInt(this.mu);
		out.writeInt(this.maxClusters);

		out.writeInt(this.keys.length);
		for(int i = 0; i < this.keys.length; i++){
			out.writeInt(this.keys[i]);
		}
	}

	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
		this.clusters = (ArrayList<Cluster>) in.readObject();
		this.reuseClusters = (PriorityQueue<Cluster>) in.readObject();

		int len = in.readInt();
		this.globLowerBounds = new double[len];
		for(int i = 0; i < len; i++){
			this.globLowerBounds[i] = in.readDouble();
		}
		len = in.readInt();
		this.globUpperBounds = new double[len];
		for(int i = 0; i < len; i++){
			this.globUpperBounds[i] = in.readDouble();
		}

		this.delayConstraint = in.readLong();
		this.threshold = in.readDouble();
		this.k = in.readInt();
		this.mu = in.readInt();
		this.maxClusters = in.readInt();

		len = in.readInt();
		this.keys = new int[len];
		for(int i = 0; i < len; i++){
			this.keys[i] = in.readInt();
		}
	}

	private void readObjectNoData() throws ObjectStreamException {
		throw new NotSerializableException("Serialization error in class " + this.getClass().getName());
	}

	//needed for sorting the groups that are created in "split"
	static class ElementComparator implements Comparator<Tuple2<?, Double>>, Serializable {

		public int compare(Tuple2<?, Double> o1, Tuple2<?, Double> o2) {
			return o1.f1.doubleValue() > o2.f1.doubleValue() ? 1 : -1;
		}
	}

	static class ClusterComparator implements Comparator<Cluster>, Serializable{

		public int compare(Cluster c1, Cluster c2) {
			return c1.timestamp > c2.timestamp ? 1 : -1;
		}
	}
}
