package spendreport;

import org.apache.flink.walkthrough.common.entity.Transaction;
import scala.Tuple2;
import sun.tools.jconsole.JConsole;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;

public class Cluster {

    double lowerBound;
    double upperBound;

    //we use a priority queue to easily check wether the oldest tuple of this cluster needs to be released
    PriorityQueue<Tuple2<Transaction, Long>> tuples;

    public Cluster(double lowerBound, double upperBound){
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;

        this.tuples = new PriorityQueue<Tuple2<Transaction, Long>>(new TransactionComparator());
    }

    public Cluster(Transaction tuple){
        this.lowerBound = tuple.getAmount();
        this.upperBound = tuple.getAmount();

        this.tuples = new PriorityQueue<Tuple2<Transaction, Long>>(new TransactionComparator());
        this.tuples.add(new Tuple2<>(tuple, System.currentTimeMillis()));
    }

    public void addTuple(Transaction tuple){

        if(this.lowerBound > tuple.getAmount()){
            this.lowerBound = tuple.getAmount();
        }
        if(this.upperBound < tuple.getAmount()){
            this.upperBound = tuple.getAmount();
        }

        this.tuples.add(new Tuple2<>(tuple, System.currentTimeMillis()));

    }

    public double lossDueToEnlargement(Transaction tuple, double globLowerBound, double globUpperBound){
        double newLowerBound = this.lowerBound;
        if(newLowerBound > tuple.getAmount()) newLowerBound = tuple.getAmount();

        double newUpperBound = this.upperBound;
        if(newUpperBound < tuple.getAmount()) newUpperBound = tuple.getAmount();

        return (newUpperBound - newLowerBound)/(globUpperBound - globLowerBound);
    }

    public Boolean testEnlargement(Transaction tuple, double threshold, double globLowerBound, double globUpperBound){
        return this.lossDueToEnlargement(tuple, globLowerBound, globUpperBound) <= threshold;
    }

    public void merge(Cluster c){
        this.lowerBound = Double.min(this.lowerBound, c.lowerBound);
        this.upperBound = Double.max(this.upperBound, c.upperBound);

        while(!c.tuples.isEmpty()){
            this.tuples.add(c.tuples.poll());
        }

    }

    public double lossDueToMerge(Cluster c, double globLowerBound, double globUpperBound){
        double newLowerBound = Double.min(this.lowerBound, c.lowerBound);
        double newUpperBound = Double.max(this.upperBound, c.upperBound);

        return (newUpperBound - newLowerBound)/(globUpperBound - globLowerBound);
    }

    public String toString(){
        return "Lower Bound: " + this.lowerBound + ", Upper Bound: " + this.upperBound + ", Size: " + this.tuples.size();
    }

    static class TransactionComparator implements Comparator<Tuple2<Transaction, Long>> {

        @Override
        public int compare(Tuple2<Transaction, Long> o1, Tuple2<Transaction, Long> o2) {
            return o1._2.longValue() > o2._2.longValue() ? 1 : -1;
        }
    }
}
