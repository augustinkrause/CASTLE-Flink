package spendreport;

import org.apache.flink.api.java.functions.KeySelector;
import scala.Tuple2;

import java.util.Comparator;
import java.util.PriorityQueue;

public class Cluster<T, K extends Number> {

    double lowerBound;
    double upperBound;

    private KeySelector<T, K> key;

    //we use a priority queue to easily check wether the oldest tuple of this cluster needs to be released
    PriorityQueue<Tuple2<T, Long>> elements;

    public Cluster(double lowerBound, double upperBound, KeySelector<T, K> key){
        this.key = key;

        this.lowerBound = lowerBound;
        this.upperBound = upperBound;

        this.elements = new PriorityQueue<>(new ElementComparator());
    }

    public Cluster(T element, KeySelector<T, K> key){
        try {
            this.key = key;

            this.lowerBound = (this.key.getKey(element)).doubleValue();
            this.upperBound = (this.key.getKey(element)).doubleValue();

            this.elements = new PriorityQueue<Tuple2<T, Long>>(new ElementComparator());
            this.elements.add(new Tuple2<>(element, System.currentTimeMillis()));
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void addTuple(T element){

        try {
            if (this.lowerBound > (this.key.getKey(element)).doubleValue()) {
                this.lowerBound = (this.key.getKey(element)).doubleValue();
            }
            if (this.upperBound < (this.key.getKey(element)).doubleValue()) {
                this.upperBound = (this.key.getKey(element)).doubleValue();
            }

            this.elements.add(new Tuple2<>(element, System.currentTimeMillis()));
        }catch (Exception e) {
            e.printStackTrace();
        }

    }

    public double lossDueToEnlargement(T element, double globLowerBound, double globUpperBound){
        try {
            double newLowerBound = this.lowerBound;
            if (newLowerBound > (this.key.getKey(element)).doubleValue())
                newLowerBound = (this.key.getKey(element)).doubleValue();

            double newUpperBound = this.upperBound;
            if (newUpperBound < (this.key.getKey(element)).doubleValue())
                newUpperBound = (this.key.getKey(element)).doubleValue();

            return (newUpperBound - newLowerBound) / (globUpperBound - globLowerBound);
        }catch (Exception e) {
            e.printStackTrace();
        }
        return 1;
    }

    public Boolean testEnlargement(T element, double threshold, double globLowerBound, double globUpperBound){
        return this.lossDueToEnlargement(element, globLowerBound, globUpperBound) <= threshold;
    }

    public void merge(Cluster<T, K> c){
        this.lowerBound = Double.min(this.lowerBound, c.lowerBound);
        this.upperBound = Double.max(this.upperBound, c.upperBound);

        while(!c.elements.isEmpty()){
            this.elements.add(c.elements.poll());
        }

    }

    public double lossDueToMerge(Cluster<T, K> c, double globLowerBound, double globUpperBound){
        double newLowerBound = Double.min(this.lowerBound, c.lowerBound);
        double newUpperBound = Double.max(this.upperBound, c.upperBound);

        return (newUpperBound - newLowerBound)/(globUpperBound - globLowerBound);
    }

    public String toString(){
        return "Lower Bound: " + this.lowerBound + ", Upper Bound: " + this.upperBound + ", Size: " + this.elements.size();
    }

    static class ElementComparator implements Comparator<Tuple2<?, Long>> {

        @Override
        public int compare(Tuple2<?, Long> o1, Tuple2<?, Long> o2) {
            return o1._2.longValue() > o2._2.longValue() ? 1 : -1;
        }
    }
}
