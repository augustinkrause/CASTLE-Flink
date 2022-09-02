package spendreport;

import org.apache.flink.util.StringUtils;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Comparator;
import java.util.PriorityQueue;

public class Cluster {

    double[] lowerBounds;
    double[] upperBounds;

    private int[] keys;
    double oldInfoLoss = 0;

    public long timestamp;

    //we use a priority queue to easily check whether the oldest tuple of this cluster needs to be released
    PriorityQueue<Tuple2<Tuple, Long>> elements;

    public Cluster(double[] lowerBounds, double[] upperBounds, int[] keys){
        this.keys = keys;

        this.lowerBounds = lowerBounds;
        this.upperBounds = upperBounds;

        this.elements = new PriorityQueue<>(new ElementComparator());

        this.timestamp = System.currentTimeMillis();
    }

    public Cluster(Tuple2<Tuple, Long> element, int[] keys){
        try {
            this.keys = keys;

            this.lowerBounds = new double[this.keys.length];
            this.upperBounds = new double[this.keys.length];
            for(int i = 0; i < this.keys.length; i++){
                this.lowerBounds[i] = ((Number)element.f0.getField(this.keys[i])).doubleValue();
                this.upperBounds[i] = ((Number)element.f0.getField(this.keys[i])).doubleValue();
            }

            this.elements = new PriorityQueue<>(new ElementComparator());
            this.elements.add(element);

            this.timestamp = System.currentTimeMillis();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void addTuple(Tuple2<Tuple, Long> element, double[] globLowerBounds, double[] globUpperBounds){

        double newInfoLoss = 0;
        for(int i = 0; i < this.keys.length; i++){
            try {
                if (this.lowerBounds[i] > ((Number) element.f0.getField(this.keys[i])).doubleValue()) {
                    this.lowerBounds[i] = ((Number) element.f0.getField(this.keys[i])).doubleValue();
                }
                if (this.upperBounds[i] < ((Number) element.f0.getField(this.keys[i])).doubleValue()) {
                    this.upperBounds[i] = ((Number) element.f0.getField(this.keys[i])).doubleValue();
                }

                this.elements.add(element);
                newInfoLoss += (this.upperBounds[i] - this.lowerBounds[i]) / (globUpperBounds[i] - globLowerBounds[i]);
            }catch (Exception e) {
                e.printStackTrace();
            }
        }

        this.oldInfoLoss = newInfoLoss;
    }

    public double lossDueToEnlargement(Tuple element, double[] globLowerBounds, double[] globUpperBounds){
        double newInfoLoss = 0;
        for(int i = 0; i < this.keys.length; i++){
            try {
                double newLowerBound = this.lowerBounds[i];
                if (newLowerBound > ((Number) element.getField(this.keys[i])).doubleValue())
                    newLowerBound = ((Number) element.getField(this.keys[i])).doubleValue();

                double newUpperBound = this.upperBounds[i];
                if (newUpperBound < ((Number) element.getField(this.keys[i])).doubleValue())
                    newUpperBound = ((Number) element.getField(this.keys[i])).doubleValue();

                newInfoLoss += (newUpperBound - newLowerBound) / (globUpperBounds[i] - globLowerBounds[i]);
            }catch (Exception e) {
                e.printStackTrace();
            }
        }

        return newInfoLoss - this.oldInfoLoss;
    }

    public Boolean testEnlargement(Tuple element, double threshold, double[] globLowerBounds, double[] globUpperBounds){
        return this.lossDueToEnlargement(element, globLowerBounds, globUpperBounds) <= threshold;
    }

    public void merge(Cluster c, double[] globLowerBounds, double[] globUpperBounds){
        double newInfoLoss = 0;

        for(int i = 0; i < this.keys.length; i++){
            this.lowerBounds[i] = Double.min(this.lowerBounds[i], c.lowerBounds[i]);
            this.upperBounds[i] = Double.max(this.upperBounds[i], c.upperBounds[i]);

            newInfoLoss += (this.upperBounds[i] - this.lowerBounds[i])/(globUpperBounds[i] - globLowerBounds[i]);
        }

        while(!c.elements.isEmpty()){
            this.elements.add(c.elements.poll());
        }

        this.oldInfoLoss = newInfoLoss;
    }

    public double lossDueToMerge(Cluster c, double[] globLowerBounds, double[] globUpperBounds){
        double newInfoLoss = 0;
        for(int i = 0; i < this.keys.length; i++){
            double newLowerBound = Double.min(this.lowerBounds[i], c.lowerBounds[i]);
            double newUpperBound = Double.max(this.upperBounds[i], c.upperBounds[i]);

            newInfoLoss += (newUpperBound - newLowerBound)/(globUpperBounds[i] - globLowerBounds[i]);
        }

        return newInfoLoss - this.oldInfoLoss;
    }

    public Tuple generalize(Tuple t){
        Tuple newT = Tuple.newInstance(t.getArity());

        for(int i = 0; i < this.keys.length; i++){
            newT.setField(new Tuple2<>(this.lowerBounds[i], this.upperBounds[i]), this.keys[i]);
        }

        for(int i = 0; i < t.getArity(); i++){
            if(newT.getField(i) == null) newT.setField(t.getField(i) ,i);
        }

        return newT;
    }

    //checks whether t lies within the bounds of each dimension
    public boolean fits(Tuple t){

        boolean check = true;
        for(int i = 0; i < this.keys.length; i++){
            check = check && (((Number) t.getField(this.keys[i])).doubleValue() >= this.lowerBounds[i] && ((Number) t.getField(this.keys[i])).doubleValue() <= this.upperBounds[i]);
        }
        return check;
    }

    public String toString(){
        return "Lower Bound: " + StringUtils.arrayAwareToString(this.lowerBounds) + ", Upper Bound: " + StringUtils.arrayAwareToString(this.upperBounds) + ", Size: " + this.elements.size();
    }

    static class ElementComparator implements Comparator<Tuple2<?, Long>> {

        @Override
        public int compare(Tuple2<?, Long> o1, Tuple2<?, Long> o2) {
            return o1.f1.longValue() > o2.f1.longValue() ? 1 : -1;
        }
    }
}
