package spendreport;

import org.apache.flink.util.StringUtils;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;

public class Cluster implements Serializable {

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

                newInfoLoss += (this.upperBounds[i] - this.lowerBounds[i]) / (globUpperBounds[i] - globLowerBounds[i]);
            }catch (Exception e) {
                e.printStackTrace();
            }
        }

        this.elements.add(element);
        this.oldInfoLoss = newInfoLoss;
    }

    //by default this function returns a difference, this can explicitly be turned off (see next signature)
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

    public double lossDueToEnlargement(Tuple element, double[] globLowerBounds, double[] globUpperBounds, boolean difference){
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

        if(difference){
            return newInfoLoss - this.oldInfoLoss;
        }else{
            return newInfoLoss;
        }
    }

    public Boolean testEnlargement(Tuple element, double threshold, double[] globLowerBounds, double[] globUpperBounds){
        return this.lossDueToEnlargement(element, globLowerBounds, globUpperBounds, false) <= threshold;
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
            if(newT.getField(i) == null) newT.setField(t.getField(i), i);
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

    //readObject, writeObject, readObjectNoData are functions that are called during serialization of instances of this class
    private void writeObject(java.io.ObjectOutputStream out) throws IOException {

        out.writeInt(this.lowerBounds.length);
        for(int i = 0; i < this.lowerBounds.length; i++){
            out.writeDouble(this.lowerBounds[i]);
        }
        out.writeInt(this.upperBounds.length);
        for(int i = 0; i < this.upperBounds.length; i++){
            out.writeDouble(this.upperBounds[i]);
        }

        out.writeInt(this.keys.length);
        for(int i = 0; i < this.keys.length; i++){
            out.writeInt(this.keys[i]);
        }

        out.writeDouble(this.oldInfoLoss);

        out.writeLong(this.timestamp);

        out.writeObject(this.elements);

    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {

        int len = in.readInt();
        this.lowerBounds = new double[len];
        for(int i = 0; i < len; i++){
            this.lowerBounds[i] = in.readDouble();
        }
        len = in.readInt();
        this.upperBounds = new double[len];
        for(int i = 0; i < len; i++){
            this.upperBounds[i] = in.readDouble();
        }

        len = in.readInt();
        this.keys = new int[len];
        for(int i = 0; i < len; i++){
            this.keys[i] = in.readInt();
        }

        this.oldInfoLoss = in.readDouble();

        this.timestamp = in.readLong();

        this.elements = (PriorityQueue<Tuple2<Tuple, Long>>) in.readObject();

    }

    private void readObjectNoData() throws ObjectStreamException {
        throw new NotSerializableException("Serialization error in class " + this.getClass().getName());
    }

    static class ElementComparator implements Comparator<Tuple2<?, Long>>, Serializable{

        @Override
        public int compare(Tuple2<?, Long> o1, Tuple2<?, Long> o2) {
            return o1.f1.longValue() > o2.f1.longValue() ? 1 : -1;
        }
    }
}
