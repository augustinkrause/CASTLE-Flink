package spendreport;

import scala.Tuple2;

import java.lang.reflect.InvocationTargetException;
import java.util.Comparator;
import java.util.PriorityQueue;

public class Cluster<T> {

    double lowerBound;
    double upperBound;

    private String methodName;

    //we use a priority queue to easily check wether the oldest tuple of this cluster needs to be released
    PriorityQueue<Tuple2<T, Long>> elements;

    public Cluster(double lowerBound, double upperBound, String methodName){
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;

        this.elements = new PriorityQueue<>(new ElementComparator());

        this.methodName = methodName;
    }

    public Cluster(T element, String methodName){
        try {
            this.lowerBound = ((Number) element.getClass().getMethod(this.methodName).invoke(element)).doubleValue();
            this.upperBound = ((Number) element.getClass().getMethod(this.methodName).invoke(element)).doubleValue();

            this.elements = new PriorityQueue<Tuple2<T, Long>>(new ElementComparator());
            this.elements.add(new Tuple2<>(element, System.currentTimeMillis()));

            this.methodName = methodName;
        }catch(NoSuchMethodException e){
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
    }

    public void addTuple(T element){

        try {
            if (this.lowerBound > ((Number) element.getClass().getMethod(this.methodName).invoke(element)).doubleValue()) {
                this.lowerBound = ((Number) element.getClass().getMethod(this.methodName).invoke(element)).doubleValue();
            }
            if (this.upperBound < ((Number) element.getClass().getMethod(this.methodName).invoke(element)).doubleValue()) {
                this.upperBound = ((Number) element.getClass().getMethod(this.methodName).invoke(element)).doubleValue();
            }

            this.elements.add(new Tuple2<>(element, System.currentTimeMillis()));
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }

    }

    public double lossDueToEnlargement(T element, double globLowerBound, double globUpperBound){
        try {
            double newLowerBound = this.lowerBound;
            if (newLowerBound > ((Number) element.getClass().getMethod(this.methodName).invoke(element)).doubleValue())
                newLowerBound = ((Number) element.getClass().getMethod(this.methodName).invoke(element)).doubleValue();

            double newUpperBound = this.upperBound;
            if (newUpperBound < ((Number) element.getClass().getMethod(this.methodName).invoke(element)).doubleValue())
                newUpperBound = ((Number) element.getClass().getMethod(this.methodName).invoke(element)).doubleValue();

            return (newUpperBound - newLowerBound) / (globUpperBound - globLowerBound);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
        return 1;
    }

    public Boolean testEnlargement(T element, double threshold, double globLowerBound, double globUpperBound){
        return this.lossDueToEnlargement(element, globLowerBound, globUpperBound) <= threshold;
    }

    public void merge(Cluster<T> c){
        this.lowerBound = Double.min(this.lowerBound, c.lowerBound);
        this.upperBound = Double.max(this.upperBound, c.upperBound);

        while(!c.elements.isEmpty()){
            this.elements.add(c.elements.poll());
        }

    }

    public double lossDueToMerge(Cluster<T> c, double globLowerBound, double globUpperBound){
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
