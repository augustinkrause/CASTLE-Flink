package spendreport;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.StringUtils;

public class GeneralizedElement<T>{

    private T element;
    private Tuple2<Double, Double>[] generalizedValues;

    public GeneralizedElement(T element, double[] lowerBounds, double[] upperBounds){
        this.element = element;
        this.generalizedValues = new Tuple2[lowerBounds.length];
        for(int i = 0; i < lowerBounds.length; i++){
            this.generalizedValues[i] = new Tuple2<>(lowerBounds[i], upperBounds[i]);
        }
    }


    public String toString() {
        return "Transaction{element=" + this.element + ", generalizedValues=" + StringUtils.arrayAwareToString(this.generalizedValues) + '}';
    }
}
