package spendreport;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.walkthrough.common.entity.Transaction;

public class GeneralizedElement<T, K extends Number>{

    private T element;
    private Tuple2<K, K> generalizedValue;

    public GeneralizedElement(T element, K lowerBound, K upperBound){
        this.element = element;
        this.generalizedValue = new Tuple2<>(lowerBound, upperBound);
    }

    public T getElement() {
        return this.element;
    }

    public Tuple2<K, K> getGeneralizedValue() {
        return this.generalizedValue;
    }

    public String toString() {
        return "Transaction{element=" + this.element + ", generalizedValue=" + this.generalizedValue + '}';
    }
}
