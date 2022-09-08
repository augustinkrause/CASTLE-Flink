package spendreport;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.HashMap;

public class FinalSink implements SinkFunction<Tuple> {

    private HashMap<Integer, Integer> uniquenessMap;
    private int nReceived = 0;
    private int nNotUnique = 0;

    public FinalSink(){
        //this.uniquenessMap = new HashMap<>();
    }

    @Override
    public void invoke(Tuple value, Context context) throws Exception {
        /*this.nReceived++;

        if(this.uniquenessMap.get(value.getField(0)) != null){
            nNotUnique++;
            this.uniquenessMap.put(value.getField(0), this.uniquenessMap.get(value.getField(0)) + 1);
            System.out.println("FOUND ONE!!!" + value.getField(0));
            System.out.println("found it " + this.uniquenessMap.get(value.getField(0)) + " times");
        }else{
            this.uniquenessMap.put(value.getField(0), 0);
        }

        System.out.println("received: " + nReceived);
        System.out.println("duplicates: " + nNotUnique);*/
    }

    @Override
    public void finish(){
    }
}
