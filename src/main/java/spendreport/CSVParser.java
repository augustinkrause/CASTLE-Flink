package spendreport;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.util.regex.Pattern;

public class CSVParser implements MapFunction<String, Tuple>, ResultTypeQueryable {

    int nCols;
    TypeInformation[] types;
    String delimiter = ",";

    public CSVParser(Integer nCols, TypeInformation[] types, String delimiter) throws Exception{
        if(types.length != nCols){
            throw new RuntimeException("You need to specify as many types as there are columns!");
        }
        this.nCols = nCols;
        this.types = types;
        this.delimiter = delimiter;
    }

    public CSVParser(Integer nCols, TypeInformation[] types){
        if(types.length != nCols){
            throw new RuntimeException("You need to specify as many types as there are columns!");
        }
        this.nCols = nCols;
        this.types = types;
    }

    @Override
    public Tuple map(String line) throws Exception {

        String[] tokens = line.split(Pattern.quote(this.delimiter));
        if (tokens.length != this.nCols) {
            throw new RuntimeException("Invalid record: " + line);
        }

        try{
            Tuple t = Tuple.newInstance(this.nCols);
            for(int i = 0; i < this.nCols; i++){
                if(this.types[i].getTypeClass().isInstance((Integer) 1)){
                    t.setField(Integer.parseInt(tokens[i]), i);
                }else if(this.types[i].getTypeClass().isInstance((Double) 1.)){
                    t.setField(Double.parseDouble(tokens[i]), i);
                }else{
                    t.setField(tokens[i], i);
                }
            }

            return t;
        }catch(NumberFormatException nfe) {
            throw new RuntimeException("Invalid field: " + line, nfe);
        }
    }

    @Override
    public TypeInformation getProducedType() {
        return Types.TUPLE(this.types);
    }
}
