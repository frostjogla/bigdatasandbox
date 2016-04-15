package ru.fj.meteodata.avg;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

public class AvgTemperatureCombiner extends Reducer<DecadeYear, Pair, DecadeYear, Pair> {

    @Override
    protected void reduce(DecadeYear key, Iterable<Pair> values,
            Reducer<DecadeYear, Pair, DecadeYear, Pair>.Context context) throws IOException, InterruptedException {

        Pair outValue = new Pair();

        for (Pair value : values) {
            outValue.amount += value.amount;
            outValue.sum += value.sum;
        }
        context.write(key, outValue);
    }
}
