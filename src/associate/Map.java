package associate;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public final class Map extends Mapper<LongWritable, Text, Text, Text> {
    final Text keyInt = new Text();
    final Text valInt = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        System.out.println(value.toString());
        String[] cols = value.toString().split("\t");

        for (int i = 0; i < cols.length - 1 && cols.length > 2; i++) {
            StringBuilder text = new StringBuilder();
            for (int j = 0; j < cols.length - 1; j++) {
                if (i != j) text.append(cols[j]).append("\t");
            }
            keyInt.set(text.toString().trim());
            valInt.set(cols[i] + "\t" + cols[cols.length - 1]);
            context.write(keyInt, valInt);
        }
        keyInt.set(String.join("\t", Arrays.copyOf(cols, cols.length - 1)));
        valInt.set(cols[cols.length - 1]);
        context.write(keyInt, valInt);
    }
}
