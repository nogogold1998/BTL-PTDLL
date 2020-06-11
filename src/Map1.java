import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map1 extends Mapper<LongWritable, Text, Text, IntWritable> {
    static final String[] columnNames = new String[]{
            "age", "menopause", "tumor-size", "inv-nodes", "node-caps", "deg-malig", "breast", "breast-quad", "irradiat"};
    final Text text = new Text();
    final IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] cells = line.split(",");

        for (int i = 0; i < columnNames.length; i++) {
            text.set(columnNames[i] + "=" + cells[i]);
            context.write(text, one);
        }
    }
}
