import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public final class Reducer1K extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // Lấy minSup và số giao dịch
        double minSup = context.getConfiguration().getDouble("minSup", 0);
        long numTransactions = context.getConfiguration().getLong("numTrans", 0);
        try {
            if(numTransactions == 0 || minSup == 0)
                throw new IllegalArgumentException("Sai minSup=" + minSup + "hoac numTransaction=" + numTransactions);

            int sum = 0;
            for(IntWritable val : values){
                sum += val.get();
            }

            // Nếu số giao tác lớn hơn hoặc bằng minSup thì mới cho vào
            if(sum >= minSup * numTransactions)
                context.write(key, new IntWritable(sum));
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
        }
    }
}
