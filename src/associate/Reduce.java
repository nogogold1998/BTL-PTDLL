package associate;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class Reduce extends Reducer<Text, Text, Text, Text> {
    final Text keyOut = new Text();
    final Text valOut = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double minConf = context.getConfiguration().getDouble("minConf", 0);
//        System.out.print("Reduce: {key=" + key.toString() + ", values=[");
//        for (Text text : values) {
//            System.out.print(text + (values.iterator().hasNext() ? ", " : ""));
//        }
//        System.out.println("]}");
        try {
            int supportX = 0;
            List<String[]> itemSets = new ArrayList<>();
            for (Text text : values) {
                String[] cols = text.toString().split("\t");
                if (cols.length == 1) supportX = Integer.parseInt(cols[0]);
                else if (cols.length == 2) itemSets.add(cols);
                else throw new IllegalArgumentException("Sai valueInt:" + text.toString());
            }
            if (supportX == 0) throw new IllegalArgumentException("Không tìm thấy support(X)");

            for (String[] itemSet : itemSets) {
                int supportXY = Integer.parseInt(itemSet[1]);

                double confidence = supportXY / (1.0 * supportX);
                if (minConf <= confidence && confidence <= 1) {
                    keyOut.set("(" + key + ")=" + supportX + "\t=>\t" + itemSet[0] + "=" + supportXY);
                    valOut.set(String.valueOf(confidence));
                    context.write(keyOut, valOut);
                }
            }
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
        }
    }
}
