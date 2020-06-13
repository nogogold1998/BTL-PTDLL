import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

public final class MapK extends Mapper<LongWritable, Text, Text, IntWritable> {
    public static volatile List<String[]> Ck;
    static final String[] columnNames = new String[]{
            "age", "menopause", "tumor-size", "inv-nodes", "node-caps", "deg-malig", "breast", "breast-quad", "irradiat"};
    final Text text = new Text();
    final IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] cells = line.split(",");

        for(String[] itemSetCol : Ck){
            if(itemSetCol.length != cells.length) throw new IllegalStateException("Độ dài Ck khác cell");
            String textKey = "";
            for(int i = 0; i < cells.length; i++){
                if(itemSetCol[i] == null || itemSetCol[i].isEmpty()) //noinspection UnnecessaryContinue
                    continue;
                else if(itemSetCol[i].equals(cells[i])) textKey += columnNames[i] + "=" + cells[i] + "\t";
                else {
                    textKey = "";
                    break;
                }
            }
            if(!textKey.isEmpty()) {
                text.set(textKey.trim());
                context.write(text, one);
            }
        }
    }

}

