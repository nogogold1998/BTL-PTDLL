package associate;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class Associate {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String INPUT_FILE_PATH = "C:\\Users\\nogog\\eclipse-workspace\\BaiTapLonPTDLL\\src\\data\\output";
//        String INPUT_FILE_PATH = "C:\\Users\\nogog\\eclipse-workspace\\BaiTapLonPTDLL\\src\\data\\breast-cancer.arff";
        String OUTPUT_FILE_PATH = "C:\\Users\\nogog\\eclipse-workspace\\BaiTapLonPTDLL\\src\\data\\associate";
        final double minConfidence = 0.9;
        final int round = 5;

        JobConf jobConf = new JobConf();
        jobConf.setDouble("minConf", minConfidence);

        FileUtils.deleteDirectory(new File(OUTPUT_FILE_PATH));
//        FileInputFormat.setInputPaths(jobConf, new Path(INPUT_FILE_PATH));
        FileOutputFormat.setOutputPath(jobConf, new Path(OUTPUT_FILE_PATH));

        Job job = Job.getInstance(jobConf, "Sinh luật kết hợp từ các tập mục phổ biến");
        job.setJarByClass(Associate.class);
        for (int k = 1; k < round; k++) {
            MultipleInputs.addInputPath(
                    job,
                    new Path(INPUT_FILE_PATH + "\\" + k + "\\part-r-00000"),
                    TextInputFormat.class,
                    Map.class);
        }
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

//        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        System.out.println("Done=" + job.waitForCompletion(true));

        printSorted(OUTPUT_FILE_PATH + "\\part-r-00000");
    }

    static void printSorted(String outputFilePath) throws IOException {
        Path path = new Path(outputFilePath);
        FileSystem fs = FileSystem.get(new Configuration());
        if (!fs.exists(path)) throw new IllegalArgumentException("Sai đường dẫn file luật kết hợp");
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        List<String[]> rules = new ArrayList<>();
        for (String line = br.readLine(); line != null; line = br.readLine()) {
            int lastTabIndex = line.lastIndexOf('\t');
            String[] s = new String[]{line.substring(0, lastTabIndex), line.substring(lastTabIndex + 1)};
            rules.add(s);
        }
        rules.sort((o1, o2) -> o2[1].compareTo(o1[1]));

        for (String[] rule : rules){
            System.out.println(Arrays.toString(rule));
        }
    }
}
