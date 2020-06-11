import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class AprioriTID {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String INPUT_FILE_PATH = "C:\\Users\\nogog\\eclipse-workspace\\BaiTapLonPTDLL\\src\\data\\breast-cancer.arff";
        String OUTPUT_FILE_PATH = "C:\\Users\\nogog\\eclipse-workspace\\BaiTapLonPTDLL\\src\\data\\output";

//        JobConf jobConf = new JobConf();
//
//        // truyền vào minSup và số giao dịch
//        jobConf.setDouble("minSup", 0.2);
//        jobConf.setLong("numTrans", 277);
//
//        try {
//            FileUtils.deleteDirectory(new File(OUTPUT_FILE_PATH));
//        } catch (IOException e1) {
//            // TODO Auto-generated catch block
//            e1.printStackTrace();
//        }
//        FileInputFormat.setInputPaths(jobConf, new Path(INPUT_FILE_PATH));
//        FileOutputFormat.setOutputPath(jobConf, new Path(OUTPUT_FILE_PATH));
//
//        Job job = Job.getInstance(jobConf, "Apriori1");
//        job.setJarByClass(AprioriTID.class);
//        // Setup
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//
//        job.setMapperClass(Map1.class);
//        job.setReducerClass(Reducer1K.class);
//        System.out.println("Done=" + job.waitForCompletion(true));

        List<String[]> Ck = apriori_gen(2, "C:\\Users\\nogog\\eclipse-workspace\\BaiTapLonPTDLL\\src\\data\\output\\part-r-00000");
        JobConf conf2 = new JobConf(AprioriTID.class);
        conf2.setDouble("minSup", 0.2);
        conf2.setLong("numTrans", 277);
        FileInputFormat.setInputPaths(conf2, new Path(INPUT_FILE_PATH));
        FileOutputFormat.setOutputPath(conf2, new Path(OUTPUT_FILE_PATH + "1"));
        Job job2 = Job.getInstance(conf2, "Apriori2");
        job2.setJarByClass(AprioriTID.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        MapK.Ck = Ck;
//        ArrayList<String[]> test = new ArrayList<>();
//        test.add(new String[]{"40-49","premeno", null, null, null, null, null, null, null});
//        MapK.Ck = test;
        job2.setMapperClass(MapK.class);
        job2.setReducerClass(Reducer1K.class);
        try {
            FileUtils.deleteDirectory(new File(OUTPUT_FILE_PATH + "1"));
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        System.out.println("Done=2" + job2.waitForCompletion(true));
    }

    /**
     * Tìm các tập mục phổ biến ứng viên Ck thứ k >= 2
     * @param k
     * @param pathString
     * @return List ItemSet Columns pattern
     * @throws IOException
     */
    static List<String[]> apriori_gen(int k, String pathString) throws IOException {
        Path path = new Path(pathString);
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        Set<List<String>> temp_Ck = new HashSet<>();
        for (String line = br.readLine(); line != null; line = br.readLine()) {
            String[] s = line.split("\t");
            temp_Ck.add(Arrays.asList(Arrays.copyOf(s, s.length - 1)));
        }

        List<List<String>> Ck = new ArrayList<>(temp_Ck);
        List<String[]> result = new ArrayList<>();
        for (int i = 0; i < temp_Ck.size() - 1; i++) {
            List<String> itemSetI = Ck.get(i);
            for (int j = i + 1; j < temp_Ck.size(); j++) {
                List<String> itemSetJ = Ck.get(j);
                for (int x = 0; x < k - 1; x++) {
                    String[] colI = itemSetI.get(x).split("="); // ["tumor-size", "20-24"]
                    String[] colJ = itemSetJ.get(x).split("=");
                    if (!colI[0].equals(colJ[0])) {
                        if (x < k - 2)
                            break;
                        else {
                            ArrayList<String> newItemSet = new ArrayList<>(itemSetI);
                            newItemSet.add(itemSetJ.get(k - 2));
                            result.add(toItemSetCols(newItemSet));
                        }
                    }
                }
            }
        }
        for(String[] i : result){
            System.out.println(Arrays.toString(i));
        }
        return result;
    }

    static String[] toItemSetCols(List<String> itemSet){
        String[] result = new String[9];
        for(String item : itemSet) {
            int colIndex;
            String[] cols = item.split("=");
            switch (cols[0]) {
                case "age":
                    colIndex = 0;
                    break;
                case "menopause":
                    colIndex = 1;
                    break;
                case "tumor-size":
                    colIndex = 2;
                    break;
                case "inv-nodes":
                    colIndex = 3;
                    break;
                case "node-caps":
                    colIndex = 4;
                    break;
                case "deg-malig":
                    colIndex = 5;
                    break;
                case "breast":
                    colIndex = 6;
                    break;
                case "breast-quad":
                    colIndex = 7;
                    break;
                case "irradiat":
                    colIndex = 8;
                    break;
                default:
                    throw new IllegalArgumentException("Wrong column name");
            }
            result[colIndex] = cols[1];
        }
        return result;
    }
}
