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

public final class AprioriTID {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String INPUT_FILE_PATH = "C:\\Users\\nogog\\eclipse-workspace\\BaiTapLonPTDLL\\src\\data\\breast-cancer.arff";
        String OUTPUT_FILE_PATH = "C:\\Users\\nogog\\eclipse-workspace\\BaiTapLonPTDLL\\src\\data\\output\\";
        double minSupport = 0.2;
        long numTransactions = 277;

        for (int k = 1; ; k++) {
            int status = run(k, minSupport, numTransactions, INPUT_FILE_PATH, OUTPUT_FILE_PATH);
            if (status == 1) {
                System.out.println("Chạy xong vòng: " + k);
            } else if (status == 0) {
                System.out.println("Hoàn thành tại vòng: " + (k - 1));
                break;
            } else {
                System.out.println("Lỗi tại vòng: " + k);
            }
        }
    }

    private static int run(int k, double minSup, long numTrans, String inputPath, String outputPath)
            throws IOException, InterruptedException, ClassNotFoundException {
        if (k < 1 || minSup < 0 || minSup > 1 || numTrans < 0)
            throw new IllegalArgumentException("Sai k hoặc minSup hoặc numTrans");
        JobConf jobConf = new JobConf();

        // truyền vào minSup và số giao dịch
        jobConf.setDouble("minSup", minSup);
        jobConf.setLong("numTrans", numTrans);

        // Xóa thư mục trước
        FileUtils.deleteDirectory(new File(outputPath + k));
        FileInputFormat.setInputPaths(jobConf, new Path(inputPath));
        FileOutputFormat.setOutputPath(jobConf, new Path(outputPath + k));

        Job job = Job.getInstance(jobConf, "Apriori tìm itemset L" + k);
        job.setJarByClass(AprioriTID.class);
        // Setup
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        if (k == 1) {
            job.setMapperClass(Map1.class);
        } else {
            List<String[]> Ck = apriori_gen(k, outputPath + (k - 1) + "\\part-r-00000");
            if (Ck.isEmpty()) return 0;
            MapK.Ck = Ck;
            job.setMapperClass(MapK.class);
        }
        job.setReducerClass(Reducer1K.class);
        return job.waitForCompletion(true) ? 1 : -1;
    }

    /**
     * Tìm các tập mục phổ biến ứng viên Ck thứ k >= 2
     */
    static List<String[]> apriori_gen(int k, String pathString) throws IOException {
        Path path = new Path(pathString);
        FileSystem fs = FileSystem.get(new Configuration());
        List<String[]> result = new ArrayList<>();
        if (!fs.exists(path)) return result;
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        Set<List<String>> temp_Ck = new HashSet<>();
        for (String line = br.readLine(); line != null; line = br.readLine()) {
            String[] s = line.split("\t");
            temp_Ck.add(Arrays.asList(Arrays.copyOf(s, s.length - 1)));
        }

        List<List<String>> Ck = new ArrayList<>(temp_Ck);
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
        return result;
    }

    static String[] toItemSetCols(List<String> itemSet) {
        String[] result = new String[9];
        for (String item : itemSet) {
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
