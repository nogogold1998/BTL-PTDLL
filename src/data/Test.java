package data;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class Test {
    public static void main(String[] args) throws IOException {
        Path path = new Path("C:\\Users\\nogog\\eclipse-workspace\\BaiTapLonPTDLL\\src\\data\\associate\\part-r-00000");
        FileSystem fs = FileSystem.get(new Configuration());
        if (!fs.exists(path)) throw new IllegalArgumentException("Sai đường dẫn file luật kết hợp");
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        List<String[]> rules = new ArrayList<>();
        for (String line = br.readLine(); line != null; line = br.readLine()) {
            int lastTabIndext = line.lastIndexOf('\t');
            String[] s = new String[]{line.substring(0, lastTabIndext), line.substring(lastTabIndext + 1)};
            rules.add(s);
        }
        rules.sort((o1, o2) -> o2[1].compareTo(o1[1]));

        for (String[] rule : rules){
            System.out.println(Arrays.toString(rule));
        }
    }
}
