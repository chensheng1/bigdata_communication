package hadoop;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;

/**
 * Created by asus on 2020/7/15.
 */
public class write {
    public  static void wri_text(Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        int i = 0;
        Path path = new Path("/data/add.txt");
        SequenceFile.Writer writer = null;


        writer = SequenceFile.createWriter(conf);
        //写入的数据可以根据你的情况来定，我这只是测试


            writer.append(1,"wyp1" );


        writer.close();
    }
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://192.168.229.111:9000");
        try {
            wri_text(conf);
            System.out.println("\n写入完成");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
