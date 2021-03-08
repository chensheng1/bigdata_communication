package hadoop;

/**
 * Created by asus on 2020/7/6.
 * user:cs
 * 描述：读取zip文件
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class readfile {
    /**
     * 读取文件内容
     */
    public static void cat(Configuration conf, String remoteFilePath) throws IOException {
        String uri = remoteFilePath;
        FileSystem fs = FileSystem.get(conf);

        Path inputpath = new Path(uri);
        FSDataInputStream in = fs.open(inputpath);
        ZipInputStream d = new ZipInputStream(in);
        ZipEntry entry;
        while ((entry = d.getNextEntry()) != null) {
            //获取当前解压的文件名
            String entryName = entry.getName();
            System.out.println("fileName:" + entryName);
            String input= "/data/" + entryName;
            //判断解压文件是否非文件夹
            if (entry.isDirectory()) {
                System.err.println("true if this is a directory entry. A directory entry is!");
            } else {
                if (entry.getName().endsWith(".gz")) {
                    if (entry.getName().contains("http.")) {
                        if (entry.getSize() == -1) {
                            ByteArrayOutputStream baos = new ByteArrayOutputStream();
                            while (true) {
                                int bytes = d.read();
                                if (bytes == -1) {
                                    break;
                                }
                                baos.write(bytes);
                            }
                            baos.close();
                            System.out.println(String.format("Name:%s,Content:%s", entry.getName(), new String(baos.toByteArray())));
                        } else {
                            byte[] bytes = new byte[(int) entry.getSize()];
                            d.read(bytes, 0, (int) entry.getSize());
                            System.out.println(String.format("Name:%s,Content:%s", entry.getName(), new String(bytes)));
                        }
                    }

                } else {
                    System.out.println("is file");
                    //在hdfs上创建指定文件

                }
            }
        }
            d.close();
            in.close();
            fs.close();
            /**
             *数据的解压执行过程
             */

    }

    /**
     * 主函数
     */
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://192.168.229.111:9000");
        String remoteFilePath = "/data/20160620.zip"; // HDFS路径

        try {
            System.out.println("读取文件: " + remoteFilePath);
            readfile.cat(conf, remoteFilePath);
            System.out.println("\n读取完成");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
