package hadoop;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * Created by asus on 2020/7/4.
 * user:cs
 * 描述：上传文件到hdfs上
 */

public class upload {

    //声明两个从不同文件系统类型的静态变量
    private static FileSystem fs = null;
    private static FileSystem local = null;
    private static int POOL_NUM = 1;

    public static void listFile(String srcPath, final String dstPath) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        //读取配置文件
        Configuration conf = new Configuration();
        //指定HDFS地址
        URI uri = new URI("hdfs://192.168.229.111:9000");
        fs = FileSystem.get(uri, conf);
        // 获取本地文件系统
        local = FileSystem.getLocal(conf);
        //获取文件目录
        FileStatus[] listFile = local.globStatus(new Path(srcPath), new RegxAcceptPathFilter("^.*pdf$"));
        //获取文件路径
        Path[] listPath = FileUtil.stat2Paths(listFile);
        //输出文件路径
        //循环遍历所有文件路径
        for (final Path p : listPath) {
            ExecutorService executorService = Executors.newFixedThreadPool(1);
            for (int i = 0; i < POOL_NUM; i++) {
                executorService.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
                            while (listFiles.hasNext()) {
                                LocatedFileStatus next = listFiles.next();
                                String filename = "/data/" + next.getPath().getName();
                                if (fs.exists(new Path(filename))) {
                                    System.out.println("文件已存在");
                                } else {
                                    Path outPath = new Path(dstPath);
                                    fs.copyFromLocalFile(p, outPath);
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
            executorService.shutdown();
        }
    }

    public static void main(String[] args) throws Exception ,InterruptedException {
        //指定在元数据目录的地址在linux环境下
        final String srcPath = "C:\\Users\\asus\\Desktop\\data\\*";
        final String dstPath = "hdfs://192.168.229.111:9000/data/";
        //调用上传到HDFS
        listFile(srcPath, dstPath);
    }
}

