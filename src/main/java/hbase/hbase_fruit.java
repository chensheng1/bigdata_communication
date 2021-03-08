package hbase;

/**

public class hbase_fruit extends Configured implements Tool {
    public static Configuration conf = null;
    public static Connection conn = null;
    public static Table table = null;
    //静态代码块设置配置文件信息、获得连接
    //但是，获得table对象需要传入tablename，所有的DML操作连接的zk的配置和连接用的同一个可拿出来
    //但tableName不同，得到的table对象不同，因此在具体的方法中获得table对象
    static{
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","192.168.229.111:2181,192.168.229.112:2181,192.168.229.113:2181");
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int run(String[] strings) throws Exception {
        //获得配置
        Configuration conf = this.getConf();
        //创建任务,并配置
        Job job = Job.getInstance(conf,this.getClass().getSimpleName());
        job.setJarByClass(hbase_fruit.class);
        //配置 Job
        Scan scan = new Scan();
        scan.setCacheBlocks(false);
        scan.setCaching(500);
        //设置 Mapper，注意导入的是 mapreduce 包下的，不是 mapred 包下的，后者是老版本
        TableMapReduceUtil.initTableMapperJob(
                "fruit", //数据源的表名
                scan, //scan 扫描控制器
                hbase_map.class,//设置 Mapper 类
                ImmutableBytesWritable.class,//设置 Mapper 输出 key 类型
                Put.class,//设置 Mapper 输出 value 值类型
                job//设置给哪个 JOB
        );
        //设置 Reducer
        TableMapReduceUtil.initTableReducerJob("fruit_mr", hbase_reduce.class, job);
        //设置 Reduce 数量，最少 1 个
        job.setNumReduceTasks(1);
        boolean isSuccess = job.waitForCompletion(true);
        System.out.println(isSuccess);
        if(!isSuccess){

            throw new IOException("Job running with error");
        }
        return isSuccess ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        int status = ToolRunner.run(conf, new hbase_fruit(), args);
        System.exit(status);

    }

}

 **/