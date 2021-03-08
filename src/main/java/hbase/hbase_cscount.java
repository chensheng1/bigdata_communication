package hbase;

/**
 * Created by asus on 2020/8/19.
 */


import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.DoubleColumnInterpreter;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class hbase_cscount {
    private static HBaseAdmin admin = null;
    // 定义配置对象HBaseConfiguration
    private static Configuration configuration;
    public hbase_cscount() throws Exception {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","192.168.229.111:2181,192.168.229.112:2181,192.168.229.113:2181");  //hbase 服务地址
        //   configuration.set("hbase.zookeeper.property.clientPort","2181"); //端口号
        admin = new HBaseAdmin(configuration);
    }

    public static void rowCountByCoprocessor(String tablename){
        try {
            TableName name=TableName.valueOf(tablename);
            //先disable表，添加协处理器后再enable表
            System.out.println(name);
            admin.disableTable(name);
            System.out.println("111");
            HTableDescriptor descriptor = admin.getTableDescriptor(name);
            String coprocessorClass = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation";
            if (! descriptor.hasCoprocessor(coprocessorClass)) {
                descriptor.addCoprocessor(coprocessorClass);
            }
            admin.modifyTable(name, descriptor);
            admin.enableTable(name);

            //计时
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();

            Scan scan = new Scan();
            AggregationClient aggregationClient = new AggregationClient(configuration);

            System.out.println("RowCount: " + aggregationClient.rowCount(name, new LongColumnInterpreter(), scan));
            stopWatch.stop();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    /** 统计表行数*/
    public static long rowCount(String tableName, String family) throws Throwable {
        AggregationClient ac = new AggregationClient(configuration);
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(family));
        scan.setFilter(new FirstKeyOnlyFilter());
        long rowCount = 0;
        rowCount = ac.rowCount(TableName.valueOf(tableName), new LongColumnInterpreter(), scan);

        return rowCount;
    }

    /** 求和*/
    public static double sum(String tableName, String family, String qualifier) throws Throwable {
        AggregationClient ac = new AggregationClient(configuration);
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        double sum = 0;
        sum = ac.sum(TableName.valueOf(tableName), new DoubleColumnInterpreter(), scan);
        return sum;
    }

    /** 求平均值*/
    public static double avg(String tableName, String family, String qualifier) throws Throwable {
        AggregationClient ac = new AggregationClient(configuration);
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        double avg = 0;
        avg = ac.avg(TableName.valueOf(tableName), new DoubleColumnInterpreter(), scan);
        return avg;
    }


    public static void main(String[] args) throws Throwable {
        hbase_cscount hbaseTest = new hbase_cscount();
       // hbaseTest.getrow();
        double count =hbaseTest.avg("user_action","f1","item_id");
        System.out.println(count);
    }
}
