package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by asus on 2020/8/12.
 */
public class hbase_filter {
    private HBaseAdmin admin = null;
    // 定义配置对象HBaseConfiguration
    private static Configuration configuration;
    public hbase_filter() throws Exception {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","192.168.229.111:2181,192.168.229.112:2181,192.168.229.113:2181");  //hbase 服务地址
        //   configuration.set("hbase.zookeeper.property.clientPort","2181"); //端口号
        admin = new HBaseAdmin(configuration);
    }

    //单值过滤
    public void filter() throws Exception {
        String tablename="user_action";
        //读取表
        HTable table = new HTable(configuration,tablename);
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        //hbase 存的是字节数组
        SingleColumnValueFilter filter =
                new SingleColumnValueFilter(Bytes.toBytes("f1"), Bytes.toBytes("province"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("湖北"));

        filterList.addFilter(filter);
        Scan scan = new Scan();
        scan.setFilter(filterList);
        scan.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("province"));//一定要有
        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            //r中有一行相关信息，需要什么可以取什么
            for (Cell cell : r.rawCells()) {
                System.out.println("Rowkey : " + Bytes.toString(r.getRow()) + "" +
                        "   Familiy:Quilifier : " + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        "   Value : " + Bytes.toString(CellUtil.cloneValue(cell)) +
                        "   Time : " + cell.getTimestamp());
            }
        }
        table.close();//不要忘记释放资源
    }

    //区间过滤
    public void selectrow() throws IOException {
        String tablename="user_action";
        //读取表
        HTable table = new HTable(configuration,tablename);
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        //组成区间过滤
        SingleColumnValueFilter filter1 =
                new SingleColumnValueFilter(Bytes.toBytes("f1"), Bytes.toBytes("item_id"), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(1));
        SingleColumnValueFilter filter2 =
                new SingleColumnValueFilter(Bytes.toBytes("f1"), Bytes.toBytes("item_id"), CompareFilter.CompareOp.LESS_OR_EQUAL,Bytes.toBytes(100000));

        filterList.addFilter(filter1);
        filterList.addFilter(filter2);
        Scan scan = new Scan();
        scan.setFilter(filterList);
        scan.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("item_category"));//追加列族和列
        ResultScanner rs = table.getScanner(scan);
        System.out.println(rs);
        for (Result r : rs) {
            System.out.println(r);
            for (Cell cell : r.rawCells()) {
                System.out.println("Rowkey : " + Bytes.toString(r.getRow()) + "" +
                        "   Familiy:Quilifier : " + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        "   Value : " + Bytes.toString(CellUtil.cloneValue(cell)) +
                        "   Time : " + cell.getTimestamp());
            }
        }
        table.close();//一定要释放资源
    }

    //值过滤
    public void valuefilter() throws IOException{
        String tablename="user_action";
        //读取表
        HTable table = new HTable(configuration,tablename);
        Scan scan = new Scan();

        System.out.println("只列出值包含data1的列");
        Filter filter1 = new ValueFilter(CompareFilter.CompareOp.EQUAL,
                new SubstringComparator("湖北"));
        scan.setFilter(filter1);
        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            //r中有一行相关信息，需要什么可以取什么
            for (Cell cell : r.rawCells()) {
                System.out.println("Rowkey : " + Bytes.toString(r.getRow()) + "" +
                        "   Familiy:Quilifier : " + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        "   Value : " + Bytes.toString(CellUtil.cloneValue(cell)) +
                        "   Time : " + cell.getTimestamp());
            }
        }
        table.close();//不要忘记释放资源
    }

    //行过滤
    public void rowfilter() throws IOException {
        String tablename="user_action";
        //读取表
        HTable table = new HTable(configuration,tablename);
       // Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator(".*01$"));
        //Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator("201407"));
        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new BinaryPrefixComparator("123".getBytes()));
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            //r中有一行相关信息，需要什么可以取什么
            for (Cell cell : r.rawCells()) {
                System.out.println("Rowkey : " + Bytes.toString(r.getRow()) + "" +
                        "   Familiy:Quilifier : " + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        "   Value : " + Bytes.toString(CellUtil.cloneValue(cell)) +
                        "   Time : " + cell.getTimestamp());
            }
        }
        table.close();//不要忘记释放资源
    }

    //多条件查询
    public void lostfilter() throws IOException {
        String tablename="user_action";
        //读取表
        HTable table = new HTable(configuration,tablename);
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        Filter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL,
                new SubstringComparator("湖北"));
        SingleColumnValueFilter filter1 =
                new SingleColumnValueFilter(Bytes.toBytes("f1"), Bytes.toBytes("item_id"), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(1));
        SingleColumnValueFilter filter2 =
                new SingleColumnValueFilter(Bytes.toBytes("f1"), Bytes.toBytes("item_id"), CompareFilter.CompareOp.LESS_OR_EQUAL,Bytes.toBytes(100000));
        filterList.addFilter(filter);
        filterList.addFilter(filter1);
        filterList.addFilter(filter2);
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes("1"));
        PageFilter pf = new PageFilter(5L);
        filterList.addFilter(pf);
        scan.setFilter(filterList);
        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            //r中有一行相关信息，需要什么可以取什么
            for (Cell cell : r.rawCells()) {
                System.out.println("Rowkey : " + Bytes.toString(r.getRow()) + "" +
                        "   Familiy:Quilifier : " + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        "   Value : " + Bytes.toString(CellUtil.cloneValue(cell)) +
                        "   Time : " + cell.getTimestamp());
            }
        }
        table.close();//不要忘记释放资源
    }

    public static void main(String[] args) throws Exception {
        hbase_filter hbaseTest = new hbase_filter();
        //hbaseTest.filter();
        //hbaseTest.selectrow();
        //hbaseTest.valuefilter();
        //hbaseTest.rowfilter();
        hbaseTest.lostfilter();
    }
}

