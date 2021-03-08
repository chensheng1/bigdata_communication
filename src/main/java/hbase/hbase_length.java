package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class hbase_length {
    private HBaseAdmin admin = null;
    // 定义配置对象HBaseConfiguration
    private static Configuration configuration;
    public hbase_length() throws Exception {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","192.168.229.111:2181,192.168.229.112:2181,192.168.229.113:2181");  //hbase 服务地址
    //   configuration.set("hbase.zookeeper.property.clientPort","2181"); //端口号
        admin = new HBaseAdmin(configuration);
    }
    // Hbase获取所有的表信息
    public List getAllTables() {
        List<String> tables = null;
        if (admin != null) {
            try {
                HTableDescriptor[] allTable = admin.listTables();
                if (allTable.length > 0)
                    tables = new ArrayList<String>();
                for (HTableDescriptor hTableDescriptor : allTable) {
                    tables.add(hTableDescriptor.getNameAsString());
                    System.out.println(hTableDescriptor.getNameAsString());
                }
            }catch (IOException e) {
                e.printStackTrace();
            }
        }
        return tables;
    }



    //hbase获取列的信息
    public void clom() throws IOException {
        List<String> tables=getAllTables();
        for(int i=0;i<tables.size();i++){
            TableName tb=TableName.valueOf(tables.get(i));
            HTable table= new HTable(configuration, tb);
            List<String> list=new ArrayList<String>();
            HTableDescriptor hTableDescriptor=table.getTableDescriptor();
            for(HColumnDescriptor fdescriptor : hTableDescriptor.getColumnFamilies()){
                list.add(fdescriptor.getNameAsString());
                System.out.println(fdescriptor.getNameAsString());
            }
            for(int x=0;x<list.size();x++) {
                System.out.println(list.get(x));
            }
        }

        admin.close();
    }

    public static void scan() throws IOException {
        String tablename="user_action";
        //读取表
        HTable table = new HTable(configuration,tablename);
        //全表扫描
        Scan scan=new Scan();
        //区间扫描
        scan.setStartRow("2".getBytes());
        scan.setStopRow("5".getBytes());
        ResultScanner scanner = table.getScanner(scan);
        //result  是与一行数据（有多个列族，多个列）
        for (Result result : scanner) {
            System.out.println(Bytes.toString(result.getRow()));
            System.out.println(Bytes.toString(result.getValue("f1".getBytes(),"name".getBytes())));

        }
        //关闭连接
        table.close();
    }

    public void getrow() throws IOException{
        String tablename="user_action";
        Get get = new Get(Bytes.toBytes("3"));
        HTable table = new HTable(configuration,tablename);
        Result result=table.get(get);
        List<Cell> listcells=result.listCells();
        for(Cell cell: listcells){
            System.out.println("列  族：" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列  名:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("列  值：" + Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("时间戳：" + cell.getTimestamp());
        }
        table.close();
    }

    //遍历全部数据
    public void getall() throws IOException {
        String tablename="user_action";
        HTable table = new HTable(configuration,tablename);
        ResultScanner resultScanner = table.getScanner(new Scan());    //针对全表的查询器
        Iterator<Result> results = resultScanner.iterator();
        System.out.println("1111");
        while(results.hasNext()){
            Result result=results.next();
            List<Cell> cells = result.listCells();
            for(Cell cell : cells) {
                System.out.println("列  族：" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列  名:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("列  值：" + Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println("时间戳：" + cell.getTimestamp() + "\n------------------");
            }
        }
    }

    //插入数据
    public void put() throws IOException {
        String tablename="user_action";
        HTable table = new HTable(configuration,tablename);
        Put put = new Put(Bytes.toBytes("2"));
        put.add("f1".getBytes(),"province".getBytes(),"湖北".getBytes());
        put.add("f1".getBytes(),"item_id".getBytes(),"400".getBytes());
        table.put(put);
    }

    public void delete() throws IOException {
        String tablename="user_action";
        HTable table = new HTable(configuration,tablename);
        Delete delete = new Delete(Bytes.toBytes("2"));
        table.delete(delete);
        table.close();
    }

    public void istable() throws IOException {
        String tablename="user_action";
        boolean result = admin.tableExists(TableName.valueOf(tablename));
        System.out.println(result);
    }


    public static void main(String[] args) throws Exception {
        hbase_length hbaseTest = new hbase_length();
        //hbaseTest.getAllTables();
        //hbaseTest.clom();
        //hbaseTest.put();
        //hbaseTest.getrow();
        //hbaseTest.scan();
        hbaseTest.getall();
        //hbaseTest.istable();
    }
}