package hbase;
/**
 * Created by asus on 2020/8/14.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Created by cwj on 17-10-26.
 *
 */

public class hbase_intoindex extends BaseRegionObserver {

    private static Configuration configuration = HBaseConfiguration.create();
    private HBaseAdmin admin = null;
    public hbase_intoindex() throws Exception {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","192.168.229.111:2181,192.168.229.112:2181,192.168.229.113:2181");  //hbase 服务地址
        //   configuration.set("hbase.zookeeper.property.clientPort","2181"); //端口号
        admin = new HBaseAdmin(configuration);
    }
    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
            throws IOException {

        HTable indexTable = new HTable(configuration, "user_action");

        List<Cell> cells = put.get("f1".getBytes(), "province".getBytes());
        Iterator<Cell> cellIterator = cells.iterator();
        while (cellIterator.hasNext()) {
            Cell cell = cellIterator.next();
            Put indexPut = new Put(CellUtil.cloneValue(cell));
            indexPut.add("f1".getBytes(), "province".getBytes(), CellUtil.cloneRow(cell));
            indexTable.put(indexPut);
        }
    }


}