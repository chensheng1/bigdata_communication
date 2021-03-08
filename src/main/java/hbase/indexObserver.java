package hbase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Created by cwj on 17-10-26.
 *
 */

public class indexObserver extends BaseRegionObserver {

    private static final byte[] TABLE_NAME = Bytes.toBytes("user_action");
    private static final byte[] COLUMN_FAMILY = Bytes.toBytes("f1");
    private static final byte[] COLUMN = Bytes.toBytes("province");

    private Configuration configuration = HBaseConfiguration.create();

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
            throws IOException {

        HTable indexTable = new HTable(configuration, TABLE_NAME);

        List<Cell> cells = put.get(COLUMN_FAMILY, COLUMN);
        Iterator<Cell> cellIterator = cells.iterator();
        while (cellIterator.hasNext()) {
            Cell cell = cellIterator.next();
            Put indexPut = new Put(CellUtil.cloneValue(cell));
            indexPut.add(COLUMN_FAMILY, COLUMN, CellUtil.cloneRow(cell));
            indexTable.put(indexPut);
        }
    }


}