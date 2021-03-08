package hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class hbase_map extends TableMapper<ImmutableBytesWritable, Put> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //将 fruit 的 name 和 color 提取出来，相当于将每一行数据读取出来放入到 Put对象中
        Put put = new Put(key.get());
        //遍历column
        for (Cell cell : value.rawCells()) {
            if("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))){
                if("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                    //将info:name加入到put对象中
                    put.add(cell);
                }else if("color".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                    //将info:color加入到put对象中
                    put.add(cell);
                }
            }
        }
        //将从fruit表中读取到的每行数据写入到context中作为map的输出
        context.write(key,put);
    }
}