package hbase;

/**
 * user:cs
 * Created by asus on 2020/7/19.
 * 描述：将本体数据写入hbase中
 */

/**
public class input extends Thread {
    public Configuration config;
    public Htable table;
    public HBaseAdamin admin;
    public input(){
        config =  HBaseConfiguration.create();
        try {
            table = new HTable(config, Bytes.toBytes("user_action"));
            admin = new HBaseAdmin(config);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void importLocalFileToHBase(String fileName) {
        long st = System.currentTimeMillis();
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(
                    fileName)));
            String line = null;
            int count = 0;
            while ((line = br.readLine()) != null) {
                count++;
                put(line);
                if (count % 10000 == 0)
                    System.out.println(count);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {

            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            try {
                table.flushCommits();
                table.close(); // must close the client
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        long en2 = System.currentTimeMillis();
        System.out.println("Total Time: " + (en2 - st) + " ms");
    }

    @SuppressWarnings("deprecation")
    public void put(String line) throws IOException {
        String[] arr = line.split("\t", -1);
        String[] column = {"id","uid","item_id","behavior_type","item_category","date","province"};

        if (arr.length == 7) {
            Put put = new Put(Bytes.toBytes(arr[0]));// rowkey
            for(int i=1;i<arr.length;i++){
                put.add(Bytes.toBytes("f1"), Bytes.toBytes(column[i]),Bytes.toBytes(arr[i]));
            }
            table.put(put); // put to server
        }
    }

    public void get(String rowkey, String columnFamily, String column,
                    int versions) throws IOException {
        long st = System.currentTimeMillis();

        Get get = new Get(Bytes.toBytes(rowkey));
        get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));

        Scan scanner = new Scan(get);
        scanner.setMaxVersions(versions);

        ResultScanner rsScanner = table.getScanner(scanner);

        for (Result result : rsScanner) {
            final List<KeyValue> list = result.list();
            for (final KeyValue kv : list) {
                System.out.println(Bytes.toStringBinary(kv.getValue()) + "\t"
                        + kv.getTimestamp()); // mid + time
            }

        }
        rsScanner.close();

        long en2 = System.currentTimeMillis();
        System.out.println("Total Time: " + (en2 - st) + " ms");
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {       //第一个参数是该jar所使用的类，第二个参数是数据集所存放的路径
            throw new Exception("You must set input path!");
        }

        String fileName = args[args.length-1];  //输入的文件路径是最后一个参数
        input test = new input();
        test.importLocalFileToHBase(fileName);
    }
}
*/