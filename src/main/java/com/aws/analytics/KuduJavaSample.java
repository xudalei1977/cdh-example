package com.aws.analytics;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import org.apache.kudu.Schema;

public class KuduJavaSample {

    private KuduClient kuduClient = null;
    private static final String KUDU_MASTER = "cdh-master-1:7051,cdh-master-2:7051,cdh-master-3:7051";

    private ColumnSchema newColumnSchema(String name, Type type, boolean isKey) {
        ColumnSchema.ColumnSchemaBuilder column = new ColumnSchema.ColumnSchemaBuilder(name, type);
        column.key(isKey);
        return column.build();
    }

    public void init() {
        kuduClient = new KuduClient.KuduClientBuilder(KUDU_MASTER)
                .defaultOperationTimeoutMs(10000)
                .build();

        System.out.println(" ******** KuduJavaSample -> init() successfule ! ");
    }

    public void insertKuduSingleData(String tableName) throws KuduException {

        KuduTable kuduTable = kuduClient.openTable(tableName);
        KuduSession kuduSession = kuduClient.newSession();

        Insert insert = kuduTable.newInsert();
        PartialRow insertRow = insert.getRow();

        insertRow.addInt("inv_item_sk", 111005);
        insertRow.addInt("inv_warehouse_sk", 11);
        insertRow.addString("inv_date_sk", "2450166");
        insertRow.addInt("inv_date_sk", 111);

        kuduSession.apply(insert);
        kuduSession.apply(insert);

        kuduSession.close();
    }

    public void insertKuduBatchData(String tableName, String[] dataArr) throws KuduException {

        KuduTable kuduTable = kuduClient.openTable(tableName);
        KuduSession kuduSession = kuduClient.newSession();

        kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        kuduSession.setMutationBufferSpace(1000);

        for(int i =0; i < dataArr.length; i++) {
            String dataStr = dataArr[i];
            String[] dataInfo = dataStr.split(",");

            Insert insert = kuduTable.newInsert();
            PartialRow row = insert.getRow();

            if(dataInfo.length == 4) {
                row.addInt("inv_item_sk", Integer.valueOf(dataInfo[0]));
                row.addInt("inv_warehouse_sk", Integer.valueOf(dataInfo[1]));
                row.addString("inv_date_sk", dataInfo[2]);
                row.addInt("inv_quantity_on_hand", Integer.valueOf(dataInfo[3]));
            }
            kuduSession.apply(insert);
        }

        kuduSession.flush();
        kuduSession.close();
    }

    public void queryKuduFullData(String tableName) throws KuduException {

        KuduTable kuduTable = kuduClient.openTable(tableName);
        KuduScanner.KuduScannerBuilder scannerBuilder = kuduClient.newScannerBuilder(kuduTable);
        KuduScanner kuduScanner = scannerBuilder.build();

        int i = 0;
        while (kuduScanner.hasMoreRows()){
            i++;
            System.out.println("tablet index : " + i);

            RowResultIterator rowResults = kuduScanner.nextRows();

            while (rowResults.hasNext()){
                RowResult rowResult = rowResults.next();
                System.out.println(
                                " inv_item_sk = " + rowResult.getInt("inv_item_sk") +
                                ", inv_warehouse_sk = " + rowResult.getInt("inv_warehouse_sk") +
                                ", inv_date_sk = " + rowResult.getString("inv_date_sk") +
                                ", inv_quantity_on_hand = " + rowResult.getInt("inv_quantity_on_hand")
                );
            }
        }
    }

    public void queryKuduData(String tableName) throws KuduException{

        KuduTable kuduTable = kuduClient.openTable("aa_users");
        KuduScanner.KuduScannerBuilder scannerBuilder = kuduClient.newScannerBuilder(kuduTable);

        scannerBuilder.setProjectedColumnNames(Arrays.asList("id","age"));
        scannerBuilder.addPredicate(
                KuduPredicate.newComparisonPredicate(
                        newColumnSchema("id", Type.INT32,true),
                        KuduPredicate.ComparisonOp.GREATER,
                        150//id>150
                )
        );

        scannerBuilder.addPredicate(
                KuduPredicate.newComparisonPredicate(
                        newColumnSchema("age",Type.INT8,false),
                        KuduPredicate.ComparisonOp.LESS,
                        (byte)25
                )
        );

        KuduScanner kuduScanner = scannerBuilder.build();
        int i = 0;
        while (kuduScanner.hasMoreRows()){
            i++;
            System.out.println("tablet index = " + i);
            //获取每个tablet中扫描的数据
            RowResultIterator rowResults = kuduScanner.nextRows();
            //遍历每个Tablet中的数据
            while (rowResults.hasNext()){
                RowResult rowResult = rowResults.next();
                System.out.println(
                        "id = " + rowResult.getInt("id") +
                                ", age = " + rowResult.getByte("age")
                );
            }
        }

    }

    public void updateKuduData(String tableName) throws KuduException{

        KuduTable kuduTable = kuduClient.openTable(tableName);
        KuduSession kuduSession = kuduClient.newSession();

        Update newUpdate = kuduTable.newUpdate();
        PartialRow updateRow = newUpdate.getRow();

        updateRow.addInt("inv_item_sk", 111005);
        updateRow.addInt("inv_warehouse_sk", 11);
        updateRow.addString("inv_date_sk", "2450166");
        updateRow.addInt("inv_quantity_on_hand", 111);

        kuduSession.apply(newUpdate);
        kuduSession.flush();

        kuduSession.close();
    }

    public void upsertKuduData(String tableName) throws KuduException{

        KuduTable kuduTable = kuduClient.openTable(tableName);
        KuduSession kuduSession = kuduClient.newSession();

        Upsert newUpsert = kuduTable.newUpsert();
        PartialRow upsertRow = newUpsert.getRow();

        upsertRow.addInt("inv_item_sk", 111005);
        upsertRow.addInt("inv_warehouse_sk", 11);
        upsertRow.addString("inv_date_sk", "2450166");
        upsertRow.addInt("inv_quantity_on_hand", 111);

        kuduSession.apply(newUpsert);
        kuduSession.flush();

        kuduSession.close();
    }

    public void deleteKuduData(String tableName) throws KuduException{

        KuduTable kuduTable = kuduClient.openTable(tableName);
        KuduSession kuduSession = kuduClient.newSession();

        Delete newDelete = kuduTable.newDelete();
        PartialRow deleteRow = newDelete.getRow();

        deleteRow.addInt("inv_item_sk",111005);
        deleteRow.addInt("inv_warehouse_sk",11);
        deleteRow.addString("inv_date_sk","2450166");

        kuduSession.apply(newDelete);
        kuduSession.flush();

        kuduSession.close();
    }

    public void close() {
        try {
            if (kuduClient != null) {
                kuduClient.close();
            System.out.println(" ******** KuduJavaSample -> close() successfule ! ");
            }
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    public void createTable(String tableName) {

        List<ColumnSchema> columns = new ArrayList<>();

        columns.add(new ColumnSchema.ColumnSchemaBuilder("inv_item_sk", Type.INT32).key(true).
                compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("inv_warehouse_sk", Type.INT32).key(true).
                compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("inv_date_sk", Type.STRING).key(true).
                compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("inv_quantity_on_hand", Type.INT32).
                compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());

        Schema schema = new Schema(columns);

        //add hash partition
        CreateTableOptions createTableOptions = new CreateTableOptions();

        List<String> hashKeys = new ArrayList<>();
        hashKeys.add("inv_item_sk");
        hashKeys.add("inv_warehouse_sk");
        int numBuckets = 8;

        createTableOptions.addHashPartitions(hashKeys, numBuckets);

        //add range partition
        List<String> parcols = new ArrayList<>();
        parcols.add("inv_date_sk");
        createTableOptions.setRangePartitionColumns(parcols);

        PartialRow lower1 = schema.newPartialRow();
        lower1.addString("inv_date_sk", "0000000");
        PartialRow upper1 = schema.newPartialRow();
        upper1.addString("inv_date_sk", "2452000");
        createTableOptions.addRangePartition(lower1, upper1);

        PartialRow lower2 = schema.newPartialRow();
        lower2.addString("inv_date_sk", "2452000");
        PartialRow upper2 = schema.newPartialRow();
        upper2.addString("inv_date_sk", "9999999");
        createTableOptions.addRangePartition(lower2, upper2);

        try {
            if(!kuduClient.tableExists(tableName)) {
                kuduClient.createTable(tableName, schema, createTableOptions);
            }
            System.out.println(" ******** KuduJavaSample -> createTable() successfule ! ");
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    public void upsert(String tableName, String[] dataArr) {
        try {
            KuduTable kuduTable = kuduClient.openTable(tableName);
            KuduSession kuduSession = kuduClient.newSession();

            kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
            for(int i =0; i < dataArr.length; i++) {
                String dataStr = dataArr[i];
                Upsert upsert = kuduTable.newUpsert();
                PartialRow row = upsert.getRow();
                String[] dataInfo = dataStr.split(",");
                if(dataInfo.length == 4) {
                    row.addInt("inv_item_sk", Integer.valueOf(dataInfo[0]));
                    row.addInt("inv_warehouse_sk", Integer.valueOf(dataInfo[1]));
                    row.addString("inv_date_sk", dataInfo[2]);
                    row.addInt("inv_quantity_on_hand", Integer.valueOf(dataInfo[3]));
                }
                kuduSession.apply(upsert);
            }
            kuduSession.flush();
            kuduSession.close();
            System.out.println(" ******** KuduJavaSample -> upsert() successfule ! ");
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    public void scanerTable(String tableName) {
        try {
            KuduTable kuduTable = kuduClient.openTable(tableName);
            KuduScanner kuduScanner = kuduClient.newScannerBuilder(kuduTable).build();
            while(kuduScanner.hasMoreRows()) {
                RowResultIterator rowResultIterator = kuduScanner.nextRows();
                while (rowResultIterator.hasNext()) {
                    RowResult rowResult = rowResultIterator.next();
                    System.out.println(" ******** KuduJavaSample -> scanerTable()  with id : " + rowResult.getInt("inv_item_sk"));
                }
            }
            kuduScanner.close();
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    public void dropTable(String tableName) {
        try {
            if(kuduClient.tableExists(tableName))
                kuduClient.deleteTable(tableName);
            System.out.println(" ******** KuduJavaSample -> dropTable() successfule ! ");
        } catch (KuduException e) {
            e.printStackTrace();
        }

    }

    public void tableList() {
        try {
            ListTablesResponse listTablesResponse = kuduClient.getTablesList();
            List<String> tblist = listTablesResponse.getTablesList();
            for(String tableName : tblist) {
                System.out.println(" ******** KuduJavaSample -> tableList() : " + tableName);
            }
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        KuduJavaSample kuduJavaSample = new KuduJavaSample();
        kuduJavaSample.init();

        String tableName = "inventory_1";

        kuduJavaSample.dropTable(tableName);
        kuduJavaSample.createTable(tableName);

        kuduJavaSample.tableList();

        String[] dataArr = {"111005,11,2450166,111", "122005,12,2450166,222", "133005,13,2450166,333"};
        kuduJavaSample.upsert(tableName, dataArr);

        kuduJavaSample.scanerTable(tableName);

        kuduJavaSample.close();
    }

}

//java -cp cdh-example-1.0-SNAPSHOT.jar:kudu-client-1.10.0-cdh6.3.2.jar:kudu-spark2_2.11-1.10.0-cdh6.3.2.jar com.aws.analytics.KuduJavaSample

