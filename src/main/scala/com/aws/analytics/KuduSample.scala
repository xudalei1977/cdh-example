package com.aws.analytics

import com.aws.analytics.conf.Config
import com.aws.analytics.util.SparkHelper
import org.apache.log4j.{Level, Logger}
import org.slf4j.LoggerFactory

import collection.JavaConverters._
import org.apache.kudu.client._
import org.apache.kudu.spark.kudu._
import org.apache.kudu.Schema
import org.apache.kudu.Type
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{lit, col}
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, StringType}


object KuduSample {

  private val log = LoggerFactory.getLogger("KuduSample")

  def main(args: Array[String]): Unit = {

    log.info(args.mkString)
    Logger.getLogger("org").setLevel(Level.WARN)

    val parmas = Config.parseConfig(KuduSample, args)
    val spark = SparkHelper.getSparkSession(parmas.env)

    val tableName = parmas.tableName
    val kuduMaster = parmas.kuduMaster
//    val tableName = "inventory_1"
//    val kuduMaster = "cdh-master-1:7051,cdh-master-2:7051,cdh-master-3:7051"

    val kuduContext = new KuduContext(kuduMaster, spark.sparkContext)
    import spark.implicits._

    // The schema of the table we're going to create.
//    val columns = List(
//      new ColumnSchemaBuilder("inv_item_sk", Type.INT32).key(true).build(),
//      new ColumnSchemaBuilder("inv_warehouse_sk", Type.INT32).key(true).build(),
//      new ColumnSchemaBuilder("inv_date_sk", Type.STRING).key(true).build(),
//      new ColumnSchemaBuilder("inv_quantity_on_hand", Type.INT32).build()
//    )
//    val schema = new Schema(columns.asJava)

    val schema_df = StructType(
      List(
        StructField("inv_item_sk", IntegerType, false),
        StructField("inv_warehouse_sk", IntegerType, false),
        StructField("inv_date_sk", StringType, false),
        StructField("inv_quantity_on_hand", IntegerType, false)
      )
    )

    val schema = kuduContext.createSchema(schema_df, Seq("inv_item_sk", "inv_warehouse_sk", "inv_date_sk"))

    try {
      // Delete the table if it already exists.
      if(kuduContext.tableExists(tableName)) {
        kuduContext.deleteTable(tableName)
      }

      // Create table with hash and range partitions.
      val lower1 = schema.newPartialRow
      lower1.addString("inv_date_sk", "0000000")
      val upper1: PartialRow = schema.newPartialRow
      upper1.addString("inv_date_sk", "2452000")

      val lower2: PartialRow = schema.newPartialRow
      lower2.addString("inv_date_sk", "2452000")
      val upper2: PartialRow = schema.newPartialRow
      upper2.addString("inv_date_sk", "9999999")

      kuduContext.createTable(tableName, schema,
                            new CreateTableOptions().
                                addHashPartitions(List("inv_item_sk", "inv_warehouse_sk").asJava, 8).
                                setRangePartitionColumns(List("inv_date_sk").asJava).
                                addRangePartition(lower1, upper1).
                                addRangePartition(lower2, upper2).
                                setNumReplicas(3))

      //Write to the table.
      val write_arr = Seq(Row(46, 1, "2451536", 898),
                          Row(50, 1, "2451536", 321),
                          Row(80, 1, "2451536", 608),
                          Row(100,1, "2451536", 614),
                          Row(109,1, "2451536", 641),
                          Row(112,1, "2451536", 371),
                          Row(26, 1, "2451536", 319),
                          Row(38, 1, "2451536", 875),
                          Row(91, 1, "2451536", 917),
                          Row(98, 1, "2451536", 238))

      val write_rdd = spark.sparkContext.parallelize(write_arr)
      val write_df = spark.createDataFrame(write_arr.asJava, schema_df)

      kuduContext.insertRows(write_df, tableName)

      // Read from the table using an RDD.
      val read_cols = Seq("inv_item_sk", "inv_warehouse_sk", "inv_date_sk", "inv_quantity_on_hand")
      val rdd = kuduContext.kuduRDD(spark.sparkContext, tableName, read_cols)
      rdd.map { case Row(inv_item_sk: Int, inv_warehouse_sk: Int, inv_date_sk: String, inv_quantity_on_hand: Int) => (inv_item_sk, inv_warehouse_sk, inv_date_sk, inv_quantity_on_hand) }.
          collect().foreach(println(_))

      // Upsert some rows.
      val upsert_df = write_df.withColumn("inv_quantity_on_hand", col("inv_quantity_on_hand") + lit(1000))
      kuduContext.upsertRows(upsert_df, tableName)

      // Read the table in SparkSql.
      val read_df = spark.read.option("kudu.master", kuduMaster).
                    option("kudu.table", tableName).
                    format("kudu").load
      read_df.createOrReplaceTempView(tableName)
      spark.sqlContext.sql(s"select * from $tableName").show

      //Using Join expression
      upsert_df.join(read_df, upsert_df("inv_item_sk") === read_df("inv_item_sk") &&
                              upsert_df("inv_warehouse_sk") === read_df("inv_warehouse_sk") &&
                              upsert_df("inv_date_sk") === read_df("inv_date_sk"), "inner" ).show

    } catch {
      case unknown : Throwable => log.error(s"got an exception: " + unknown)
        throw unknown
    } finally {
      log.info(s"deleting table '$tableName'")
      kuduContext.deleteTable(tableName)

      log.info(s"closing down the session")
      spark.close()
    }

  }

}

//spark-submit --master yarn \
//--deploy-mode client \
//--jars ./kudu-client-1.10.0-cdh6.3.2.jar,./kudu-spark2_2.11-1.10.0-cdh6.3.2.jar,./scopt_2.11-3.7.1.jar \
//--class com.aws.analytics.KuduSample ./cdh-example-1.0-SNAPSHOT.jar \
//-e prod \
//-k cdh-master-1:7051,cdh-master-2:7051,cdh-master-3:7051 \
//-t inventory_1