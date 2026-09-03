import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object BatchProcessor {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Pharmacy Sales and Stock Monitoring - Phase 3")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val productPath = "data/reference/product_master.csv"
    val eventPath = "data/input/pharmacy_events.csv"
    val analysisBase = "data/output/analysis"

    Utils.printSection("PHARMACY SALES AND STOCK MONITORING - PHASE 3")

    // ------------------------------------------------------------
    // 1. READ EVENTS + ACCUMULATOR
    // ------------------------------------------------------------

    val malformedEventAccumulator =
      sc.longAccumulator("Malformed Pharmacy Events")

    val rawEventLines = sc.textFile(eventPath)
      .filter(line => !line.startsWith("event_id"))

    val eventsRDD = rawEventLines
      .flatMap { line =>
        val fields = Utils.parseCsvLine(line)

        if (
          fields.length == 5 &&
          Utils.isValidEventType(fields(4).toUpperCase) &&
          fields(3).trim.matches("-?\\d+")
        ) {
          Some(
            PharmacyEvent(
              eventId = fields(0).trim,
              timestamp = fields(1).trim,
              productId = fields(2).trim,
              quantity = Utils.safeInt(fields(3)),
              eventType = fields(4).trim.toUpperCase
            )
          )
        } else {
          malformedEventAccumulator.add(1)
          None
        }
      }
      .persist(StorageLevel.MEMORY_ONLY)

    val eventCount = eventsRDD.count()

    println(s"Valid events: $eventCount")
    println(
      s"Malformed/invalid events: ${malformedEventAccumulator.value}"
    )

    println(
      s"eventsRDD storage level: ${eventsRDD.getStorageLevel.description}"
    )

    // ------------------------------------------------------------
    // 2. DATASET DEMONSTRATION
    // ------------------------------------------------------------

    val eventDS = eventsRDD.toDS()

    Utils.printSection("DATASET DEMONSTRATION")

    println(s"Dataset record count: ${eventDS.count()}")

    eventDS
      .select("eventId", "productId", "quantity", "eventType")
      .show(5, truncate = false)

    // ------------------------------------------------------------
    // 3. PRODUCT MASTER DATAFRAME
    // ------------------------------------------------------------

    val productDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(productPath)

    // Convert case-class camelCase columns into snake_case once.
    // This keeps all later Spark SQL/DataFrame operations consistent.
    val eventDF = eventsRDD.toDF()
      .withColumnRenamed("eventId", "event_id")
      .withColumnRenamed("productId", "product_id")
      .withColumnRenamed("eventType", "event_type")
      .withColumn(
        "event_time",
        to_timestamp(
          col("timestamp"),
          "yyyy-MM-dd HH:mm:ss"
        )
      )

    Utils.printSection("DATAFRAME SCHEMAS")

    productDF.printSchema()
    eventDF.printSchema()

    // ------------------------------------------------------------
    // 4. TEMPORARY VIEWS + SPARK SQL
    // ------------------------------------------------------------

    productDF.createOrReplaceTempView("products")
    eventDF.createOrReplaceTempView("pharmacy_events")

    val sqlSalesDF = spark.sql(
      """
        |SELECT
        |  product_id,
        |  SUM(quantity) AS total_sold
        |FROM pharmacy_events
        |WHERE event_type = 'SALE'
        |GROUP BY product_id
        |ORDER BY product_id
        |""".stripMargin
    )

    Utils.printSection("SPARK SQL - TOTAL SALES")

    sqlSalesDF.show(false)

    // ------------------------------------------------------------
    // 5. DATAFRAME AGGREGATIONS
    // ------------------------------------------------------------

    val salesDF = eventDF
      .filter(col("event_type") === "SALE")
      .groupBy("product_id")
      .agg(
        sum("quantity").alias("total_sold")
      )

    val restockDF = eventDF
      .filter(col("event_type") === "RESTOCK")
      .groupBy("product_id")
      .agg(
        sum("quantity").alias("total_restocked")
      )

    // ------------------------------------------------------------
    // 6. JOIN PRODUCT MASTER + AGGREGATES
    // ------------------------------------------------------------

    val stockAnalysisDF = productDF
      .join(
        salesDF,
        Seq("product_id"),
        "left"
      )
      .join(
        restockDF,
        Seq("product_id"),
        "left"
      )
      .na.fill(
        0,
        Seq("total_sold", "total_restocked")
      )

    Utils.printSection("JOIN + AGGREGATION")

    stockAnalysisDF
      .select(
        "product_id",
        "product_name",
        "category",
        "initial_stock",
        "reorder_level",
        "total_sold",
        "total_restocked"
      )
      .orderBy("product_id")
      .show(false)

    // ------------------------------------------------------------
    // 7. UDF FOR STOCK STATUS
    // ------------------------------------------------------------

    val classifyStock = udf {
      (
        currentStock: Int,
        reorderLevel: Int
      ) =>
        if (currentStock <= 0) {
          "STOCK_OUT"
        } else if (currentStock < reorderLevel) {
          "BELOW_REORDER"
        } else if (currentStock <= reorderLevel * 2) {
          "AT_RISK"
        } else {
          "ADEQUATE"
        }
    }

    val finalStockDF = stockAnalysisDF
      .withColumn(
        "current_stock",
        col("initial_stock") -
          col("total_sold") +
          col("total_restocked")
      )
      .withColumn(
        "stock_status",
        classifyStock(
          col("current_stock"),
          col("reorder_level")
        )
      )

    Utils.printSection("UDF + CURRENT STOCK")

    finalStockDF
      .select(
        "product_id",
        "product_name",
        "initial_stock",
        "total_sold",
        "total_restocked",
        "current_stock",
        "reorder_level",
        "stock_status"
      )
      .orderBy("product_id")
      .show(false)

    // ------------------------------------------------------------
    // 8. WINDOW FUNCTION
    // ------------------------------------------------------------

    val saleEventsDF = eventDF
      .filter(col("event_type") === "SALE")
      .select(
        col("event_id"),
        col("event_time"),
        col("product_id"),
        col("quantity")
      )

    val salesWindow = Window
      .partitionBy("product_id")
      .orderBy(col("event_time"))
      .rowsBetween(-2, Window.currentRow)

    val recentSalesDF = saleEventsDF
      .withColumn(
        "recent_3_sale_quantity",
        sum(col("quantity")).over(salesWindow)
      )
      .withColumn(
        "recent_3_sale_events",
        count(lit(1)).over(salesWindow)
      )
      .withColumn(
        "recent_average_sale",
        round(
          col("recent_3_sale_quantity") /
            col("recent_3_sale_events"),
          2
        )
      )

    Utils.printSection("WINDOW FUNCTION - RECENT SALES")

    recentSalesDF
      .orderBy("product_id", "event_time")
      .show(false)

    // ------------------------------------------------------------
    // 9. CACHE / PERSIST DATAFRAME
    // ------------------------------------------------------------

    val cachedStockDF = finalStockDF
      .persist(StorageLevel.MEMORY_ONLY)

    println(
      s"cachedStockDF storage level: " +
        cachedStockDF.storageLevel.description
    )

    val stockCount = cachedStockDF.count()

    val alertCount = cachedStockDF
      .filter(
        col("stock_status") =!= "ADEQUATE"
      )
      .count()

    println(s"Products in stock analysis: $stockCount")
    println(s"Products requiring attention: $alertCount")

    // ------------------------------------------------------------
    // 10. partitionBy() ON PAIR RDD
    // ------------------------------------------------------------

    val eventPairRDD = eventsRDD
      .map(event => (event.productId, event.eventId))

    val partitionedEventRDD = eventPairRDD
      .partitionBy(
        new HashPartitioner(2)
      )

    println(
      "Partitions after partitionBy(2): " +
        partitionedEventRDD.getNumPartitions
    )

    val partitionedEventOutput = partitionedEventRDD
      .mapValues(
        eventId => s"event=$eventId"
      )

    println(
      "Partitioner preserved by mapValues: " +
        partitionedEventOutput.partitioner.isDefined
    )

    // ------------------------------------------------------------
    // 11. repartition() AND coalesce()
    // ------------------------------------------------------------

    val repartitionedDF =
      cachedStockDF.repartition(
        4,
        col("product_id")
      )

    val coalescedDF =
      repartitionedDF.coalesce(1)

    println(
      "Original stock partitions: " +
        cachedStockDF.rdd.getNumPartitions
    )

    println(
      "After repartition(4): " +
        repartitionedDF.rdd.getNumPartitions
    )

    println(
      "After coalesce(1): " +
        coalescedDF.rdd.getNumPartitions
    )

    // ------------------------------------------------------------
    // 12. RDD LINEAGE / DAG
    // ------------------------------------------------------------

    Utils.printSection("RDD LINEAGE / DAG DEBUG STRING")

    val salesPairRDD = eventsRDD
      .filter(
        event => event.eventType == "SALE"
      )
      .map(
        event => (event.productId, event.quantity)
      )

    val totalSalesRDD =
      salesPairRDD.reduceByKey(_ + _)

    println(totalSalesRDD.toDebugString)

    // Trigger the RDD action so the reduceByKey computation is
    // actually executed and visible in the Spark DAG.
    val totalSalesCollected = totalSalesRDD.collect()

    println("RDD total sales:")
    totalSalesCollected
      .sortBy(_._1)
      .foreach {
        case (productId, totalSold) =>
          println(s"$productId -> $totalSold")
      }

    // ------------------------------------------------------------
    // 13. OUTPUT DIRECTORIES
    // ------------------------------------------------------------

    deleteOutputPath(
      sc,
      analysisBase
    )

    val stockAnalysisOutput =
      s"$analysisBase/stock_analysis"

    val sqlSalesOutput =
      s"$analysisBase/sql_sales"

    val recentSalesOutput =
      s"$analysisBase/recent_sales_window"

    val partitionedOutput =
      s"$analysisBase/partitioned_events"

    val partitionedStockOutput =
      s"$analysisBase/stock_partitioned_by_category"

    val repartitionedOutput =
      s"$analysisBase/repartitioned_stock"

    val coalescedOutput =
      s"$analysisBase/coalesced_stock"

    // ------------------------------------------------------------
    // 14. WRITE STOCK ANALYSIS
    // ------------------------------------------------------------

    cachedStockDF
      .select(
        "product_id",
        "product_name",
        "category",
        "initial_stock",
        "total_sold",
        "total_restocked",
        "current_stock",
        "reorder_level",
        "stock_status"
      )
      .orderBy("product_id")
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(stockAnalysisOutput)

    // ------------------------------------------------------------
    // 15. WRITE SQL OUTPUT
    // ------------------------------------------------------------

    sqlSalesDF
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(sqlSalesOutput)

    // ------------------------------------------------------------
    // 16. WRITE WINDOW OUTPUT
    // ------------------------------------------------------------

    recentSalesDF
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(recentSalesOutput)

    // ------------------------------------------------------------
    // 17. WRITE partitionBy() RDD OUTPUT
    // ------------------------------------------------------------

    partitionedEventOutput
      .saveAsTextFile(partitionedOutput)

    // ------------------------------------------------------------
    // 18. WRITE DATAFRAME partitionBy() OUTPUT
    // ------------------------------------------------------------

    cachedStockDF
      .write
      .mode("overwrite")
      .option("header", "true")
      .partitionBy("category")
      .csv(partitionedStockOutput)

    // ------------------------------------------------------------
    // 19. WRITE repartition() OUTPUT
    // ------------------------------------------------------------

    repartitionedDF
      .orderBy("product_id")
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(repartitionedOutput)

    // ------------------------------------------------------------
    // 20. WRITE coalesce() OUTPUT
    // ------------------------------------------------------------

    coalescedDF
      .orderBy("product_id")
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(coalescedOutput)

    // ------------------------------------------------------------
    // 21. FINAL SUMMARY
    // ------------------------------------------------------------

    Utils.printSection("PHASE 3 ANALYSIS SUMMARY")

    println("DATAFRAME / DATASET:")
    println("1. DataFrame creation from CSV")
    println("2. Dataset creation from case class")
    println("3. Temporary views")
    println("4. Spark SQL")
    println("5. Joins")
    println("6. Aggregations")
    println("7. Window functions")
    println("8. UDF")
    println("9. DataFrame persistence")

    println()
    println("RDD / PERFORMANCE:")
    println("1. partitionBy()")
    println("2. mapValues()")
    println("3. repartition()")
    println("4. coalesce()")
    println("5. persist()")
    println("6. accumulator")
    println("7. RDD lineage")

    println()
    println("SHUFFLE / WIDE OPERATIONS:")
    println("1. reduceByKey()")
    println("2. partitionBy()")
    println("3. groupBy / aggregation")
    println("4. join")
    println("5. window processing")
    println("6. repartition()")
    println("7. orderBy()")

    println()
    println(
      s"Phase 3 outputs written to: $analysisBase"
    )

    // ------------------------------------------------------------
    // 22. CLEANUP
    // ------------------------------------------------------------

    cachedStockDF.unpersist()
    eventsRDD.unpersist()

    spark.stop()
  }

  private def deleteOutputPath(
      sc: org.apache.spark.SparkContext,
      path: String
  ): Unit = {

    val fileSystem =
      FileSystem.get(sc.hadoopConfiguration)

    val outputPath = new Path(path)

    if (fileSystem.exists(outputPath)) {
      fileSystem.delete(outputPath, true)
    }
  }
}
