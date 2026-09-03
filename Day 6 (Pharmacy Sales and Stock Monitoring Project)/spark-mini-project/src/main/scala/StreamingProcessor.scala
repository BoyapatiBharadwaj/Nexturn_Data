import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingProcessor {

  case class StreamEvent(
      eventId: String,
      timestamp: String,
      productId: String,
      quantity: Int,
      eventType: String
  )

  def main(args: Array[String]): Unit = {

    val inputDirectory = "data/streaming/input"
    val checkpointDirectory = "data/streaming/checkpoint"
    val outputDirectory = "data/output/streaming"

    val conf = new SparkConf()
      .setAppName("Pharmacy Sales and Stock Monitoring - DStreams")
      .setMaster("local[*]")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")

    val ssc = new StreamingContext(conf, Seconds(5))

    ssc.sparkContext.setLogLevel("WARN")

    // ------------------------------------------------------------
    // 1. LOAD PRODUCT MASTER
    // ------------------------------------------------------------

    val productLines = ssc.sparkContext
      .textFile("data/reference/product_master.csv")

    val products = productLines
      .filter(line => !line.startsWith("product_id"))
      .map { line =>
        val fields = Utils.parseCsvLine(line)

        Product(
          productId = fields(0),
          productName = fields(1),
          category = fields(2),
          reorderLevel = Utils.safeInt(fields(3)),
          initialStock = Utils.safeInt(fields(4))
        )
      }
      .collect()

    val productMap = products
      .map(product => product.productId -> product)
      .toMap

    // Broadcast small product master.
    val productBroadcast =
      ssc.sparkContext.broadcast(productMap)

    println()
    println("=" * 70)
    println("PHARMACY SALES AND STOCK MONITORING - DSTREAMS")
    println("=" * 70)
    println(s"Products loaded: ${productMap.size}")
    println("Batch interval: 5 seconds")
    println("Sales window: 15 seconds")
    println("Window slide: 5 seconds")
    println(s"Watching directory: $inputDirectory")
    println("=" * 70)

    // ------------------------------------------------------------
    // 2. FILE STREAM
    // ------------------------------------------------------------

    // Spark watches this directory for NEW files.
    // Files already present before startup may not be treated as
    // new streaming input, so copy test files after starting.
    val rawLinesDStream =
      ssc.textFileStream(inputDirectory)

    // ------------------------------------------------------------
    // 3. PARSE STREAM EVENTS
    // ------------------------------------------------------------

    val eventsDStream = rawLinesDStream
      .filter(line => !line.startsWith("event_id"))
      .flatMap { line =>
        val fields = Utils.parseCsvLine(line)

        if (
          fields.length == 5 &&
          Utils.isValidEventType(fields(4).toUpperCase) &&
          fields(3).trim.matches("-?\\d+")
        ) {
          Some(
            StreamEvent(
              eventId = fields(0),
              timestamp = fields(1),
              productId = fields(2),
              quantity = Utils.safeInt(fields(3)),
              eventType = fields(4).toUpperCase
            )
          )
        } else {
          None
        }
      }

    // ------------------------------------------------------------
    // 4. STOCK DELTA DSTREAM
    // ------------------------------------------------------------

    // SALE     -> negative stock movement
    // RESTOCK  -> positive stock movement
    val stockDeltaDStream = eventsDStream
      .map { event =>
        val delta =
          if (event.eventType == "SALE") {
            -event.quantity
          } else {
            event.quantity
          }

        (event.productId, delta)
      }

    // ------------------------------------------------------------
    // 5. STATEFUL STOCK PROCESSING
    // ------------------------------------------------------------

    // The state stores cumulative net stock movement.
    //
    // Example:
    //   SALE 10     -> -10
    //   RESTOCK 5   -> -5
    //
    // Initial stock is added later using product master data.

    val updateStockState =
      (
          newValues: Seq[Int],
          previousState: Option[Int]
      ) => {

        val currentValue =
          previousState.getOrElse(0)

        val newMovement =
          newValues.sum

        Some(currentValue + newMovement)
      }

    // updateStateByKey is a stateful DStream operation.
    val stockStateDStream =
      stockDeltaDStream.updateStateByKey(updateStockState)

    // Checkpointing is required for reliable state recovery.
    ssc.checkpoint(checkpointDirectory)

    // ------------------------------------------------------------
    // 6. SALES STREAM
    // ------------------------------------------------------------

    val saleDStream = eventsDStream
      .filter(
        event => event.eventType == "SALE"
      )
      .map(
        event => (event.productId, event.quantity)
      )

    // ------------------------------------------------------------
    // 7. SLIDING WINDOW
    // ------------------------------------------------------------

    // 15-second window with a 5-second slide.
    //
    // This provides recent sales activity over multiple
    // micro-batches.
    val recentSalesWindowDStream =
      saleDStream.reduceByKeyAndWindow(
        (x: Int, y: Int) => x + y,
        Seconds(15),
        Seconds(5)
      )

    // ------------------------------------------------------------
    // 8. PROCESS STOCK STATE EACH MICRO-BATCH
    // ------------------------------------------------------------

    stockStateDStream.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        val stateMap =
          rdd.collect().toMap

        val stockResults =
          productMap.values.toSeq
            .map { product =>

              val netMovement =
                stateMap.getOrElse(
                  product.productId,
                  0
                )

              val currentStock =
                product.initialStock + netMovement

              val stockStatus =
                if (currentStock <= 0) {
                  "STOCK_OUT"
                } else if (
                  currentStock < product.reorderLevel
                ) {
                  "BELOW_REORDER"
                } else if (
                  currentStock <=
                    product.reorderLevel * 2
                ) {
                  "AT_RISK"
                } else {
                  "ADEQUATE"
                }

              (
                product.productId,
                product.productName,
                currentStock,
                product.reorderLevel,
                stockStatus
              )
            }

        val timestamp =
          System.currentTimeMillis()

        val outputPath =
          s"$outputDirectory/stock_state_$timestamp"

        val outputRDD =
          rdd.sparkContext
            .parallelize(stockResults)
            .map {
              case (
                  productId,
                  productName,
                  currentStock,
                  reorderLevel,
                  status
              ) =>
                s"$productId,$productName," +
                s"$currentStock," +
                s"$reorderLevel," +
                status
            }

        outputRDD
          .coalesce(1)
          .saveAsTextFile(outputPath)

        println()
        println("-" * 70)
        println(
          s"STATE MICRO-BATCH: $timestamp"
        )
        println("-" * 70)

        stockResults
          .sortBy(_._1)
          .foreach {
            case (
                productId,
                productName,
                currentStock,
                reorderLevel,
                status
            ) =>
              println(
                s"$productId - $productName " +
                s"current=$currentStock " +
                s"reorder=$reorderLevel " +
                s"status=$status"
              )
          }

        println(
          s"State output: $outputPath"
        )
      }
    }

    // ------------------------------------------------------------
    // 9. PROCESS SALES SLIDING WINDOW
    // ------------------------------------------------------------

    recentSalesWindowDStream.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        val timestamp =
          System.currentTimeMillis()

        val windowOutputPath =
          s"$outputDirectory/sales_window_$timestamp"

        val windowResults =
          rdd
            .sortByKey()
            .map {
              case (productId, quantity) =>
                val productName =
                  productBroadcast.value
                    .get(productId)
                    .map(_.productName)
                    .getOrElse("Unknown")

                s"$productId,$productName,$quantity"
            }

        windowResults
          .coalesce(1)
          .saveAsTextFile(windowOutputPath)

        println()
        println(
          s"SALES SLIDING WINDOW: $timestamp"
        )

        rdd
          .sortByKey()
          .collect()
          .foreach {
            case (productId, quantity) =>
              val productName =
                productBroadcast.value
                  .get(productId)
                  .map(_.productName)
                  .getOrElse("Unknown")

              println(
                s"$productId - $productName " +
                s"recent_sales=$quantity"
              )
          }

        println(
          s"Window output: $windowOutputPath"
        )
      }
    }

    // ------------------------------------------------------------
    // 10. START STREAMING
    // ------------------------------------------------------------

    ssc.start()

    println()
    println("=" * 70)
    println("DSTREAM APPLICATION STARTED")
    println("=" * 70)
    println("Copy NEW CSV files into:")
    println(inputDirectory)
    println()
    println("The application processes files every 5 seconds.")
    println("Stop with Ctrl+C.")
    println("=" * 70)

    ssc.awaitTermination()

    productBroadcast.destroy()
  }
}
