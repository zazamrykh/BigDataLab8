package ru.itmo.datamart

import org.apache.spark.sql.SparkSession
import ru.itmo.datamart.config.Config
import ru.itmo.datamart.loader.ParquetLoader
import ru.itmo.datamart.preprocessor.DataPreprocessor
import ru.itmo.datamart.saver.DataSaver
import ru.itmo.datamart.api.{DataMartAPI, HttpServer}
import org.slf4j.LoggerFactory

object Main {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    logger.info("Starting Food Data Mart application")

    // Парсим аргументы командной строки
    val mode = if (args.length > 0) args(0) else "etl"

    // Загружаем конфигурацию
    val config = new Config()

    // Создаем Spark сессию
    val spark = createSparkSession(config)

    try {
      mode match {
        case "etl" =>
          runETLMode(spark, config)
          // Останавливаем Spark-сессию после завершения ETL
          spark.stop()
          logger.info("Spark session stopped")
        case "api" =>
          // В режиме API не останавливаем Spark-сессию
          runAPIMode(spark, config)
          // Этот код не будет выполнен, так как runAPIMode не возвращается
        case _ =>
          logger.error(s"Unknown mode: $mode. Supported modes: etl, api")
          System.exit(1)
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error in application: ${e.getMessage}", e)
        spark.stop()
        logger.info("Spark session stopped due to error")
        throw e
    }
  }

  private def runETLMode(spark: SparkSession, config: Config): Unit = {
    logger.info("Running in ETL mode")

    // Загружаем данные из parquet
    logger.info("Loading data from parquet")
    val parquetLoader = new ParquetLoader(spark, config)
    val df = parquetLoader.loadData()

    // Предобработка данных
    logger.info("Preprocessing data")
    val preprocessor = new DataPreprocessor(config)
    val preprocessedDf = preprocessor.preprocessData(df)

    // Сохраняем предобработанные данные в MSSQL
    logger.info("Saving preprocessed data to MS SQL Server")
    val dataSaver = new DataSaver(spark, config)
    dataSaver.savePreprocessedData(preprocessedDf)

    logger.info("ETL process completed successfully")
  }

  private def runAPIMode(spark: SparkSession, config: Config): Unit = {
    logger.info("Running in API mode")

    // Запускаем HTTP-сервер
    logger.info("Starting HTTP server...")
    val server = new HttpServer(spark, config)

    // Получаем порт из конфигурации или используем порт по умолчанию
    val port = try {
      config.getDataConfig().apiPort
    } catch {
      case _: Exception => 8080
    }

    server.start(host = "0.0.0.0", port = port)

    logger.info(s"HTTP server started on port $port")

    // Бесконечный цикл, чтобы приложение не завершалось
    // Это блокирует поток, но в контейнере Docker это нормально
    while (true) {
      try {
        Thread.sleep(10000)  // Спим 10 секунд
      } catch {
        case _: InterruptedException =>
          logger.info("Sleep interrupted, exiting...")
          return
      }
    }
  }

  private def createSparkSession(config: Config): SparkSession = {
    val sparkConfig = config.getSparkConfig()

    val builder = SparkSession.builder()
      .appName(sparkConfig.appName)
      .master(sparkConfig.master)
      .config("spark.driver.memory", sparkConfig.driverMemory)
      .config("spark.executor.memory", sparkConfig.executorMemory)
      .config("spark.executor.cores", sparkConfig.executorCores.toString)
      .config("spark.executor.instances", sparkConfig.numExecutors.toString)
      .config("spark.sql.shuffle.partitions", sparkConfig.shufflePartitions.toString)
      .config("spark.default.parallelism", sparkConfig.defaultParallelism.toString)
      .config("spark.serializer", sparkConfig.serializer)

    // Добавляем JDBC драйвер для MS SQL Server
    builder.config("spark.driver.extraClassPath", "/opt/mssql-jdbc.jar")
      .config("spark.executor.extraClassPath", "/opt/mssql-jdbc.jar")

    val spark = builder.getOrCreate()

    logger.info(s"Created Spark session: ${spark.version}")

    spark
  }
}
