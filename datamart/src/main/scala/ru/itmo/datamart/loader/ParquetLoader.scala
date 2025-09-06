package ru.itmo.datamart.loader

import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.itmo.datamart.config.Config
import org.slf4j.LoggerFactory

class ParquetLoader(spark: SparkSession, config: Config) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val dataConfig = config.getDataConfig()

  def loadData(): DataFrame = {
    val parquetPath = dataConfig.parquetPath
    logger.info(s"Loading data from parquet file: $parquetPath")

    try {
      // Загружаем данные из parquet файла с использованием sample для уменьшения объема данных
      logger.info("Using sample(false, 0.1) to reduce data volume during reading")
      val sampledDf = spark.read.parquet(parquetPath).sample(withReplacement = false, fraction = 0.1)

      // Применяем ограничение на количество строк, если указано
      val limitedDf = if (dataConfig.sampleSize > 0) {
        logger.info(s"Limiting data to ${dataConfig.sampleSize} rows")
        sampledDf.limit(dataConfig.sampleSize)
      } else {
        sampledDf
      }

      // Выводим схему данных и колонки
      logger.info("DataFrame schema (loaded from parquet):")
      limitedDf.printSchema()

      // Выводим список колонок
      val columns = limitedDf.columns
      logger.info(s"DataFrame columns: ${columns.mkString(", ")}")

      // Подсчитываем количество записей
      val count = limitedDf.count()
      logger.info(s"Data successfully loaded from parquet: $count records")

      limitedDf
    } catch {
      case e: Exception =>
        logger.error(s"Error loading data from parquet: ${e.getMessage}", e)
        throw e
    }
  }
}
