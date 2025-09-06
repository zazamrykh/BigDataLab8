package ru.itmo.datamart.saver

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import ru.itmo.datamart.config.Config
import org.slf4j.LoggerFactory

class DataSaver(spark: SparkSession, config: Config) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val mssqlConfig = config.getMSSQLConfig()

  // Метод для сохранения предобработанных данных
  def savePreprocessedData(df: DataFrame): Unit = {
    logger.info(s"Saving preprocessed data to MS SQL Server: ${mssqlConfig.server}:${mssqlConfig.port}/${mssqlConfig.database}")

    try {
      // Выводим схему данных для отладки
      logger.info("DataFrame schema before saving:")
      df.printSchema()

      // Проверяем, какие колонки есть в DataFrame
      val dfColumns = df.columns
      logger.info(s"DataFrame columns: ${dfColumns.mkString(", ")}")

      // Определяем колонки, которые есть в таблице food_products
      val validColumns = Seq("code", "product_name", "energy-kcal", "fat", "carbohydrates", "proteins", "sugars")
      val existingColumns = validColumns.filter(dfColumns.contains)

      logger.info(s"Columns to save: ${existingColumns.mkString(", ")}")

      // Выбираем только нужные колонки
      val dfToSave = df.select(existingColumns.map(col): _*)
        .withColumn("created_at", current_timestamp())

      // Формируем URL для подключения к MS SQL Server
      val jdbcUrl = s"jdbc:sqlserver://${mssqlConfig.server}:${mssqlConfig.port};databaseName=${mssqlConfig.database};encrypt=false;trustServerCertificate=true"

      // Настройки подключения
      val connectionProperties = new java.util.Properties()
      connectionProperties.put("user", mssqlConfig.username)
      connectionProperties.put("password", mssqlConfig.password)
      connectionProperties.put("driver", mssqlConfig.driver)

      logger.info(s"Saving data to table: ${mssqlConfig.outputTable}")

      // Сохраняем данные в таблицу
      dfToSave.write
        .mode("append")  // Добавляем данные к существующей таблице
        .jdbc(
          url = jdbcUrl,
          table = mssqlConfig.outputTable,
          connectionProperties
        )

      logger.info(s"Preprocessed data successfully saved to ${mssqlConfig.outputTable}")
    } catch {
      case e: Exception =>
        logger.error(s"Error saving preprocessed data to MS SQL Server: ${e.getMessage}", e)
        throw e
    }
  }

  def saveClusteringResults(df: DataFrame): Unit = {
    logger.info(s"Saving clustering results to MS SQL Server: ${mssqlConfig.server}:${mssqlConfig.port}/${mssqlConfig.database}")

    try {
      // Выводим схему данных для отладки
      logger.info("DataFrame schema before saving clustering results:")
      df.printSchema()

      // Проверяем, какие колонки есть в DataFrame
      val dfColumns = df.columns
      logger.info(s"DataFrame columns: ${dfColumns.mkString(", ")}")

      // Определяем идентификатор продукта
      val dfWithProductId = if (df.columns.contains("id")) {
        df.withColumn("product_id", col("id").cast(IntegerType))
      } else if (df.columns.contains("code")) {
        // Используем хеш-функцию для преобразования кода продукта в INT
        // abs(hash(code) % 2147483647) гарантирует, что значение будет в пределах INT
        df.withColumn("product_id",
          when(
            col("code").isNotNull,
            abs(hash(col("code")) % 2147483647).cast(IntegerType)
          )
          // Если не удалось преобразовать, используем автоинкрементный id
          .otherwise(monotonically_increasing_id().cast(IntegerType))
        )
      } else {
        // Если нет ни id, ни code — создаем уникальный id
        df.withColumn("product_id", monotonically_increasing_id().cast(IntegerType))
      }

      // Приводим названия колонок к тем, что в таблице SQL
      val dfToSave = dfWithProductId.select(
        col("product_id").cast(IntegerType),
        col("cluster").cast(IntegerType).alias("cluster_id"),
        current_timestamp().alias("created_at")
      )

      // Формируем URL для подключения к MS SQL Server
      val jdbcUrl = s"jdbc:sqlserver://${mssqlConfig.server}:${mssqlConfig.port};databaseName=${mssqlConfig.database};encrypt=false;trustServerCertificate=true"

      // Настройки подключения
      val connectionProperties = new java.util.Properties()
      connectionProperties.put("user", mssqlConfig.username)
      connectionProperties.put("password", mssqlConfig.password)
      connectionProperties.put("driver", mssqlConfig.driver)

      logger.info(s"Saving clustering results to table: ${mssqlConfig.clusteringResultsTable}")

      // Сохраняем данные в таблицу
      dfToSave.write
        .mode("append")  // Добавляем данные к существующей таблице
        .jdbc(
          url = jdbcUrl,
          table = mssqlConfig.clusteringResultsTable,
          connectionProperties
        )

      logger.info(s"Clustering results successfully saved to ${mssqlConfig.clusteringResultsTable}")
    } catch {
      case e: Exception =>
        logger.error(s"Error saving clustering results to MS SQL Server: ${e.getMessage}", e)
        throw e
    }
  }

  def saveClusterCenters(clusterCenters: Array[Array[Double]], featureNames: Array[String]): Unit = {
    logger.info(s"Saving cluster centers to MS SQL Server: ${mssqlConfig.server}:${mssqlConfig.port}/${mssqlConfig.database}")

    try {
      // Создаем DataFrame из массива центров кластеров
      import spark.implicits._

      // Создаем список строк для DataFrame
      val rows = clusterCenters.zipWithIndex.flatMap { case (center, clusterId) =>
        center.zipWithIndex.map { case (value, featureIdx) =>
          (clusterId + 1, featureNames(featureIdx), value)
        }
      }

      // Создаем DataFrame
      val dfCenters = rows.toSeq.toDF("cluster_id", "feature_name", "feature_value")
        .withColumn("created_at", current_timestamp())

      // Формируем URL для подключения к MS SQL Server
      val jdbcUrl = s"jdbc:sqlserver://${mssqlConfig.server}:${mssqlConfig.port};databaseName=${mssqlConfig.database};encrypt=false;trustServerCertificate=true"

      // Настройки подключения
      val connectionProperties = new java.util.Properties()
      connectionProperties.put("user", mssqlConfig.username)
      connectionProperties.put("password", mssqlConfig.password)
      connectionProperties.put("driver", mssqlConfig.driver)

      logger.info(s"Saving cluster centers to table: ${mssqlConfig.clusterCentersTable}")

      // Сохраняем данные в таблицу
      dfCenters.write
        .mode("append")  // Добавляем данные к существующей таблице
        .jdbc(
          url = jdbcUrl,
          table = mssqlConfig.clusterCentersTable,
          connectionProperties
        )

      logger.info(s"Cluster centers successfully saved to ${mssqlConfig.clusterCentersTable}")
    } catch {
      case e: Exception =>
        logger.error(s"Error saving cluster centers to MS SQL Server: ${e.getMessage}", e)
        throw e
    }
  }
}
