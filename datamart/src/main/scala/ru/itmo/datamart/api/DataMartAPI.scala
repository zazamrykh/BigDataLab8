package ru.itmo.datamart.api

import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import ru.itmo.datamart.config.Config
import org.slf4j.LoggerFactory

class DataMartAPI(spark: SparkSession, config: Config) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val mssqlConfig = config.getMSSQLConfig()
  private val nutrimentFeatures = config.getNutrimentFeatures()
  private val dataSaver = new ru.itmo.datamart.saver.DataSaver(spark, config)

  def getPreprocessedData(): DataFrame = {
    logger.info(s"Getting preprocessed data from MS SQL Server: ${mssqlConfig.server}:${mssqlConfig.port}/${mssqlConfig.database}")

    try {
      // Формируем URL для подключения к MS SQL Server
      val jdbcUrl = s"jdbc:sqlserver://${mssqlConfig.server}:${mssqlConfig.port};databaseName=${mssqlConfig.database};encrypt=false;trustServerCertificate=true"

      // Настройки подключения
      val connectionProperties = new java.util.Properties()
      connectionProperties.put("user", mssqlConfig.username)
      connectionProperties.put("password", mssqlConfig.password)
      connectionProperties.put("driver", mssqlConfig.driver)

      // Загружаем данные из таблицы
      val df = spark.read
        .jdbc(
          url = jdbcUrl,
          table = mssqlConfig.outputTable,
          properties = connectionProperties
        )

      // Валидируем данные
      val validatedDf = validateData(df)

      logger.info(s"Preprocessed data successfully loaded and validated: ${validatedDf.count()} records")
      validatedDf
    } catch {
      case e: Exception =>
        logger.error(s"Error getting preprocessed data from MS SQL Server: ${e.getMessage}", e)
        throw e
    }
  }

  def saveClusteringResults(df: DataFrame): DataFrame = {
    logger.info("Saving clustering results through Data Mart API")

    // Проверяем наличие колонок features и scaled_features
    val dfColumns = df.columns
    logger.info(s"Input DataFrame columns: ${dfColumns.mkString(", ")}")

    // Удаляем колонки features и scaled_features, если они есть
    val columnsToRemove = Seq("features", "scaled_features")
    val dfToSave = columnsToRemove.foldLeft(df) { (currentDf, colName) =>
      if (currentDf.columns.contains(colName)) {
        logger.info(s"Removing column: $colName")
        currentDf.drop(colName)
      } else {
        currentDf
      }
    }

    // Вызываем правильный метод для сохранения результатов кластеризации
    dataSaver.saveClusteringResults(dfToSave)
    dfToSave
  }

  def saveClusterCenters(clusterCenters: Array[Array[Double]], featureNames: Array[String]): DataFrame = {
    logger.info("Saving cluster centers through Data Mart API")

    // Создаем DataFrame из центров кластеров
    import spark.implicits._

    // Создаем список строк для DataFrame
    val rows = clusterCenters.zipWithIndex.flatMap { case (center, clusterId) =>
      center.zipWithIndex.map { case (value, featureIdx) =>
        (clusterId + 1, featureNames(featureIdx), value)
      }
    }

    // Создаем DataFrame
    val dfCenters = rows.toSeq.toDF("cluster_id", "feature_name", "feature_value")
      .withColumn("created_at", org.apache.spark.sql.functions.current_timestamp())

    // Сохраняем центры кластеров
    dataSaver.saveClusterCenters(clusterCenters, featureNames)

    dfCenters
  }

  private def validateData(df: DataFrame): DataFrame = {
    logger.info("Validating data")

    // Получаем список признаков
    val availableFeatures = nutrimentFeatures.filter(df.columns.contains)

    if (availableFeatures.isEmpty) {
      logger.warn("No configured nutriment features found in DataFrame columns")
      return df
    }

    // Проверяем только на наличие значений (не NULL)
    var validatedDf = df

    // Проверяем, что все строки имеют хотя бы одно ненулевое значение признака
    val nonZeroCondition = availableFeatures.map(feature => F.col(feature) > 0).reduce(_ || _)
    validatedDf = validatedDf.filter(nonZeroCondition)

    // Логируем результаты валидации
    val originalCount = df.count()
    val validatedCount = validatedDf.count()
    val removedCount = originalCount - validatedCount
    val pct = if (originalCount > 0) (removedCount.toDouble / originalCount * 100) else 0.0

    logger.info(
      s"Validation: Removed $removedCount rows (${pct.formatted("%.2f")}%); kept $validatedCount/$originalCount."
    )

    validatedDf
  }
}
