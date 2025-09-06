package ru.itmo.datamart.preprocessor

import org.apache.spark.sql.{DataFrame, functions => F}
import org.apache.spark.sql.types.DoubleType
import ru.itmo.datamart.config.Config
import org.slf4j.LoggerFactory

class DataPreprocessor(config: Config) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val preprocessingConfig = config.getPreprocessingConfig()
  private val nutrimentFeatures = config.getNutrimentFeatures()

  def preprocessData(df: DataFrame): DataFrame = {
    logger.info("Starting data preprocessing")

    // Определяем целевые признаки
    logger.info(s"Target nutriment features: ${nutrimentFeatures.mkString(", ")}")

    // Выводим список колонок исходного DataFrame
    val originalColumns = df.columns
    logger.info(s"Original DataFrame columns: ${originalColumns.mkString(", ")}")

    // 1) Преобразуем колонку product_name из сложного типа в строку, если она есть
    var processedDf = df
    if (processedDf.columns.contains("product_name")) {
      // Проверяем тип колонки product_name
      val productNameType = processedDf.schema("product_name").dataType.toString
      logger.info(s"product_name column type: $productNameType")

      if (productNameType.startsWith("ArrayType")) {
        logger.info("Converting product_name from array to string")
        // Преобразуем массив структур в строку, выбирая первый элемент массива и поле text
        processedDf = processedDf.withColumn("product_name",
          F.when(F.size(F.col("product_name")) > 0,
            F.col("product_name").getItem(0).getField("text")
          ).otherwise(F.lit(null)))
      }
    } else {
      logger.warn("product_name column not found in DataFrame")
    }

    // 2) Извлекаем питательные вещества из колонки nutriments
    if (processedDf.columns.contains("nutriments")) {
      logger.info("Extracting nutriment features from 'nutriments' column")

      // Извлекаем каждый признак из колонки nutriments
      for (feature <- nutrimentFeatures) {
        // Создаем варианты имени признака (с разными разделителями)
        val variants = Seq(
          feature,
          feature.replace("_", "-"),
          feature.replace("-", " "),
          feature.replace("-", "_"),
          feature.replace("_", " ")
        ).distinct

        // Строим условие для фильтрации: x.name = 'v1' OR x.name = 'v2' ...
        val conds = variants.map(v => s"x.name = '$v'").mkString(" OR ")

        // Используем выражение для извлечения значения из nutriments
        val expr = s"try_element_at(filter(nutriments, x -> $conds), 1)['100g']"

        logger.info(s"Extracting feature '$feature' with expression: $expr")

        // Добавляем колонку с извлеченным значением
        processedDf = processedDf.withColumn(feature, F.expr(expr).cast(DoubleType))
      }
    } else {
      logger.warn("'nutriments' column not found in DataFrame")
    }

    // 3) Проверяем, какие признаки теперь доступны
    val availableFeatures = nutrimentFeatures.filter(processedDf.columns.contains)
    logger.info(s"Available nutriment features after extraction: ${availableFeatures.mkString(", ")}")

    // 4) Заполняем NULL нулями
    val fillMap = availableFeatures.map(feature => (feature, 0.0)).toMap
    if (fillMap.nonEmpty) {
      processedDf = processedDf.na.fill(fillMap)
      logger.info("Filled NULLs with 0 for nutriment features")
    } else {
      logger.warn("No nutriment features found in DataFrame columns")
    }

    // 5) Удаляем выбросы, если это требуется
    if (preprocessingConfig.removeOutliers) {
      processedDf = removeOutliers(processedDf)
    }

    // 6) Приводим к окончательному виду: оставляем только нужные колонки
    val finalCols = Seq("code", "product_name") ++ availableFeatures
    val filteredCols = finalCols.filter(processedDf.columns.contains).map(F.col)
    val finalDf = processedDf.select(filteredCols: _*)

    // Выводим схему данных после предобработки
    logger.info("DataFrame schema after preprocessing:")
    finalDf.printSchema()

    // Выводим список колонок после предобработки
    val finalColumns = finalDf.columns
    logger.info(s"DataFrame columns after preprocessing: ${finalColumns.mkString(", ")}")

    logger.info(s"Preprocessing completed. Rows: ${finalDf.count()}, Columns: ${finalDf.columns.mkString(", ")}")
    finalDf
  }

  private def removeOutliers(df: DataFrame): DataFrame = {
    logger.info("Removing outliers using z-score")

    // Определяем, какие признаки присутствуют в DataFrame
    val availableFeatures = nutrimentFeatures.filter(df.columns.contains)

    if (availableFeatures.isEmpty) {
      logger.warn("No configured nutriment features found in DataFrame columns; skipping outlier removal")
      return df
    }

    // Вычисляем среднее и стандартное отклонение для каждого признака
    val statsExprs = availableFeatures.flatMap(feature =>
      Seq(
        F.mean(F.col(feature)).alias(s"${feature}_mean"),
        F.stddev(F.col(feature)).alias(s"${feature}_stddev")
      )
    )

    val stats = df.select(statsExprs: _*).first()

    // Строим условие для z-score, пропуская признаки с нулевым стандартным отклонением
    var condition: Option[org.apache.spark.sql.Column] = None
    val usedFeatures = scala.collection.mutable.ListBuffer[String]()

    for (feature <- availableFeatures) {
      val mean = stats.getAs[Double](s"${feature}_mean")
      val stddev = stats.getAs[Double](s"${feature}_stddev")

      if (stddev != 0) {
        val zCond = F.abs((F.col(feature) - F.lit(mean)) / F.lit(stddev)) < preprocessingConfig.outlierThreshold
        condition = condition match {
          case Some(cond) => Some(cond.and(zCond))
          case None => Some(zCond)
        }
        usedFeatures += feature
      } else {
        logger.warn(s"Feature '$feature': stddev is null or zero ($stddev), skipping outlier removal for this feature")
      }
    }

    condition match {
      case Some(cond) =>
        // Вычисляем количество строк до и после удаления выбросов
        val totalRows = df.count()
        val filteredDf = df.filter(cond)
        val keptRows = filteredDf.count()
        val removedRows = totalRows - keptRows
        val pct = if (totalRows > 0) (removedRows.toDouble / totalRows * 100) else 0.0

        logger.info(
          s"Outlier removal: used_features=${usedFeatures.mkString(", ")}. " +
          s"Removed $removedRows rows (${pct.formatted("%.2f")}%); kept $keptRows/$totalRows."
        )

        filteredDf

      case None =>
        logger.warn("No valid features (with non-zero stddev) for outlier removal. Skipping outlier removal.")
        df
    }
  }
}
