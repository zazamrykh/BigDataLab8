package ru.itmo.datamart.config

import com.typesafe.config.{Config => TypesafeConfig, ConfigFactory}
import scala.jdk.CollectionConverters._

class Config {
  private val config: TypesafeConfig = ConfigFactory.load()
  private val datamartConfig = config.getConfig("datamart")

  case class SparkConfig(
    appName: String,
    master: String,
    driverMemory: String,
    executorMemory: String,
    executorCores: Int,
    numExecutors: Int,
    shufflePartitions: Int,
    defaultParallelism: Int,
    serializer: String
  )

  case class MSSQLConfig(
    server: String,
    port: String,
    database: String,
    username: String,
    password: String,
    driver: String,
    inputTable: String,
    outputTable: String,
    clusteringResultsTable: String,
    clusterCentersTable: String
  )

  case class DataConfig(
    parquetPath: String,
    sampleSize: Int,
    apiPort: Int
  )

  case class PreprocessingConfig(
    minValuesThreshold: Double,
    removeOutliers: Boolean,
    outlierThreshold: Double
  )

  def getSparkConfig(): SparkConfig = {
    val sparkConfig = datamartConfig.getConfig("spark")
    SparkConfig(
      appName = sparkConfig.getString("app-name"),
      master = sparkConfig.getString("master"),
      driverMemory = sparkConfig.getString("driver-memory"),
      executorMemory = sparkConfig.getString("executor-memory"),
      executorCores = sparkConfig.getInt("executor-cores"),
      numExecutors = sparkConfig.getInt("num-executors"),
      shufflePartitions = sparkConfig.getInt("shuffle-partitions"),
      defaultParallelism = sparkConfig.getInt("default-parallelism"),
      serializer = sparkConfig.getString("serializer")
    )
  }

  def getMSSQLConfig(): MSSQLConfig = {
    val mssqlConfig = datamartConfig.getConfig("mssql")
    MSSQLConfig(
      server = mssqlConfig.getString("server"),
      port = mssqlConfig.getString("port"),
      database = mssqlConfig.getString("database"),
      username = mssqlConfig.getString("username"),
      password = mssqlConfig.getString("password"),
      driver = mssqlConfig.getString("driver"),
      inputTable = mssqlConfig.getString("input-table"),
      outputTable = mssqlConfig.getString("output-table"),
      clusteringResultsTable = mssqlConfig.getString("clustering-results-table"),
      clusterCentersTable = mssqlConfig.getString("cluster-centers-table")
    )
  }

  def getDataConfig(): DataConfig = {
    val dataConfig = datamartConfig.getConfig("data")
    DataConfig(
      parquetPath = dataConfig.getString("parquet-path"),
      sampleSize = dataConfig.getInt("sample-size"),
      apiPort = dataConfig.getInt("api-port")
    )
  }

  def getPreprocessingConfig(): PreprocessingConfig = {
    val preprocessingConfig = datamartConfig.getConfig("preprocessing")
    PreprocessingConfig(
      minValuesThreshold = preprocessingConfig.getDouble("min-values-threshold"),
      removeOutliers = preprocessingConfig.getBoolean("remove-outliers"),
      outlierThreshold = preprocessingConfig.getDouble("outlier-threshold")
    )
  }

  def getNutrimentFeatures(): List[String] = {
    datamartConfig.getConfig("features")
      .getStringList("nutriment-features")
      .asScala
      .toList
  }
}
