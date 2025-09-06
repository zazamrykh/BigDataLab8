package ru.itmo.datamart.api

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import spray.json.DefaultJsonProtocol._
import spray.json._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import ru.itmo.datamart.config.Config

import scala.concurrent.ExecutionContextExecutor
import scala.io.StdIn
import scala.util.{Failure, Success}
import scala.jdk.CollectionConverters._

// Определяем модели данных для JSON
case class DataResponse(success: Boolean, message: String, data: List[Map[String, Any]])
case class ClusteringResultsRequest(data: List[Map[String, Any]])
case class ClusterCentersRequest(centers: List[List[Double]], featureNames: List[String])
case class ApiResponse(success: Boolean, message: String)

// Конвертер для преобразования Any в JsValue
object AnyJsonFormat extends JsonFormat[Any] {
  def write(x: Any): JsValue = x match {
    case n: Int => JsNumber(n)
    case n: Long => JsNumber(n)
    case n: Float => JsNumber(n)
    case n: Double => JsNumber(n)
    case s: String => JsString(s)
    case b: Boolean => JsBoolean(b)
    case null => JsNull
    case list: List[_] => JsArray(list.map(item => write(item)).toVector)
    case array: Array[_] => JsArray(array.map(item => write(item)).toVector)
    case _ => JsString(x.toString)
  }

  def read(value: JsValue): Any = value match {
    case JsNumber(n) => n.toDouble
    case JsString(s) => s
    case JsBoolean(b) => b
    case JsNull => null
    case JsArray(elements) => elements.map(read).toList
    case _ => throw new IllegalArgumentException(s"Unsupported JSON value: $value")
  }
}

// Форматы для JSON сериализации/десериализации
object JsonFormats {
  import spray.json.DefaultJsonProtocol._

  implicit val anyFormat: JsonFormat[Any] = AnyJsonFormat

  // Создаем формат для Map[String, Any]
  implicit object MapJsonFormat extends RootJsonFormat[Map[String, Any]] {
    def write(m: Map[String, Any]): JsValue = JsObject(
      m.map { case (key, value) => key -> anyFormat.write(value) }
    )

    def read(value: JsValue): Map[String, Any] = value match {
      case JsObject(fields) => fields.map { case (key, value) => key -> anyFormat.read(value) }.toMap
      case _ => throw DeserializationException("Expected JsObject")
    }
  }

  // Создаем формат для List[Map[String, Any]]
  implicit object ListMapJsonFormat extends RootJsonFormat[List[Map[String, Any]]] {
    def write(list: List[Map[String, Any]]): JsValue = JsArray(
      list.map(MapJsonFormat.write).toVector
    )

    def read(value: JsValue): List[Map[String, Any]] = value match {
      case JsArray(elements) => elements.map(MapJsonFormat.read).toList
      case _ => throw DeserializationException("Expected JsArray")
    }
  }

  // Создаем формат для List[Double] и List[List[Double]]
  implicit object ListDoubleFormat extends RootJsonFormat[List[Double]] {
    def write(list: List[Double]): JsValue = JsArray(list.map(x => JsNumber(x)).toVector)
    def read(value: JsValue): List[Double] = value match {
      case JsArray(elements) => elements.map {
        case JsNumber(n) => n.toDouble
        case _ => throw DeserializationException("Expected JsNumber")
      }.toList
      case _ => throw DeserializationException("Expected JsArray")
    }
  }

  implicit object ListListDoubleFormat extends RootJsonFormat[List[List[Double]]] {
    def write(list: List[List[Double]]): JsValue = JsArray(list.map(ListDoubleFormat.write).toVector)
    def read(value: JsValue): List[List[Double]] = value match {
      case JsArray(elements) => elements.map(ListDoubleFormat.read).toList
      case _ => throw DeserializationException("Expected JsArray")
    }
  }

  // Теперь можем создать форматы для всех моделей
  implicit val dataResponseFormat: RootJsonFormat[DataResponse] = jsonFormat3(DataResponse)
  implicit val clusteringResultsRequestFormat: RootJsonFormat[ClusteringResultsRequest] = jsonFormat1(ClusteringResultsRequest)
  implicit val clusterCentersRequestFormat: RootJsonFormat[ClusterCentersRequest] = jsonFormat2(ClusterCentersRequest)
  implicit val apiResponseFormat: RootJsonFormat[ApiResponse] = jsonFormat2(ApiResponse)
}

class HttpServer(spark: SparkSession, config: Config) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val dataMartAPI = new DataMartAPI(spark, config)

  import JsonFormats._

  def start(host: String = "0.0.0.0", port: Int = 8080): Unit = {
    implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "datamart-system")
    implicit val executionContext: ExecutionContextExecutor = system.executionContext

    // Определяем маршруты
    val route =
      pathPrefix("api") {
        path("data") {
          get {
            logger.info("Received request for data")

            // Получаем данные из MSSQL с валидацией
            val df = dataMartAPI.getPreprocessedData()

            // Преобразуем DataFrame в список Map для JSON
            val data = dataFrameToListOfMaps(df)

            // Возвращаем данные в формате JSON
            complete(DataResponse(success = true, message = "Data retrieved successfully", data = data))
          }
        } ~
        path("clustering-results") {
          post {
            entity(as[ClusteringResultsRequest]) { request =>
              logger.info("Received request to save clustering results")

              try {
                // Преобразуем список Map в DataFrame
                import spark.implicits._
                import org.apache.spark.sql.Row
                import org.apache.spark.sql.types._

                // Создаем схему на основе первого элемента
                val firstRow = if (request.data.nonEmpty) request.data.head else Map.empty[String, Any]
                val schema = StructType(
                  firstRow.map { case (key, value) =>
                    val dataType = value match {
                      case _: Int | _: Integer => IntegerType
                      case _: Double => DoubleType
                      case _: String => StringType
                      case _: Boolean => BooleanType
                      case list: List[_] if list.nonEmpty && list.head.isInstanceOf[Double] =>
                        // Если это список чисел, преобразуем его в строку
                        StringType
                      case _ => StringType // Fallback
                    }
                    StructField(key, dataType, nullable = true)
                  }.toSeq
                )

                // Преобразуем все списки в строки
                val processedData = request.data.map { map =>
                  map.map {
                    case (key, value: List[_]) => key -> value.mkString("[", ",", "]")
                    case (key, value) => key -> value
                  }
                }

                // Создаем Row объекты
                val rows = processedData.map { map =>
                  Row.fromSeq(schema.fields.map { field =>
                    map.getOrElse(field.name, null)
                  })
                }

                // Создаем DataFrame
                val df = spark.createDataFrame(rows.asJava, schema)

                // Сохраняем результаты кластеризации
                dataMartAPI.saveClusteringResults(df)

                // Возвращаем успешный ответ
                complete(ApiResponse(success = true, message = "Clustering results saved successfully"))
              } catch {
                case e: Exception =>
                  logger.error(s"Error saving clustering results: ${e.getMessage}", e)
                  complete(StatusCodes.InternalServerError, ApiResponse(success = false, message = e.getMessage))
              }
            }
          }
        } ~
        path("cluster-centers") {
          post {
            entity(as[ClusterCentersRequest]) { request =>
              logger.info("Received request to save cluster centers")

              try {
                // Логируем полученные данные
                logger.info(s"Received centers: ${request.centers}")
                logger.info(s"Received feature names: ${request.featureNames}")

                // Преобразуем List[List[Double]] в Array[Array[Double]]
                val centersArray = request.centers.map(_.toArray).toArray
                val featureNamesArray = request.featureNames.toArray

                // Сохраняем центры кластеров
                dataMartAPI.saveClusterCenters(centersArray, featureNamesArray)

                // Возвращаем успешный ответ
                complete(ApiResponse(success = true, message = "Cluster centers saved successfully"))
              } catch {
                case e: Exception =>
                  logger.error(s"Error saving cluster centers: ${e.getMessage}", e)
                  complete(StatusCodes.InternalServerError, ApiResponse(success = false, message = e.getMessage))
              }
            }
          }
        }
      }

    // Запускаем HTTP-сервер
    val bindingFuture = Http().newServerAt(host, port).bind(route)

    bindingFuture.onComplete {
      case Success(binding) =>
        val address = binding.localAddress
        logger.info(s"Server online at http://${address.getHostString}:${address.getPort}/")
      case Failure(ex) =>
        logger.error(s"Failed to bind HTTP server: ${ex.getMessage}", ex)
        system.terminate()
    }

    // Добавляем обработчик завершения
    sys.addShutdownHook {
      logger.info("Shutting down server...")
      bindingFuture
        .flatMap(_.unbind())
        .onComplete(_ => {
          logger.info("Server shutdown complete")
          system.terminate()
        })
    }
  }

  // Преобразование DataFrame в список Map для JSON
  private def dataFrameToListOfMaps(df: DataFrame): List[Map[String, Any]] = {
    // Ограничиваем количество строк для безопасности
    val rows = df.limit(1000).collect()

    // Преобразуем каждую строку в Map
    rows.map { row =>
      val scalaMap = scala.collection.mutable.Map[String, Any]()
      row.schema.fields.foreach { field =>
        val value = row.get(row.fieldIndex(field.name))
        scalaMap(field.name) = value
      }
      scalaMap.toMap
    }.toList
  }
}
