name := "food-datamart"
version := "1.0"
scalaVersion := "2.13.12"

// Spark зависимости
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "4.0.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "4.0.0" % "provided",

  // JDBC драйвер для MS SQL Server (обновленная версия)
  "com.microsoft.sqlserver" % "mssql-jdbc" % "13.2.0.jre11",

  // Библиотека для работы с конфигурацией
  "com.typesafe" % "config" % "1.4.2",

  // HTTP сервер
  "com.typesafe.akka" %% "akka-http" % "10.5.0",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.5.0",
  "com.typesafe.akka" %% "akka-actor-typed" % "2.8.0",
  "com.typesafe.akka" %% "akka-stream" % "2.8.0",

  // Логирование
  "org.slf4j" % "slf4j-api" % "2.0.7",
  "ch.qos.logback" % "logback-classic" % "1.4.11",

  // Тестирование
  "org.scalatest" %% "scalatest" % "3.2.15" % Test
)

// Настройки сборки
// Настройки для сборки fat JAR
assembly / assemblyJarName := "food-datamart.jar"
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case x => MergeStrategy.first
}
