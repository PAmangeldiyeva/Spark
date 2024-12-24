ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "HelloSpark",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "2.1.1",
      "org.apache.spark" %% "spark-sql" % "2.1.1",
      "org.scalaj" %% "scalaj-http" % "2.4.2",
      "com.lihaoyi" %% "upickle" % "0.4.4", // replace latest_version with the actual version number
      // Add the scalaj-http library for HTTP requests
      "org.scalaj" %% "scalaj-http" % "2.4.2", // replace latest_version with the actual version number
      "com.lihaoyi" %% "upickle" % "1.4.0",
      "ch.hsr" % "geohash" % "1.4.0"


      // Add any additional libraries as needed
      // Дополнительные зависимости, если они есть в вашем проекте
      // Пример: "org.apache.spark" %% "spark-streaming" % "2.1.1",
      // "org.apache.spark" %% "spark-mllib" % "2.1.1"
    )
  )


