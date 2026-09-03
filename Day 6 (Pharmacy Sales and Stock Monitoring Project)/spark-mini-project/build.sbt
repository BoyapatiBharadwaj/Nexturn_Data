ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "pharmacy-sales-stock-monitoring",
    version := "1.0",
    organization := "edu.spark.pharmacy",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.9",
      "org.apache.spark" %% "spark-sql" % "3.5.9",
      "org.apache.spark" %% "spark-streaming" % "3.5.9"
    ),

    Compile / run / fork := true,

    Compile / run / javaOptions ++= Seq(
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.net.util=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED"
    ),

    Compile / run / mainClass := Some("BatchProcessor")
  )
