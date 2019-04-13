name := "Spark Learning"

version := "1.0"

scalaVersion := "2.12.8"

sbtVersion := "1.2.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.1",
  "org.apache.spark" %% "spark-sql" % "2.4.1"
)
