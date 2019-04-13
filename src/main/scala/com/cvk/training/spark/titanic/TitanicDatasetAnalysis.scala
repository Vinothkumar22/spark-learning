package com.cvk.training.spark.titanic

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions._

object TitanicDatasetAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Titanic Data Set Analysis").setMaster("local")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    var df = spark.read.format("csv").option("header", "true").load("titanic.csv")

    val distinctCountDf = df.select(df.columns.map(c => countDistinct(col(c)).alias(c)): _*)
    distinctCountDf.show()

    val missingDataCountDf = df.select(df.columns.map(c => count(when(col(c).isNull, 1)).alias(c)):_*)
    missingDataCountDf.show()

    val categoricalColumns = Array("Survived", "Pclass", "Sex", "Cabin", "Embarked")
    categoricalColumns.foreach(df.groupBy(_).count.show)

    val numericColumns = Array("PassengerId", "Age", "SibSp", "Parch", "Fare")
    df.select(numericColumns.flatMap(c =>
      Array(
        min(col(c).cast("Float")).alias(c + "_min"),
        max(col(c).cast("Float")).alias(c + "_max"))).map(c => c):_*).show()

    val stringColumns = Array("Name", "Ticket")
    stringColumns.foreach(c => df = df.withColumn(c + "_w_count", size(split(col(c), " "))))
    df.select(stringColumns.flatMap(c => Array(col(c), col(c + "_w_count"))).map(c => c):_*).show()
  }
}
