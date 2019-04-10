package com.cvk.training.spark.movie_lens

import java.util.Collections

import org.apache.spark.{SparkConf, SparkContext}

object RatingsCount {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("Ratings Count").setMaster("local")
        val sc = SparkContext.getOrCreate(sparkConf)

        val lines = sc.textFile("ml-100k/u.data")
        val fieldsList = lines.map(_.split("\t"))
        val ratings = fieldsList.map((_(2)))
        val ratingsCounter = ratings.map((_, 1))
        val ratingsCount = ratingsCounter.reduceByKey(_ + _)
        ratingsCount.sortByKey(false).foreach(println)
    }
}
