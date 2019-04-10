import org.apache.spark.internal.config.SparkConfigProvider
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object WordCount {
  def main(args: Array[String]): Unit = {
    val sc = SparkContext.getOrCreate(new SparkConf().setAppName("Word Count").setMaster("local"));
    
    val words = sc.textFile("story.txt").flatMap(_.split(" "))
    val wordCountIndex = words.map((_, 1)).reduceByKey(_ + _)

    wordCountIndex.collect().foreach(println)
  }
}
