/**
 * Created by mata on 6/23/15.
 */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

val conf = new SparkConf().setMaster("local").setAppName("Scala Sample")
val sc = new SparkContext(conf)


