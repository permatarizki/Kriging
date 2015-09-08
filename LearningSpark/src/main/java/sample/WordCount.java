package sample; /**
 * Created by mata on 6/22/15.
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
    public static void main(String[] args){
        System.out.println("Learning Spark Using maven Intellij");

        //Create a Java Spark Context
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Intellij WordCount spark");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //Load our input data
//        JavaRDD<String> input = sc.textFile(inputFile);
//        //Split up into words
//        JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
//            public Iterable<String> call(String x) throws Exception {
//                return Arrays.asList(x.split(" "));
//            }
//        });
//        //Transform into pairs and count
//        JavaPairRDD<String, Integer> counts = words.mapToPair(
//                new PairFunction<String, String, Integer>() {
//                    public Tuple2<String, Integer> call(String x) throws Exception {
//                        return new Tuple2(x,1);
//                    }
//                }
//        ).reduceByKey(new Function2<Integer, Integer, Integer>() {
//            public Integer call(Integer x, Integer y) throws Exception {
//                return x+y;
//            }
//        });
//        counts.saveAsTextFile(outputFile);
//
//        System.exit(0);
    }
}
