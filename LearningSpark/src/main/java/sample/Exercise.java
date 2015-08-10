package sample;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import scala.Tuple2;

/**
 * Created by mata on 6/24/15.
 */
public class Exercise {
    public static void main(String args[]){
        System.out.println("Start Exercise");

        //Create a Java Spark Context
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Intellij WordCount spark");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //Splitting words
//        JavaRDD<String> inputFiles = sc.textFile("file:////Users/mata/code/scala/advance_analytic_with_spark/sample.txt");
//        System.out.println(inputFiles.first());
//new Tuple2<String, Integer>(s, 1);
//        JavaRDD<String> words = inputFiles.flatMap(new FlatMapFunction<String, String>() {
//            public Iterable<String> call(String s) throws Exception {
//                return Arrays.asList(s.split(" "));
//            }
//        });
//        System.out.println(words.count());


        List<Integer> data = Arrays.asList(1,2,3,4,5,6);
        JavaRDD<String> distData = sc.textFile("input/DataSample.txt");

        JavaPairRDD<String, String> oneline = distData.mapToPair(
                new PairFunction<String, String, String>() {
                    public Tuple2<String, String> call(String s) throws Exception {
                        //Parsing raw data to its coordinates & weight
                        String[] elements = s.split(" ");
                        return new Tuple2<String, String>(elements[0]+","+elements[1],elements[2]);
                    }
                }
        );


        oneline.collect();
        oneline.saveAsTextFile("output");
    }
}
