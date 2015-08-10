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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import scala.Tuple2;

/**
 * Created by mata on 6/24/15.
 */
public class Exercise {
    public static void main(String args[]){
        System.out.println("Start Exercise");

        //Regular Expression in Java
        String input = "(1,2,-1.0)";
        String filtered = input.replace("(","").replace(")","");
        String elements[] = filtered.split(",");
        for(int i=0; i<elements.length;i++)
            System.out.println(elements[i]);
        System.out.println(filtered);
    }
}
