package sample;

import IDWPrediction.IDWPredictionRun;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

/**
 * Created by mata on 12/22/15.
 */
public class FindMinMaxValues {
    private static Logger logger = Logger.getLogger(IDWPredictionRun.class.getName());

    public static void main(String args[]) {
        if (args.length != 1) {
            System.err.println("Usage: FindMinMaxValues <pathtoLidarData>");
            System.exit(1);
        }

        logger.info("Starting Finding Minimum & Maximum Values using Spark");

        //Create a Java Spark Context
        SparkConf conf = new SparkConf().setAppName("Find Min Max Values").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.startTime();

        //Now we load LiDAR data input from a file and create RDD for it
        final JavaRDD<String> rdd_inputLIDAR = sc.textFile(args[0], 200).persist(StorageLevel.MEMORY_ONLY_SER_2());

        JavaPairRDD<String, Double> ones = rdd_inputLIDAR.flatMapToPair(new PairFlatMapFunction<String, String, Double>() {
            @Override
            public Iterable<Tuple2<String, Double>> call(String s) {
                String delims = " ";
                String[] elements = s.toString().split(delims);
                List<Tuple2<String,Double>> mapoutputs = new ArrayList<Tuple2<String, Double>>();
                double x = Double.parseDouble(elements[0]);
                double y = Double.parseDouble(elements[1]);
                mapoutputs.add(new Tuple2<String, Double>("x", x));
                mapoutputs.add(new Tuple2<String, Double>("y", y));
                return mapoutputs;
            }
        });

        //Finding max values
        JavaPairRDD<String, Double> maxvals = ones.reduceByKey(new Function2<Double, Double, Double>() {
            @Override public Double call(Double i1, Double i2) {
                if (i1 > i2)
                    return i1;
                else
                    return i2;
            }
        });

        List<Tuple2<String, Double>> output1 = maxvals.collect();
        for (Tuple2<?,?> tuple : output1) {
            System.out.println(tuple._1() + " max: " + tuple._2());
        }

        //Finding max values
        JavaPairRDD<String, Double> minvals = ones.reduceByKey(new Function2<Double, Double, Double>() {
            @Override public Double call(Double i1, Double i2) {
                if (i1 < i2)
                    return i1;
                else
                    return i2;
            }
        });

        List<Tuple2<String, Double>> output2 = minvals.collect();
        for (Tuple2<?,?> tuple : output2) {
            System.out.println(tuple._1() + " min: " + tuple._2());
        }

        sc.stop();

    }
}