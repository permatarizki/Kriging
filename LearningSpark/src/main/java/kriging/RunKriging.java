package kriging;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import sun.beans.editors.DoubleEditor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by mata on 7/7/15.
 */

public class RunKriging {
    public static void main (String args[]){
    //
        System.out.println("Starting Kriging Application");

        //Create a Java Spark Context
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Intellij WordCount spark");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //Specify Grid Dimension
        int x_length_grid = 50; //in meters
        int y_length_grid = 1000; //in meters
        double grid_size = 1; //in meters

        //create RDD based on this grid points
        List<GeoPoint> gridpoints = new ArrayList<GeoPoint>();
        int x,y;
        for(x=0; x<x_length_grid; x++){
            for (y=0; y<y_length_grid; y++){
                GeoPoint onenode = new GeoPoint();
                onenode.set_x(x);
                onenode.set_y(y);
                gridpoints.add(onenode);
            }
        }
        //check size of grids
        System.out.println(gridpoints.size());
        //create RDD for each grid points
        JavaRDD<GeoPoint> rdd_gridpoints = sc.parallelize(gridpoints);

        //Now we load LiDAR data input from a file
        JavaRDD<String> rdd_inputLIDAR = sc.textFile("input/DataSample.txt");
        JavaPairRDD<String, Double> oneline = rdd_inputLIDAR.mapToPair(
                new PairFunction<String, String, Double>() {
                    public Tuple2<String, Double> call(String s) throws Exception {
                        //Parsing raw data to its coordinates & weight
                        String[] elements = s.split(" ");
                        return new Tuple2<String, Double>(elements[0] + "," + elements[1], Double.parseDouble(elements[2]));
                    }
                }
        );
        oneline.collect();
        oneline.saveAsTextFile("output");









    }
}
