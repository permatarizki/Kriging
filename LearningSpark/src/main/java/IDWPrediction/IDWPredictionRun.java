package IDWPrediction;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by mata on 7/8/15.
 */
public class IDWPredictionRun {
    public static void main(String args[]){
        System.out.println("Starting Kriging Application");

        //Create a Java Spark Context
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Intellij WordCount spark");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //Specify Grid Dimension
        int x_length_grid = 70; //in meters
        int y_length_grid = 70; //in meters
        final double grid_size = 1; //in meters
        //specify radius range to search nearest points
        final double radiusrange = 3; //in meters

        //create RDD based on this grid points
        List<String> gridpoints = new ArrayList<String>();
        int x,y;
        for(x=0; x<x_length_grid; x++){
            for (y=0; y<y_length_grid; y++){
                gridpoints.add(x+","+y);
            }
        }
        //check size of grids
        System.out.println(gridpoints.size());
        //create RDD for each grid points
        JavaRDD<String> rdd_gridpoints = sc.parallelize(gridpoints);

        //Now we load LiDAR data input from a file and create RDD for it
        final JavaRDD<String> rdd_inputLIDAR = sc.textFile("input/DataSample.txt");
        final int length_inputLIDAR = (int) rdd_inputLIDAR.count();
        System.out.println(length_inputLIDAR);
        final List<String> inputLIDAR = rdd_inputLIDAR.collect();
//        System.out.println(inputLIDAR.get(3));

        //
        JavaPairRDD<String,Double> closestpointsofgrid = rdd_gridpoints.flatMapToPair(
                new PairFlatMapFunction<String, String, Double>() {
                    public Iterable<Tuple2<String, Double>> call(String s) throws Exception {
                        //Parsing raw data to its coordinates & weight
                        String[] elements = s.split(",");
                        double xgrid = Double.parseDouble(elements[0]);
                        double ygrid = Double.parseDouble(elements[1]);
                        int i;
                        //store nearest points in this variable
                        List<Tuple2<String, Double>> grid_with_closestPoints = new ArrayList<Tuple2<String, Double>>();
                        //calculate distance each input point to a specific grid
                        for (i = 0; i < length_inputLIDAR; i++) {
                            //this is only split one space from the input. Beware about input format!
                            //if there is double space from input will cause error in this code
                            elements = inputLIDAR.get(i).split(" ");
                            double xlidarpoint = Double.parseDouble(elements[0]);
                            double ylidarpoint = Double.parseDouble(elements[1]);
                            double deltaX = xlidarpoint - xgrid;
                            double deltaY = ylidarpoint - ygrid;
                            double distance = Math.sqrt(deltaX * deltaX + deltaY * deltaY);
                            //filter based on radius range
                            if (distance < radiusrange) {
                                //<key,value> = <gridxy, indexLidar>
                                grid_with_closestPoints.add(new Tuple2<String, Double>(s, (double) i));
                            }
                        }
                        //Exception for any grid which don't have any closest points
                        if (grid_with_closestPoints.size() == 0) {
                            grid_with_closestPoints.add(new Tuple2<String, Double>(s, -1.0));
                        }

                        return grid_with_closestPoints;
                    }
                }
        );
        closestpointsofgrid.collect();
        System.out.println("number of closest points:"+closestpointsofgrid.count());
        closestpointsofgrid.saveAsTextFile("output");

        //Prediction calculation for each grids
        JavaRDD<String> prediction = (JavaRDD<String>)closestpointsofgrid.reduceByKey(
                new Function2<Double, Double, String>() {
                    public String call(Double aDouble, Double aDouble2) throws Exception {
                        return "ok";
                    }
                }
        );

        //TODO change this part with more efficient RDD transformation
        JavaRDD<String> grid_with_closestpoints = sc.textFile("output/part-00000");
        JavaRDD<String> predictions = grid_with_closestpoints.map(
                new Function<String,String>() {
                    public String call(String s) throws Exception {
                        //Parsing each line

                        return s;
                    }
                }
        );

        predictions.collect();
        predictions.saveAsTextFile("prediction_result");
    }
}
