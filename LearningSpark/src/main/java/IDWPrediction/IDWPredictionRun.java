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

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mata on 7/8/15.
 */
public class IDWPredictionRun {
    public static void main(String args[]){
        System.out.println("Starting IDW Prediction");

        //Create a Java Spark Context
        SparkConf conf = new SparkConf().setMaster("local").setAppName("IDW Prediction spark");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //Specify Grid Dimension
        int x_length_grid = 2; //in meters
        int y_length_grid = 2; //in meters
        final double grid_size = 1; //in meters
        //specify radius range to search nearest points
        final double radiusrange = 500; //in meters

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
        JavaPairRDD<String,String> gridWithOnePoint = rdd_gridpoints.flatMapToPair(
                new PairFlatMapFunction<String, String, String>() {
                    public Iterable<Tuple2<String, String>> call(String s) throws Exception {
                        //Parsing raw data to its coordinates & weight
                        String[] elements = s.split(",");
                        double xgrid = Double.parseDouble(elements[0]);
                        double ygrid = Double.parseDouble(elements[1]);
                        int i;
                        //store nearest points in this variable
                        List<Tuple2<String, String>> grid_with_closestPoints = new ArrayList<Tuple2<String, String>>();
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
                                grid_with_closestPoints.add(new Tuple2<String, String>(s,Integer.toString(i)));
                            }
                        }
                        //Exception for any grid which don't have any closest points
                        if (grid_with_closestPoints.size() == 0) {
                            grid_with_closestPoints.add(new Tuple2<String, String>(s, "no_points"));
                        }

                        return grid_with_closestPoints;
                    }
                }
        );
        //gridWithOnePoint.collect();      aDouble
        System.out.println("number of closest points:"+gridWithOnePoint.count());
        gridWithOnePoint.saveAsTextFile("gridWithOnePoint");

        JavaPairRDD<String, String> gridWithPoints = gridWithOnePoint.reduceByKey(
                new Function2<String, String, String>() {
                    public String call(String s1, String s2) throws Exception {
                        //accumulate all index of closest points
                        return s1 +","+ s2;
                    }
                }
        );
        gridWithPoints.collect();
        gridWithPoints.saveAsTextFile("gridWithPoints");

        JavaRDD<String> gridWithPrediction = gridWithPoints.map(
                new Function<Tuple2<String, String>, String>() {
                    public String call(Tuple2<String, String> gridwithpointsTuple) throws Exception {
                        //Parse elements both from grid and closest point
                        String[] elementsGrid= gridwithpointsTuple._1().split(",");
                        double x_grid = Double.parseDouble(elementsGrid[0]);
                        double y_grid = Double.parseDouble(elementsGrid[1]);
                        String[] elementsIndex = gridwithpointsTuple._2().split(",");
                        double numerator = 0;
                        double denominator = 0;
                        for(int i=0; i<elementsIndex.length; i++){
                            //get x,y,z Lidar based on index
                            String[] elementsPoint = inputLIDAR.get(i).split(" ");
                            double x_point = Double.parseDouble(elementsPoint[0]);
                            double y_point = Double.parseDouble(elementsPoint[1]);
                            double z_point = Double.parseDouble(elementsPoint[2]);
                            //calculate euclidian distance
                            double deltaX = x_grid - x_point;
                            double deltaY = y_grid - y_point;
                            double distance = Math.sqrt(deltaX * deltaX + deltaY * deltaY);

                            numerator = numerator + z_point/distance;
                            denominator  = denominator + 1/distance;

                        }

                        double weight = numerator/denominator;
                        DecimalFormat df = new DecimalFormat("#.##");
                        String dx = df.format(weight);
                        return gridwithpointsTuple._1()+","+dx;
                    }
                }
        );
        gridWithPrediction.collect();
        gridWithPrediction.saveAsTextFile("gridWithPrediction");

    }
}
