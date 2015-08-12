package IDWPrediction;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Created by mata on 7/8/15.
 * email : permatarizki.at.gmail.com
 */
public class IDWPredictionRun {
    public static void main(String args[]){
        if(args.length != 2){
            System.err.println("Usage: IDWPrediction <pathtoLidarData> <radiusrange>");
            System.exit(1);
        }

        Logger logger = Logger.getLogger(IDWPredictionRun.class.getName());
        logger.info("Starting IDW Prediction");

        //Create a Java Spark Context
        SparkConf conf = new SparkConf().setMaster("local[16]").setAppName("IDW Prediction spark");
        JavaSparkContext sc = new JavaSparkContext(conf);


        //Specify Grid Dimension
        int x_length_grid = 1000; //in meters
        int y_length_grid = 1000; //in meters
        final double grid_size = 1; //in meters
        //specify radius range to search nearest points
        final double radiusrange = Double.parseDouble(args[1]); //in meters

        //create RDD based on this grid point
        List<String> gridpoints = new ArrayList<String>();
        int x,y;
        int minx = 561000;
        int miny = 4198000;
        for(x=minx; x<x_length_grid+minx; x++){
            for (y=miny; y<y_length_grid+miny; y++){
                gridpoints.add(x+","+y);
            }
        }
        //check size of gridsu
        logger.info("Number of GRID points : " + String.valueOf(gridpoints.size()));
        //create RDD for each grid points
        JavaRDD<String> rdd_gridpoints = sc.parallelize(gridpoints);

        //Now we load LiDAR data input from a file and create RDD for it
        final JavaRDD<String> rdd_inputLIDAR = sc.textFile(args[0]);
        final int length_inputLIDAR = (int) rdd_inputLIDAR.count();
        logger.info("Number of LiDAR input points : " + String.valueOf(length_inputLIDAR));
        final List<String> inputLIDAR = rdd_inputLIDAR.collect();

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
        gridWithOnePoint.collect();

        JavaPairRDD<String, String> gridWithPoints = gridWithOnePoint.reduceByKey(
                new Function2<String, String, String>() {
                    public String call(String s1, String s2) throws Exception {
                        //accumulate all index of closest points from one grid point
                        return s1 +","+ s2;
                    }
                }
        );
        gridWithPoints.collect();

        JavaRDD<String> gridWithPrediction = gridWithPoints.map(
                new Function<Tuple2<String, String>, String>() {
                    public String call(Tuple2<String, String> gridwithpointsTuple) throws Exception {
                        if (gridwithpointsTuple._2().contentEquals("no_points")) {
                            return gridwithpointsTuple._1() + ",0";
                        } else {
                            //Parse elements both from grid and closest point
                            String[] elementsGrid = gridwithpointsTuple._1().split(",");
                            double x_grid = Double.parseDouble(elementsGrid[0]);
                            double y_grid = Double.parseDouble(elementsGrid[1]);
                            String[] elementsIndex = gridwithpointsTuple._2().split(",");
                            double numerator = 0;
                            double denominator = 0;
                            for (int i = 0; i < elementsIndex.length; i++) {
                                //get x,y,z Lidar based on index
                                String[] elementsPoint = inputLIDAR.get(i).split(" ");
                                double x_point = Double.parseDouble(elementsPoint[0]);
                                double y_point = Double.parseDouble(elementsPoint[1]);
                                double z_point = Double.parseDouble(elementsPoint[2]);
                                //calculate euclidian distance
                                double deltaX = x_grid - x_point;
                                double deltaY = y_grid - y_point;
                                double distance = Math.sqrt(deltaX * deltaX + deltaY * deltaY);

                                numerator = numerator + z_point / distance;
                                denominator = denominator + 1 / distance;

                            }

                            double weight = numerator / denominator;
                            DecimalFormat df = new DecimalFormat("#.##");
                            String dx = df.format(weight);
                            return gridwithpointsTuple._1() + "," + dx;
                        }
                    }
                }
        );
        gridWithPrediction.collect();
        gridWithPrediction.saveAsTextFile("gridWithPrediction");

    }
}
