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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by mata on 7/8/15.
 * email : permatarizki.at.gmail.com
 */
public class RunKriging {
    private static Logger logger = Logger.getLogger(IDWPredictionRun.class.getName());

    public static void main(String args[]){
        if(args.length != 2){
            System.err.println("Usage: IDWPrediction <pathtoLidarData> <radiusrange>");
            System.exit(1);
        }

        logger.info("Starting IDW Prediction");

        //Create a Java Spark Context
        SparkConf conf = new SparkConf().setAppName("IDW Prediction spark");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.startTime();

        //specify radius range to search nearest points
        final double radiusrange = Double.parseDouble(args[1]); //in meters

        //Now we load LiDAR data input from a file and create RDD for it
        final JavaRDD<String> rdd_inputLIDAR = sc.textFile(args[0]);
        final int length_inputLIDAR = (int) rdd_inputLIDAR.count();
        logger.info("Number of LiDAR input points : " + String.valueOf(length_inputLIDAR));
        final List<String> inputLIDAR = rdd_inputLIDAR.collect();
        rdd_inputLIDAR.unpersist();

        double minX = 9999999;
        double maxX = 0;
        double minY = 9999999;
        double maxY = 0;
        for(int i=0; i<inputLIDAR.size(); i++){
            String delims = " ";
            String[] elements= inputLIDAR.get(i).split(delims);
            double x = Double.parseDouble(elements[0]);
            double y = Double.parseDouble(elements[1]);

            if(x < minX)
                minX = x;
            if(x > maxX)
                maxX = x;
            if(y < minY)
                minY = y;
            if(y > maxY)
                maxY = y;
        }

        //create RDD based on this grid point
        List<String> gridpoints = new ArrayList<String>();
        int minx = (int) Math.floor(minX);
        int miny = (int) Math.floor(minY);
        int maxx = (int) Math.ceil(maxX);
        int maxy = (int) Math.ceil(maxY);
        logger.info("MIN: ("+minx+","+miny+")");
        logger.info("MAX: ("+maxx+","+maxy+")");
        //Specify Grid Dimension
        final double grid_size = 1; //in meters
        int x_length_grid = (int) ((maxx-minx)/grid_size); //in meters
        int y_length_grid = (int) ((maxy-miny)/grid_size); //in meters

        int x,y;
        int idxX = 0;
        int idxY = 0;
        for(x=0; x<x_length_grid; x++)
            for (y = 0; y < y_length_grid; y++) {
                idxX = (int) (minx + (x * grid_size));
                idxY = (int) (miny + (y * grid_size));
                gridpoints.add(idxX + "," + idxY);
            }
        //check size of gridsu
        logger.log(Level.INFO,"Number of GRID points : " + String.valueOf(gridpoints.size()));
        //create RDD for each grid points
        JavaRDD<String> rdd_gridpoints = sc.parallelize(gridpoints);
        rdd_gridpoints.saveAsTextFile("gridPoints");
        rdd_gridpoints.unpersist();

        final JavaRDD<String> rdd_gridfromtext = sc.textFile("gridPoints/part-*");
        JavaPairRDD<String,String> gridWithOnePoint = rdd_gridfromtext.flatMapToPair(
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
                                grid_with_closestPoints.add(new Tuple2<String, String>(s, Integer.toString(i)));
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
                        return s1 + "," + s2;
                    }
                }
        );
        gridWithPoints.collect();
//        gridWithPoints.saveAsTextFile("gridWithPoints");
        gridWithOnePoint.unpersist();

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
                                if(distance==0) {
                                    //nothing to do here
                                }else{
                                    numerator = numerator + (double) (z_point / distance);
                                    denominator = denominator + (double) (1 / distance);
                                }

                            }

                            double weight = (double) (numerator / denominator);
                            DecimalFormat df = new DecimalFormat("#.##");
                            String dx = df.format(weight);
                            return gridwithpointsTuple._1() + "," + dx;
                        }
                    }
                }
        );
        gridWithPrediction.collect();
        gridWithPrediction.saveAsTextFile("gridWithPrediction");

        sc.stop();
        logger.info("FINISHED IDW PREDICTION");
    }
}
