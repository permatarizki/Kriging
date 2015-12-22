package IDWPrediction;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Created by mata on 7/8/15.
 * email : permatarizki.at.gmail.com
 */
public class WordCount {
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

        //specofy gridsize
        final double grid_size = 1; //in meter
        //specify radius range to search nearest points
        final double radiusrange = Double.parseDouble(args[1]); //in meters
        //Now we load LiDAR data input from a file and create RDD for it
        final JavaRDD<String> rdd_inputLIDAR = sc.textFile(args[0],200).persist(StorageLevel.MEMORY_ONLY_SER_2());
        //Finding minimum & maximum values
        final List<String> inputLIDAR = rdd_inputLIDAR.collect();
        final int length_inputLIDAR = inputLIDAR.size();
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
        final int minx = (int) Math.floor(minX);
        final int miny = (int) Math.floor(minY);
        final int maxx = (int) Math.ceil(maxX);
        final int maxy = (int) Math.ceil(maxY);
        logger.info("+------------------------------------------------+");
        logger.info("| Number of LiDAR input points : " + String.valueOf(length_inputLIDAR));
        logger.info("| MIN: ("+minx+","+miny+")");
        logger.info("| MAX: ("+maxx+","+maxy+")");
        logger.info("| Length X: "+(int)(maxx-minx));
        logger.info("| Length Y: "+(int)(maxy-miny));
        logger.info("+------------------------------------------------+");
        //TODO create grid points, and merge it with data input

        /**
         * The ourput of this RDD will be
         * <key,value> = <gridpoint(x,y) numerator denumerator>
         * </>*/
        JavaRDD<String> gridWithDeNumerators = rdd_inputLIDAR.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterable<String> call(String s) throws Exception {
                        //Parsing raw data to its coordinates & weight
                        String[] elements = s.split(" ");
                        double xpoint = Double.parseDouble(elements[0]);
                        double ypoint = Double.parseDouble(elements[1]);
                        double zpoint = Double.parseDouble(elements[2]);
                        double xpoint_floor = Math.floor(xpoint);
                        double ypoint_floor = Math.floor(ypoint);
                        double start_x = xpoint_floor - radiusrange;
                        double start_y = ypoint_floor - radiusrange;
                        double anchorx = start_x;
                        double anchory = start_y;
                        int i;
                        List<String> grid_with_closestPoints = new ArrayList<String>();
                        while(start_x <= (anchorx+2*radiusrange)){
                            while(start_y <= (anchory+2*radiusrange)) {
                                if((start_x>=minx)&&(start_y>=miny)&&(start_x<=maxx)&&(start_y<=maxy)){
                                    //calculate distance start_x with xpoint
                                    double deltaX = start_x - xpoint;
                                    double deltaY = start_y - ypoint;
                                    double distance = Math.sqrt(deltaX * deltaX + deltaY * deltaY);
                                    //filter based on radius range
                                    if (distance < radiusrange) {
                                        //calculate nominator & denominator
                                        if (distance == 0) {
                                            double numerator = 0;
                                            double denominator = 0;
                                            grid_with_closestPoints.add(Double.toString(start_x) + "," +
                                                    Double.toString(start_y)+" "+Double.toString(numerator)+" "+Double.toString(denominator));
                                        } else {
                                            double numerator = (double) (zpoint / distance);
                                            double denominator = (double) (1 / distance);
                                            grid_with_closestPoints.add(Double.toString(start_x) + "," +
                                                    Double.toString(start_y)+" "+Double.toString(numerator)+" "+Double.toString(denominator));
                                        }
                                    }
                                }
                                start_y = start_y + grid_size;
                            }
                            start_y = anchory;
                            start_x = start_x + grid_size;
                        }
                        return grid_with_closestPoints;
                    }
                }
        ).persist(StorageLevel.MEMORY_ONLY_SER());
        //gridWithDeNumerators.collect();
        gridWithDeNumerators.saveAsTextFile("gridWithDeNumerators");
        rdd_inputLIDAR.unpersist();

//
//        JavaPairRDD<String, String> gridWithOneDeNumerator = gridWithDeNumerators.reduce(
//                new Function2<String, String, String>() {
//                    public String call(String s1, String s2) throws Exception {
//                        //accumulate all index of closest points from one grid point
//                        String[] elements1 = s1.split(" ");
//                        double numerator1 = Double.parseDouble(elements1[0]);
//                        double denumerator1 = Double.parseDouble(elements1[1]);
//
//                        String[] elements2 = s2.split(" ");
//                        double numerator2 = Double.parseDouble(elements2[0]);
//                        double denumerator2 = Double.parseDouble(elements2[1]);
//
//                        return Double.toString(numerator1+numerator2) + " " + Double.toString(denumerator1+denumerator2);
//                    }
//                }
//        );
//        gridWithOneDeNumerator.collect();
//        gridWithOneDeNumerator.saveAsTextFile("gridWithOneDeNumerator");
//        gridWithDeNumerators.unpersist();
//
//        JavaPairRDD<String, String> gridWithPrediction = gridWithOneDeNumerator.mapValues(
//                new Function<String, String>() {
//                    public String call(String s) throws Exception {
//                        double weight;
//                        String[] elements = s.split(" ");
//                        double numerator = Double.parseDouble(elements[0]);
//                        double denumerator = Double.parseDouble(elements[1]);
//                        if (denumerator == 0) {
//                            weight = 0;
//                        } else {
//                            weight = (double) (numerator / denumerator);
//                        }
//                        DecimalFormat df = new DecimalFormat("#.##");
//                        String dx = df.format(weight);
//                        return dx;
//                    }
//                }
//        );
//        gridWithPrediction.map(
//                new Function<Tuple2<String,String>, Object>() {
//                    @Override
//                    public Object call(Tuple2<String, String> v1) throws Exception {
//                        String[] elements = v1._1().split(",");
//                        return elements[0]+" "+elements[1]+" "+v1._2();
//                    }
//                }
//        ).saveAsTextFile("gridWithPrediction");

        sc.stop();
        logger.info("FINISHED IDW PREDICTION");
    }
}
