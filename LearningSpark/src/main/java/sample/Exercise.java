package sample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
    public static void main(String args[]) throws IOException {
        System.out.println("Start Exercise");

        //Finding max & Minimum value
        FileSystem fs = FileSystem.get(new Configuration());
        Path pt = new Path("input/Data2_XYZ.txt");
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line;
        int counter = 0;
        double minX = 9999999;
        double maxX = 0;
        double minY = 9999999;
        double maxY = 0;
        while((line = br.readLine()) != null){
            String delims = " ";
            String[] elements= line.split(delims);
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

            counter++;
        }

        System.out.println("minX: "+minX);
        System.out.println("maxX: "+maxX);
        System.out.println("minY: "+minY);
        System.out.println("maxY: "+maxY);
        System.out.println("counter: "+counter);

        //Regular Expression in Java
//        String input = "(1,2,-1.0)";
//        String filtered = input.replace("(","").replace(")","");
//        String elements[] = filtered.split(",");
//        for(int i=0; i<elements.length;i++)
//            System.out.println(elements[i]);
//        System.out.println(filtered);
    }
}
