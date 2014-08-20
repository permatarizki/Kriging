package wise.semivariogram.mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Semivariogram extends Configured implements Tool {

	// we need the number of input data rows!!!
	private int rows = 0;
	private int nrbins = 50;

	private double maxdist = 0.0;
	
	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 5) {
			System.err.println("Usage: Semivariogram <inputPath> <intermediatePath> <outputPath> <rows> <nrbins>");
			System.exit(2);
		}

		String pathInput = otherArgs[0];
		String pathIntermediate = otherArgs[1];
		String pathOutput = otherArgs[2];
		rows = Integer.parseInt(otherArgs[3]);
		nrbins = Integer.parseInt(otherArgs[4]);
		
		System.out.println("input : " + pathInput);
		System.out.println("interm : " + pathIntermediate);
		System.out.println("output : " + pathOutput);

		/*//start file read from HDFS
		//Path pt=new Path(pathInput+"/variogram_test"  );
		Path pt=new Path(pathInput); 
		FileSystem fin = FileSystem.get(new Configuration());
		
		BufferedReader br=new BufferedReader(new InputStreamReader(fin.open(pt)));
				
		String sCurrentLine="";
		for (int i=0; (sCurrentLine = br.readLine()) != null && i<rows ; i++){ 
			 StringTokenizer inputXYZ = new StringTokenizer(sCurrentLine, "\t"); 
			 input[i][0] = Double.parseDouble(inputXYZ.nextToken()); 
			 input[i][1] = Double.parseDouble(inputXYZ.nextToken()); 
			 input[i][2] = Double.parseDouble(inputXYZ.nextToken());
		  
			 findMinXY(input[i][0], input[i][1]); 
			 findMaxXY(input[i][0], input[i][1]); 
		 } 
		 br.close(); fin.close();
		//end file read from HDFS
*/		 
		
		
		/*//start write file to HDFS
		Path ptInter=new Path(pathIntermediate+"/variogram_test_nrbins"  ); 
		FileSystem fin_nrbins = FileSystem.get(new Configuration());
		
		BufferedWriter foutnrBins =new BufferedWriter (new OutputStreamWriter(fin_nrbins.create(ptInter)));
		foutnrBins.write(String.valueOf(nrbins));
		foutnrBins.close(); fin_nrbins.close();
		
		//start write file to HDFS
		Path ptIntrmd=new Path(pathIntermediate+"/variogram_test_intermediate"  ); 
		FileSystem fin_inter = FileSystem.get(new Configuration());
		BufferedWriter foutinter =new BufferedWriter (new OutputStreamWriter(fin_inter.create(ptIntrmd)));
				
		for (int i=0 ; i<rows ; i++){
			for (int j=i ; j<rows ; j++){
				double d=Math.sqrt( Math.pow(input[i][0]-input[j][0], 2)
						+ Math.pow(input[i][1]-input[j][1], 2));
				if (d <= maxdist) {
					foutinter.write(String.valueOf(d) + " "
							+ String.valueOf(Math.pow(input[i][2]
									- input[j][2], 2)) + "\n");
				}
			}
			System.out.println(i);
		}		
		foutinter.close(); fin_inter.close();
		//end write file to HDFS
*/				
		
		Path pt=new Path(pathIntermediate+"/variogram_test_maxdist"); 
		FileSystem fin = FileSystem.get(new Configuration());
		
		BufferedReader br=new BufferedReader(new InputStreamReader(fin.open(pt)));
		maxdist=Double.parseDouble(br.readLine()); 
		br.close(); fin.close();
		
		System.out.println("maxdist : " + maxdist);
		conf.set("maxdist", String.valueOf(maxdist));
		conf.set("nrbins", String.valueOf(nrbins));
		
		Job job = new Job(conf, "Semivariogram");
		job.setJarByClass(Semivariogram.class);

		job.setMapperClass(SeperateDistanceMapper.class);
		// this line code can be omitted
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(SumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// delete the outputpath if already exists
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(pathOutput)))
			fs.delete(new Path(pathOutput), true);
		
		FileInputFormat.addInputPath(job, new Path(pathInput));
		FileOutputFormat.setOutputPath(job, new Path(pathOutput));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}


	/*public static void main(String[] args) throws Exception {
		
		int exitCode = ToolRunner.run(new Semivariogram(), args);
		System.exit(exitCode);
	}*/
}
