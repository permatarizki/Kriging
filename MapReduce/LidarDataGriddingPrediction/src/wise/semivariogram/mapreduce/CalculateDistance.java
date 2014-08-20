package wise.semivariogram.mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
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

public class CalculateDistance extends Configured implements Tool {

	// we need the number of input data rows!!!
	private int rows = 0;
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 5) {
			System.err.println("Usage: Semivariogram <inputPath> <interPath> <splitPath> <outputPath> <rows>");
			System.exit(2);
		}

		String pathInput = otherArgs[0];
		String pathIntermediate = otherArgs[1];
		String pathSplit = otherArgs[2];
		String pathOutput = otherArgs[3];
		rows = Integer.parseInt(otherArgs[4]);
		
		conf.set("origin_file", pathInput.toString());
		conf.set("inter_dir", pathIntermediate.toString());
		conf.setInt("rows", rows);
		
		System.out.println("input : " + pathInput);
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
		
		//start file read from local
		//and append line number
		Path pt=new Path(pathInput); 
		FileSystem fin = FileSystem.get(new Configuration());
		BufferedReader br=new BufferedReader(new InputStreamReader(fin.open(pt)));
				
		String sCurrentLine="";
		String a="";
		for (int i=0; (sCurrentLine = br.readLine()) != null && i<rows ; i++){ 
			a+=sCurrentLine+" "+String.valueOf(i)+"\n";
		} 
		br.close(); fin.close();
		
		Path ptInter=new Path(pathIntermediate+"/variogram_test_intermediate");
		FileSystem fin_nrbins = FileSystem.get(new Configuration());
		BufferedWriter foutnrBins =new BufferedWriter (new OutputStreamWriter(fin_nrbins.create(ptInter)));
		foutnrBins.write(a);
		foutnrBins.close(); fin_nrbins.close();
		//end append line number
		
		//start split intermediate file
		String cmd = "split -l 10 /home/seop/workspace/LidarDataGriddingPrediction/job1-1_intermediate/variogram_test_intermediate /home/seop/workspace/LidarDataGriddingPrediction/job1-1_split/";
		Process p = Runtime.getRuntime().exec(cmd);
		//end split intermediate file

		
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
		
		Job job = new Job(conf, "Calculate Distance and square Z");
		job.setJarByClass(CalculateDistance.class);

		job.setMapperClass(CalculateDistanceMapper.class);
		// this line code can be omitted
		// job.setCombinerClass(IntSumReducer.class);
		//job.setReducerClass(SumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// delete the outputpath if already exists
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(pathOutput)))
			fs.delete(new Path(pathOutput), true);
		
		FileInputFormat.addInputPath(job, new Path(pathSplit));
		FileOutputFormat.setOutputPath(job, new Path(pathOutput));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	
}
