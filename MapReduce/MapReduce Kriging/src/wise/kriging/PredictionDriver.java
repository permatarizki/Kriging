package wise.kriging;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import wise.kriging.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PredictionDriver {
	public static void main(String[] args) throws Exception {
		//System.out.println("[Finding K Closes Nodes]");

		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 5) {
			System.err.println("$ hadoop jar [NAME].jar wise.kriging.PredictionDriver <input_filePath> <output_directoryname> <searchRange> <num_reducer> <gridsize>");
			System.exit(2);
		}

		//Finding minimum maximum value
		long startTime = System.currentTimeMillis();
		double min_X=9999999;
		double max_X=0;
		double min_Y=9999999;
		double max_Y=0;
		ArrayList<Double> lists_X = new ArrayList<Double>();
		BufferedReader br = null;
		Path pt=null;
		try {
			String sCurrentLine;
			pt =new Path("hdfs://jobs.ajou.ac.kr:8020/user/hduser/"+otherArgs[0]);
			
			//this path for local test
			//pt =new Path("sampledummy/Data3_XYZ_Default.txt");
			FileSystem fs = FileSystem.get(new Configuration());
			br=new BufferedReader(new InputStreamReader(fs.open(pt)));

			while ((sCurrentLine = br.readLine()) != null) {
				String delims = " ";
				StringTokenizer itr2 = new StringTokenizer(sCurrentLine,delims);	

				double[] A = {Double.parseDouble(itr2.nextElement().toString()),Double.parseDouble(itr2.nextElement().toString())};
				if(A[0]<min_X)
					min_X = A[0];
				if(A[0]>max_X)
					max_X = A[0];
				if(A[1]<min_Y)
					min_Y = A[1];
				if(A[1]>max_Y)
					max_Y = A[1];
				lists_X.add(A[0]);

			}
		}catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		min_X = Math.floor(min_X);
		max_X = Math.ceil(max_X);
		min_Y = Math.floor(min_Y);
		max_Y = Math.ceil(max_Y);
		System.out.println("min_X:"+min_X);
		System.out.println("max_X:"+max_X);
		System.out.println("min_Y:"+min_Y);
		System.out.println("max_Y:"+max_Y);
		long endTime = System.currentTimeMillis();
		long difference1 = endTime - startTime;
		

	/*	//sort this input value
		System.out.println("Sorting values...");
		lStartTime = new Date().getTime();
		Collections.sort(lists_X);
		lEndTime = new Date().getTime();
		difference = lEndTime - lStartTime;
		System.out.println("Elapsed Sorting X nodes: " + difference+" milliseconds");
		System.out.println("data_size:"+lists_X.size());*/

		//Start map-reduce phase

		//default deltaX Y is 1
		int deltaX=Integer.parseInt(otherArgs[4]); //this is represent X grid sizes in meter
		int deltaY=Integer.parseInt(otherArgs[4]); //this is represent Y grid sizes in meter
		//default grid radius is 3
		int gridRadius=Integer.parseInt(otherArgs[2]);
		conf.setInt("gridRadius", gridRadius);
		conf.setInt("gridXrange", (int)(max_X-min_X));
		conf.setInt("gridYrange", (int)(max_Y-min_Y));
		conf.set("deltaX", Integer.toString(deltaX));
		conf.set("deltaY", Integer.toString(deltaY));
		conf.set("min_X", Integer.toString((int)min_X));
		conf.set("min_Y", Integer.toString((int)min_Y));
		conf.set("max_X", Integer.toString((int)max_X));
		conf.set("max_Y", Integer.toString((int)max_Y));
		
		System.out.println("gridXrange : "+(int)(max_X-min_X)+", gridYrange : "+(int)(max_Y-min_Y));
		
		startTime = System.currentTimeMillis();
		Job job = new Job(conf, otherArgs[0]+","+otherArgs[1]+",radius:"+otherArgs[2]);

		job.setJarByClass(PredictionDriver.class);
		job.setMapperClass(GriddingMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setCombinerClass(Reducer.class);
		job.setReducerClass(PredictionReducer.class);
		job.setNumReduceTasks( Integer.parseInt(otherArgs[3]));

		// delete the outputpath if already exists
		FileSystem fs2 = FileSystem.get(conf);
		if (fs2.exists(new Path(otherArgs[1])))
			fs2.delete(new Path(otherArgs[1]), true);

		job.setInputFormatClass(NLineInputFormat.class);
		NLineInputFormat.addInputPath(job, pt);
		job.getConfiguration().setInt(
		"mapreduce.input.lineinputformat.linespermap", 100000);
		
		//FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		job.waitForCompletion(true);
		
		endTime = System.currentTimeMillis();
		long difference2 = endTime - startTime;
		
		System.out.println("Elapsed Finding Min,Max XY: " + difference1+" milliseconds");
		System.out.println("Elapsed MR processing: " + difference2+" milliseconds");
		
	}
}
