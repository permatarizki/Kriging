package wise.idw;

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
import wise.idw.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class RunIDW {
	public static void main(String[] args) throws Exception {
		System.out.println("Starting IDW Prediction ...");

		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 5) {
			System.err.println("$ hadoop jar [NAME].jar wise.idw.RunIDW <input_filePath> <output_directoryname> <searchRange> <num_reducer> <gridsize>");
			System.out.println("otherArgs.length: "+otherArgs.length);
			System.exit(2);
		}

		//Finding minimum maximum value
		long startTime = System.currentTimeMillis();
		double min_X=9999999;
		double max_X=0;
		double min_Y=9999999;
		double max_Y=0;
		int totalinputdata = 0;
		ArrayList<Double> lists_X = new ArrayList<Double>();
		BufferedReader br = null;
		Path pt=null;
		try {
			String sCurrentLine;
			//				pt =new Path("hdfs://jobs.ajou.ac.kr:8020/user/hduser/"+otherArgs[0]);0
			pt = new Path(otherArgs[0]);
			//this path for local test
//			pt =new Path("sampledummy/10000.txt");
			FileSystem fs = FileSystem.get(new Configuration());
			br=new BufferedReader(new InputStreamReader(fs.open(pt)));

			while ((sCurrentLine = br.readLine()) != null) {
				String delims = " ";
				StringTokenizer itr2 = new StringTokenizer(sCurrentLine,delims);	
				totalinputdata++;
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
		System.out.println("Total Input data: "+totalinputdata);
		System.out.println(" min_X:"+min_X);
		System.out.println(" max_X:"+max_X);
		System.out.println(" min_Y:"+min_Y);
		System.out.println(" max_Y:"+max_Y);
		long endTime = System.currentTimeMillis();
		long difference1 = endTime - startTime;

		double deltaX = Double.parseDouble(otherArgs[4]);
		double deltaY = Double.parseDouble(otherArgs[4]);

		//default grid radius is 3
		int gridRadius=Integer.parseInt(otherArgs[2]);
		conf.setInt("gridRadius", gridRadius);
		conf.setInt("gridXrange", (int)(max_X-min_X));
		conf.setInt("gridYrange", (int)(max_Y-min_Y));
		conf.set("deltaX", Double.toString(deltaX));
		conf.set("deltaY", Double.toString(deltaY));
		conf.set("min_X", Integer.toString((int)min_X));
		conf.set("min_Y", Integer.toString((int)min_Y));
		conf.set("max_X", Integer.toString((int)max_X));
		conf.set("max_Y", Integer.toString((int)max_Y));

		System.out.println("gridXrange : "+(int)(max_X-min_X)+", gridYrange : "+(int)(max_Y-min_Y));

		// delete the outputpath if already exists
		FileSystem fs2 = FileSystem.get(conf);
		if (fs2.exists(new Path(otherArgs[1])))
			fs2.delete(new Path(otherArgs[1]), true);

		startTime = System.currentTimeMillis();
		Job job = new Job(conf, otherArgs[0]+", output: "+otherArgs[1]+", searchRange:"+otherArgs[2]+", numReducer:"+otherArgs[3]+ "gridsizes:"+otherArgs[4]);

		job.setJarByClass(RunIDW.class);
		job.setMapperClass(MapperIDW.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setCombinerClass(Reducer.class);
		job.setReducerClass(ReducerIDW.class);
		job.setNumReduceTasks( Integer.parseInt(otherArgs[3]));

		job.setInputFormatClass(NLineInputFormat.class);
		NLineInputFormat.addInputPath(job, pt);
		job.getConfiguration().setInt(
				"mapreduce.input.lineinputformat.linespermap", 1000);

		//FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		job.waitForCompletion(true);

		endTime = System.currentTimeMillis();
		long difference2 = endTime - startTime;

		System.out.println("Elapsed Finding Min,Max XY: " + difference1+" milliseconds");
		System.out.println("Elapsed MR processing: " + difference2+" milliseconds");

	}
}

