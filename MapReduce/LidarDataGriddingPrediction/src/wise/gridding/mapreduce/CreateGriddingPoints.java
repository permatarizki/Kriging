package wise.gridding.mapreduce;



/**
 * The purposes of this code is finding gridding points from LIDAR data
 * Input XYZ lidar data (located on input file): ex: 607439.680 5245000.970 585.510
 */
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;

public class CreateGriddingPoints extends Configured implements Tool{	
	
	public static class TokenizerMapper 
	extends Mapper<Object, Text, IntWritable, Text>{

		public void map(Object key, Text value, Context context
		) throws IOException, InterruptedException {
//			System.out.println("mapper : value "+value);

			// parsing X, Y value 
			String delims = "\t";
			StringTokenizer itr = new StringTokenizer(value.toString(),delims);	
			
			//write counter
			context.write(new IntWritable(1), new Text("1"));
			
			//write X value
			context.write(new IntWritable(2), new Text(itr.nextElement().toString()));

			//write X value
			context.write(new IntWritable(3), new Text(itr.nextElement().toString()));
			
		}

	}

	/**
	 * 
	 * @author Mata
	 * 
	 */
	public static class IntSumReducer 
	extends Reducer<IntWritable,Text,Text,Text> {

		public void reduce(IntWritable key, Iterable<Text> values, 
				Context context
		) throws IOException, InterruptedException {

			
			switch(key.get()){
			case 1:
				int counter_node = 0;
				for(Text value: values){
//					System.out.println("1: "+value.toString());
					//count total node
					counter_node++;
				}
				
				//write total nodes
				context.write(new Text("total_nodes"), new Text(Integer.toString(counter_node)));
				break;
			case 2: 
				double min_X=0;
				double max_X=0;
				int counterX = 0;
				for(Text value: values){
					double temp_val = Double.parseDouble(value.toString());
//					System.out.println("2: "+value.toString());
					if(counterX == 0){
						min_X = temp_val;
						max_X = temp_val;
					}else{
						if(temp_val<min_X)
							min_X = temp_val;
						if(temp_val>max_X)
							max_X = temp_val;
					}
					counterX++;
				}
				
				context.write(new Text("min_X"), new Text(Double.toString(min_X)));
				context.write(new Text("max_X"), new Text(Double.toString(max_X)));
				
				
				break;
			case 3: 
				double min_Y=0;
				double max_Y=0;
				int counterY = 0;
				for(Text value: values){
					double temp_val = Double.parseDouble(value.toString());
//					System.out.println("2: "+value.toString());
					if(counterY == 0){
						min_Y = temp_val;
						max_Y = temp_val;
					}else{
						if(temp_val<min_Y)
							min_Y = temp_val;
						if(temp_val>max_Y)
							max_Y = temp_val;
					}
					counterY++;
				}
				
				context.write(new Text("min_Y"), new Text(Double.toString(min_Y)));
				context.write(new Text("max_Y"), new Text(Double.toString(max_Y)));
				break;
			
			default:
					
			}
	

		}
	}

	
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "Create Grid points");
		job.setJarByClass(TokenizerMapper.class);
		job.setMapperClass(TokenizerMapper.class);
		//this line code can be omitted but affect the performance
		//		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		//		job.setNumReduceTasks(matrix_size);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		// delete the outputpath if already exists
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(otherArgs[1])))
			fs.delete(new Path(otherArgs[1]), true);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		return job.waitForCompletion(true) ? 0 : 1;
		
	}
	
	/*public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new CreateGriddingPoints(), args);
		System.exit(exitCode);
	}*/

}

