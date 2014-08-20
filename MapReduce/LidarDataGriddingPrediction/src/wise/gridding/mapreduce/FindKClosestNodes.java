package wise.gridding.mapreduce;

/**
 * The purposes of this code is finding K closest points from LIDAR data
 * Input : Gridding Points
 * Output : Gridding point with their K closest points
 * 
 */
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

import edu.wlu.cs.levy.CG.KDTree;
import edu.wlu.cs.levy.CG.KeyDuplicateException;
import edu.wlu.cs.levy.CG.KeySizeException;

public class FindKClosestNodes extends Configured implements Tool {
	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			System.out.println("[Mapper] <key,value> : <" + key + "," + value
					+ ">");

			context.write(new Text(key.toString()), value);
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			//
			Configuration conf = context.getConfiguration();
			String originInput = conf.get("originInput");
			int maxNearestSearch = Integer.parseInt(conf.get("maxsearch"));

			String kClosestNodes = "";
			double[] oneGridPoint = {0,0};
			for (Text value : values) {
				System.out.println("1[Reducer] <key,value> : <" + key + ","
						+ value + ">");
				// parsing X, Y gridding point to array
				String delims = " ,";
				StringTokenizer itr = new StringTokenizer(value.toString(),
						delims);

				oneGridPoint [0] =	Double.parseDouble(itr.nextElement().toString());
				oneGridPoint [1] =  Double.parseDouble(itr.nextElement().toString());

				// make a KD-tree and hashmap for storing LIDAR nodes
				KDTree<String> kd = new KDTree<String>(2);
				HashMap<String, String> hm = new HashMap<String, String>();

				// TODO load LIDAR data from file and parse it into double array
				BufferedReader br = null;
				try {

					String sCurrentLine;

					//file read from local
//					br = new BufferedReader(new FileReader(originInput
//							+ "/variogram_test"));

					
					//file read from HDFS
					Path pt=new Path(originInput); 
					FileSystem fs = FileSystem.get(new Configuration());
					
					br=new BufferedReader(new InputStreamReader(fs.open(pt)));
					 
					int ctr = 0;

					while ((sCurrentLine = br.readLine()) != null) {
						ctr++;
						// System.out.println(sCurrentLine);
						String delims2 = "\t";
						StringTokenizer itr2 = new StringTokenizer(
								sCurrentLine, delims2);

						double[] A = {
								Double.parseDouble(itr2.nextElement()
										.toString()),
								Double.parseDouble(itr2.nextElement()
										.toString()) };
						hm.put(Integer.toString(ctr), Double.toString(A[0])
								+ "," + Double.toString(A[1]));
						try {
							kd.insert(A, new String(Integer.toString(ctr)));
						} catch (KeySizeException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (KeyDuplicateException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

					}

				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					try {
						if (br != null)
							br.close();
					} catch (IOException ex) {
						ex.printStackTrace();
					}
				}

				try {
					//System.out.println("grid point : " + oneGridPoint[0]);
					// find K's nearest neighbor
					List<String> nearest_nodes = kd.nearest(oneGridPoint,
							maxNearestSearch);

					
					for (String node : nearest_nodes) {
						// System.out.println(hm.get(node));
						// concatenate all closest nodes
						kClosestNodes = " " + kClosestNodes + hm.get(node)
								+ " ";
					}
					// System.out.println(kClosestNodes);

					
				} catch (Exception e) {
					System.err.println(e);
				}
			}
			
			context.write(new Text(Double.toString(oneGridPoint[0])
					+ "," + Double.toString(oneGridPoint[1])),
					new Text(kClosestNodes));
			
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 4) {
			System.err
					.println("Usage: wordcount <in> <origin input> <out> <max nearest search>");
			System.exit(2);
		}

		conf.set("originInput", otherArgs[1]);
		conf.set("maxsearch", otherArgs[3]);

		Job job = new Job(conf, "Find K Closest Nodes");
		job.setJarByClass(TokenizerMapper.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		// job.setNumReduceTasks(matrix_size);

		// delete the outputpath if already exists
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(otherArgs[2])))
			fs.delete(new Path(otherArgs[2]), true);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		return job.waitForCompletion(true) ? 0 : 1;

	}

}
