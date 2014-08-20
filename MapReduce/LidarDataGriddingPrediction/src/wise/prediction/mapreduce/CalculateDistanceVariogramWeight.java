package wise.prediction.mapreduce;

/**
 * The purposes of this code is calculating matrix distance for each point
 * and writes it on output file
 * 
 * Input:
 * [Mapper] : <key,value> : <line_offset, [node_S0, node_S1, node_S2, node_S3, ...., node_S30], where S0 is grid point, S1-S30 is closest points from LIDAR data
 * [reduce] : <key,value> : <line_offset, [node_S0, node_S1, node_S2, node_S3, ...., node_S30], where S0 is grid point, S1-S30 is closest points from LIDAR data
 * 
 */
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Arrays;
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
import org.apache.hadoop.util.Tool;

public class CalculateDistanceVariogramWeight extends Configured implements Tool{	
	public static class TokenizerMapper 
	extends Mapper<Object, Text, Text, Text>{

		public void map(Object key, Text value, Context context
		) throws IOException, InterruptedException {

			//get counter value
			Configuration conf = context.getConfiguration();
			int matrixsize = Integer.parseInt(conf.get("matrix.size.NbyN"));

			double[] input_matrix_X = new double[matrixsize];
			double[] input_matrix_Y = new double[matrixsize];
			double[][] distance = new double[matrixsize][matrixsize];
			double[][] variogram_result = new double[matrixsize][matrixsize];

			String delims = " ,";
			StringTokenizer itr = new StringTokenizer(value.toString(),delims);

			//fill the input matrix
			int i=0;
			while(itr.hasMoreTokens()){		
				input_matrix_X[i] = Double.parseDouble(itr.nextToken().toString());
				input_matrix_Y[i] = Double.parseDouble(itr.nextToken().toString());
				i++;			
			}

			//check contents
			int j;
			//			for(j=0;j<i;j++){
			//				System.out.println("input_matrix_X["+j+"]"+input_matrix_X[j]);
			//				System.out.println("input_matrix_Y["+j+"]"+input_matrix_Y[j]);
			//			}

			//calculate each distance and put it on distance matrix
			for(i=0;i<matrixsize;i++){
				for(j=0;j<matrixsize;j++){
					distance[i][j]=Math.sqrt(Math.pow(input_matrix_X[i]-input_matrix_X[j], 2)+Math.pow(input_matrix_Y[i]-input_matrix_Y[j], 2));
				}
			}

			//check value
			System.out.println("Print Distance");
			for(i=0;i<matrixsize;i++){
				for(j=0;j<matrixsize;j++){
					System.out.print(" "+distance[i][j]);
				}
				System.out.println("");
			}

			//do calculate variogram
			for(i=0;i<matrixsize;i++){
				for(j=0;j<matrixsize;j++){
					variogram_result[i][j] = semivariogramExponential(distance[i][j]);
				}
			}

			//check variogram value 
			System.out.println("Print Variogram");
			for(i=0;i<matrixsize;i++){
				for(j=0;j<matrixsize;j++){
					System.out.print(" "+variogram_result[i][j]);
				}
				System.out.println("");
			}

			// Pass Determinant value
			double[] temp_matrix_A = new double [matrixsize-1];
			double[][] temp_matrix_B = new double [matrixsize-1][matrixsize-1];

			// Fill each matrix A & B value
			for(i=0;i<matrixsize-1;i++){
				temp_matrix_A[i] = variogram_result[0][i+1];
			}
			for(i=0;i<matrixsize-1;i++){
				for(j=0;j<matrixsize-1;j++){
					temp_matrix_B[i][j] = variogram_result[i+1][j+1];
				}
			}

			//check temp_matrix_B value 
			System.out.println("Print temp_matrix_B");
			for(i=0;i<matrixsize-1;i++){
				for(j=0;j<matrixsize-1;j++){
					System.out.print(" "+temp_matrix_B[i][j]);
				}
				System.out.println("");
			}

			//do calculate determinant
			//calc det 
			double[] det_matrix = new double[matrixsize]; 

			det_matrix[0] = determinant(temp_matrix_B, matrixsize-1);		
			

			
			//substitute temp_matrix_A to temp_matrix_B on each column
			for(i=0;i<matrixsize-1;i++){
				double[] temp = new double [matrixsize-1];
				for(j=0;j<matrixsize-1;j++){
					temp[j] = temp_matrix_B[j][i];
					temp_matrix_B[j][i] = temp_matrix_A[j];
				}
				//calculate determinant
				det_matrix[i+1]=determinant(temp_matrix_B, matrixsize-1);
				//give back to original value
				for(j=0;j<matrixsize-1;j++){
					temp_matrix_B[j][i] = temp[j];
				}
				
				System.out.println("det_matrix[i+1] "+det_matrix[i+1]);
			}
			
			//check temp_matrix_B value 
			System.out.println("Print temp_matrix_B");
			for(i=0;i<matrixsize-1;i++){
				for(j=0;j<matrixsize-1;j++){
					System.out.print(" "+temp_matrix_B[i][j]);
				}
				System.out.println("");
			}

			for(i=0;i<matrixsize-1;i++){
				context.write(new Text(key.toString()), new Text(Double.toString(det_matrix[i+1]/det_matrix[0])));
			}
			
		}

		//calculate variogram
		private static double semivariogramExponential(double h) {
			return 10 * (1 - Math.pow(Math.E, - (h / 3.33)));
		}


		//calculate determinant
		private double determinant(double A[][],int N){
			double m[][];
			double res;
			if(N == 1)
				res = A[0][0];
			else if (N == 2){
				res = A[0][0]*A[1][1] - A[1][0]*A[0][1];
			}
			else{
				res=0;
				for(int j1=0;j1<N;j1++){
					m = new double[N-1][];
					for(int k=0;k<(N-1);k++)
						m[k] = new double[N-1];
					for(int i=1;i<N;i++){
						int j2=0;
						for(int j=0;j<N;j++){
							if(j == j1)
								continue;
							m[i-1][j2] = A[i][j];
							j2++;
						}
					}
					res += Math.pow(-1.0,1.0+j1+1.0)* A[0][j1] * determinant(m,N-1);
				}
			}
			return res;
		}

	}

	/**
	 * 
	 * @author Mata
	 */
	public static class IntSumReducer 
	extends Reducer<Text,Text,Text,Text> {

		public void reduce(Text key, Iterable<Text> values, 
				Context context
		) throws IOException, InterruptedException {

			double weight=0;
			for(Text value: values){
				System.out.println(value);
				weight= weight+Double.parseDouble(value.toString());
			}
			
			context.write(new Text(key.toString()), new Text(Double.toString(weight)));	
		}
	}

	public int run(String[] args) throws Exception {
		//define global variable
		int matrix_size = 0;  //this size is K closest Nodes + 1

		Configuration conf = new Configuration();		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: wordcount <in> <out> <grid_and_KclosestNodes>");
			System.exit(2);
		}

		matrix_size = Integer.parseInt(otherArgs[2]);
		conf.set("line.counter", "0");
		conf.set("matrix.size.NbyN", Integer.toString(matrix_size+1));
		
		Job job = new Job(conf, "Prediction");
		job.setJarByClass(TokenizerMapper.class);
		job.setMapperClass(TokenizerMapper.class);
		//this line code can be omitted but affect the performance
		//		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		//		job.setNumReduceTasks(matrix_size);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// delete the outputpath if already exists
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(otherArgs[1])))
			fs.delete(new Path(otherArgs[1]), true);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

}
