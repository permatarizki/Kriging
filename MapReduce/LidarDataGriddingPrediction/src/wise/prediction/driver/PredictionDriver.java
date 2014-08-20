package wise.prediction.driver;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import wise.gridding.mapreduce.CreateGriddingPoints;
import wise.gridding.mapreduce.FindKClosestNodes;
import wise.gridding.write.WriteFiles;
import wise.prediction.mapreduce.CalculateDistanceVariogramWeight;
import wise.semivariogram.mapreduce.CalculateDistance;
import wise.semivariogram.mapreduce.Semivariogram;


public class PredictionDriver {

	public static void main(String[] args) throws Exception {
		String path = "hdfs://jobs.ajou.ac.kr:8020/user/hduser";
		String localPath = "/home/seop/workspace/LidarDataGriddingPrediction";
		
		//for hdfs path
		/*Path job1Input = new Path(path + "/job1_input/sample_data(100).txt");
		Path job1Intermediate = new Path(path + "/job1_intermediate");
		Path job1Output = new Path(path + "/job1_output");*/
		
		//for local path
		Path job1_1Input = new Path(localPath + "/job1-1_input/variogram_test");
		Path job1_1Intermediate = new Path(localPath + "/job1-1_intermediate");
		Path job1_1Split = new Path(localPath +"/job1-1_split");
		Path job1_1Output = new Path(localPath + "/job1-1_output");
		Path job1_2Input = new Path(localPath + "/job1-1_output");
		Path job1_2Output = new Path(localPath + "/job1-2_output");
		
		Path job2_1Input = new Path(path + "/job1_input/sample_data(100).txt");
		Path job2_1Output = new Path(path + "/job2-1_output");
		Path writefileInput = new Path(path + "/job2-1_output");
		Path writefileOutput = new Path(path + "/writefile_output");
		Path job2_2Input = new Path(path + "/writefile_output");
		Path job2_2Output = new Path(path + "/job2-2_output");
		Path job3Input = new Path(path + "/job2-2_output");
		Path job3Output = new Path(path + "/job3_output");
		
		//set number of input Lidar points
		int numberofPoints = 100;
		
		////set size of grid
		//int sizeofGrid = 1;
		
		//set max number of nearest search around grid point
		int maxNearestSearchinGrid = 10;
	
		//set number of bins
		int numberofBins = 50;
		
		String[] job1_1Args = new String [5];
		job1_1Args[0] = job1_1Input.toString();//input path
		job1_1Args[1] = job1_1Intermediate.toString();//intermediate path
		job1_1Args[2] = job1_1Split.toString();//split Path
		job1_1Args[3] = job1_1Output.toString();//output path
		job1_1Args[4] = String.valueOf(numberofPoints);//rows
		
		String[] job1_2Args = new String [5];
		job1_2Args[0] = job1_2Input.toString();//input path
		job1_2Args[1] = job1_1Intermediate.toString();//intermediate path
		job1_2Args[2] = job1_2Output.toString();//output path
		job1_2Args[3] = String.valueOf(numberofPoints);//rows
		job1_2Args[4] = String.valueOf(numberofBins);//nrbins
		
		String[] job2_1Args = new String [2];
		job2_1Args[0] = job2_1Input.toString();//input path
		job2_1Args[1] = job2_1Output.toString();//output path
		
		String[] writefArgs = new String [2];
		writefArgs[0] = writefileInput.toString();//input path
		writefArgs[1] = writefileOutput.toString();//output path
		
		String[] job2_2Args = new String [4];
		job2_2Args[0] = job2_2Input.toString();//input path
		job2_2Args[1] = job1_1Input.toString();//origin input path
		job2_2Args[2] = job2_2Output.toString();//output path
		job2_2Args[3] = String.valueOf(maxNearestSearchinGrid);//max number of nearest search around grid point
		
		String[] job3Args = new String [3];
		job3Args[0] = job3Input.toString();//input path
		job3Args[1] = job3Output.toString();//output path
		job3Args[2] = String.valueOf(maxNearestSearchinGrid);//max number of nearest search around grid point
		
		//job1-1 : Calculate distance
		System.out.println("::::::::::::Calculate distance");
		int exitCode = ToolRunner.run(new CalculateDistance(), job1_1Args);
						
		//job1-2 : Semivariogram
		System.out.println("::::::::::::semivariogram");
		exitCode = ToolRunner.run(new Semivariogram(), job1_2Args);
		
/*		//job2-1 : Gridding part1
		System.out.println("::::::::::::creategridding");
		exitCode = ToolRunner.run(new CreateGriddingPoints(), job2_1Args);
		
		//write Grid Points : Gridding part2
		new WriteFiles().run(writefArgs);
		
		//job2-2 : Gridding part3
		System.out.println("::::::::::::kCloesestnodes");
		exitCode = ToolRunner.run(new FindKClosestNodes(), job2_2Args);
		
		//job3 : Prediction
		System.out.println("::::::::::::prediction");
		exitCode = ToolRunner.run(new CalculateDistanceVariogramWeight(), job3Args);*/
	}
}
