package wise.semivariogram.mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CalculateDistanceMapper extends Mapper<Object, Text, Text, Text>{
	private double input[][];
	private int rows=0;
	
	private double minX = 0, minY = 0;
	private double maxX = 0, maxY = 0;
	private double maxd = 0.0;
	private double maxdist = 0.0;
	
	//initialize this mapper
	protected void setup(Context context) throws IOException{
		Configuration conf = context.getConfiguration();
		String pathInput = conf.get("origin_file");
		String pathIntermediate = conf.get("inter_dir");
		rows = conf.getInt("rows", 0);
		input = new double [rows][3];// 0:X, 1:Y, 2:Z
		
		//start file read from local
		Path pt=new Path(pathInput); 
		FileSystem fin = FileSystem.get(new Configuration());
		
		BufferedReader br=new BufferedReader(new InputStreamReader(fin.open(pt)));
				
		String sCurrentLine="";
		for (int i=0; (sCurrentLine = br.readLine()) != null && i<rows ; i++){ 
			 StringTokenizer inputXYZ = new StringTokenizer(sCurrentLine, " "); 
			 input[i][0] = Double.parseDouble(inputXYZ.nextToken()); 
			 input[i][1] = Double.parseDouble(inputXYZ.nextToken()); 
			 input[i][2] = Double.parseDouble(inputXYZ.nextToken());
		  
			 findMinXY(input[i][0], input[i][1]); 
			 findMaxXY(input[i][0], input[i][1]); 
		} 
		br.close(); fin.close();
		 
		maxd = Math.sqrt(Math.pow(
				minX - maxX, 2)
				+ Math.pow(minY - maxY,
						2));

		System.out.println("maxDistance : " + maxd);

		maxdist = maxd / 2;
		System.out.println("maxdist in job1-1 : " + maxdist);
		Path ptMaxdist=new Path(pathIntermediate+"/variogram_test_maxdist");
		FileSystem fin_maxdist = FileSystem.get(new Configuration());
		BufferedWriter foutmaxDist =new BufferedWriter (new OutputStreamWriter(fin_maxdist.create(ptMaxdist)));
		foutmaxDist.write(String.valueOf(maxdist));
		foutmaxDist.close(); fin_maxdist.close();
	}
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String delims = " ";
		StringTokenizer itr = new StringTokenizer(value.toString(), delims);
		double point[] = new double[3];
		
		point[0] = Double.parseDouble(itr.nextToken());//x
		point[1] = Double.parseDouble(itr.nextToken());//y
		point[2] = Double.parseDouble(itr.nextToken());//z
		int nrLine = Integer.parseInt(itr.nextToken());
		String a = "";
		
		for (int i=nrLine ; i<rows ; i++){
//			System.out.println("i : " + i + " j : " + j);				
//			System.out.println("x1 : "+ input[i][0] +" x2 : " + input[j][0]);
//			System.out.println("y1 : "+ input[i][1] +" y2 : " + input[j][1]);
			double d=Math.sqrt( Math.pow(input[i][0]-point[0], 2)
					+ Math.pow(input[i][1]-point[1], 2));
			if (d <= maxdist) {
				
				double squrZ = Math.pow(input[i][2]
						- point[2], 2);
				a += String.valueOf(d) + " "
						+ String.valueOf(squrZ) + "\n";
			}
		}
		
		context.write(new Text(a.substring(0, a.length()-1)), new Text(""));
	}
	
	private void findMinXY(double coordX, double coordY) {
		if (minX == 0 && minY == 0) {
			minX = coordX;
			minY = coordY;
		} else {
			if (minX > coordX) {
				minX = coordX;
			}
			if (minY > coordY) {
				minY = coordY;
			}
		}

	}

	private void findMaxXY(double coordX, double coordY) {
		if (maxX == 0 && maxY == 0) {
			maxX = coordX;
			maxY = coordY;
		} else {
			if (maxX < coordX) {
				maxX = coordX;
			}
			if (maxY < coordY) {
				maxY = coordY;
			}
		}
	}
}
