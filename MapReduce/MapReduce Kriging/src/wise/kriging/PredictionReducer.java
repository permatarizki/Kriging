package wise.kriging;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import wise.kriging.OrdinaryKriging;
import wise.kriging.Semivariogram2D;
import wise.kriging.SemivariogramFit2D;

/**
 * 
 * @author Mata
 * Input for reducer : <key, value> = <line of input text, [matrixval1, matrixval1, ... matrixval1, row_matrix_number]>
 */
public class PredictionReducer 
extends Reducer<Text,Text,Text,DoubleWritable> {

	enum Time {
		SEMIVARIOGRAM,
		FIT,
		PREDICTION,
		TOTAL_POINTS
	}

	//	private long startTime;
	//	private long endTime;

	public void reduce(Text key, Iterable<Text> values, 
			Context context) throws IOException, InterruptedException {
		//System.out.println("[Reducer] <key: "+key+"> ; <mapper : value "+values.toString()+">");

		//		double minX=0.0, minY =0.0;
		//		double maxX=0.0, maxY =0.0;
		double[] minmaxXY = new double[4]; //minX, minY, maxX, maxY
		int counter=0;
		int nrbins = 50;
		double input[][] = new double [5000][3];
		String delims = " ";
		for (Text val : values) {
			//System.out.println("in for val : "+val.toString());				
			StringTokenizer inputXYZ = new StringTokenizer(val.toString(), delims);
			input[counter][0] = Double.parseDouble(inputXYZ.nextToken());
			input[counter][1] = Double.parseDouble(inputXYZ.nextToken());
			input[counter][2] = Double.parseDouble(inputXYZ.nextToken());
			//TODO find minMax
			minmaxXY= findMinMaxXY(input[counter][0], input[counter][1], minmaxXY);

			counter++;
		}

		if (counter>1000)
			System.out.println("counter : "+counter);
		
		context.getCounter(Time.TOTAL_POINTS).increment(counter);

		String [] argsSemi = new String [2];
		argsSemi [0] = String.valueOf(nrbins);
		argsSemi [1] = String.valueOf(counter);

		//cal exponential semivariogram and fit
		Semivariogram2D s = new Semivariogram2D(argsSemi, input, minmaxXY[0], minmaxXY[1], minmaxXY[2], minmaxXY[3]);

		context.getCounter(Time.SEMIVARIOGRAM).increment(s.getSemiTime());
		context.getCounter(Time.FIT).increment(s.getFitTime());


		StringTokenizer st = new StringTokenizer(key.toString(), " ");
		double gridX = Double.parseDouble(st.nextToken());
		double gridY = Double.parseDouble(st.nextToken());

		long startTime = System.currentTimeMillis();
		OrdinaryKriging ok = new OrdinaryKriging ();
		double prediction = ok.run(input, s.getRange(), s.getSill(), gridX, gridY, counter, s.getptopDist());
		//no negative value for the result
		if(prediction<0){
			prediction = 0;
		}
		long endTime = System.currentTimeMillis();
		context.getCounter(Time.PREDICTION).increment(endTime-startTime);

		DoubleWritable l = new DoubleWritable();
		l.set(prediction);

		context.write(key, l);
	}

	private double[] findMinMaxXY (double x, double y, double[] minmaxXY){
		if (minmaxXY[0] == 0 && minmaxXY[2] == 0){
			minmaxXY[0] = x;
			minmaxXY[1] = y;
			minmaxXY[2] = x;
			minmaxXY[3] = y;
		}

		if(minmaxXY[1]>y)
			minmaxXY[1] = y;
		if(minmaxXY[0]>x)
			minmaxXY[0] = x;
		if(minmaxXY[2]<x)
			minmaxXY[2] = x;
		if(minmaxXY[3]<y)
			minmaxXY[3] = y;

		return minmaxXY;
	}

}
