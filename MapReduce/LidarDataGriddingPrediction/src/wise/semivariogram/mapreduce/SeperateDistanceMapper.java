package wise.semivariogram.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class SeperateDistanceMapper extends Mapper<Object, Text, Text, Text>{
	
	double maxdist;
	int nrbins;
	
	double [] distBins;
	int [] idxDistBins;
	double delta;
	
	//initialize this mapper
	protected void setup(Context context){
		Configuration conf = context.getConfiguration();
		maxdist = Double.parseDouble(conf.get("maxdist"));
		System.out.println("maxdist in map : " + maxdist);
		nrbins = Integer.parseInt(conf.get ("nrbins"));
		//nrpoints = Integer.parseInt(conf.get("nrpoints"));
		
		distBins = new double[nrbins+2];//distBins is called "edges" in matlab code
		idxDistBins = new int[nrbins+2];
		delta = maxdist/nrbins;

		//make a seperate distance
		distBins[0] = 0.0;
		for (int i=1 ; i<nrbins ; i++){
			distBins[i] = distBins[i-1] + delta;
		}
		distBins[nrbins] = maxdist;
	}
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String delims = " ";
		StringTokenizer itr = new StringTokenizer(value.toString(), delims);
		//these idxI and idxJ might will be deprecated.
//		int idxI = Integer.parseInt(itr.nextToken());
//		int idxJ = Integer.parseInt(itr.nextToken());
		double distance = Double.parseDouble(itr.nextToken()); 
		double squrZ = Double.parseDouble(itr.nextToken()); 
		
		int idx=0;
		
		for (int idxBins=0 ; idxBins<nrbins+1 ; idxBins++){
			if (distance > distBins[idxBins]
				&& distance <= distBins[idxBins+1]){

				idx = idxBins;
				break;
			}
		}
		
		context.write(new Text(String.valueOf(idx)), new Text(String.valueOf(squrZ)));
	}
}
