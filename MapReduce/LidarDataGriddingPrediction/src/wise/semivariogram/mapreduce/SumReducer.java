package wise.semivariogram.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SumReducer extends Reducer <Text,Text,Text,Text>{
	private Text result = new Text();
	
	double maxdist;
	int nrbins;
	//int nrpoints;

	double [] distance;
	double [] semivar;
	
	protected void setup(Context context){
		Configuration conf = context.getConfiguration();
		maxdist = Double.parseDouble(conf.get("maxdist"));
		//System.out.println("maxdist in reducer : " + maxdist);
		nrbins = Integer.parseInt(conf.get ("nrbins"));
		//System.out.println("nrbins in reducer : " + nrbins);
		//nrpoints = Integer.parseInt(conf.get("nrpoints"));
		
		distance = new double[nrbins+2];
		semivar = new double[nrbins+2];
		
		double delta = maxdist/nrbins;
		distance[0] = delta/2;
		
		for (int i=1 ; i<nrbins ; i++){
			distance[i] = distance[i-1] + delta;
		}
	}
	
	public void reduce(Text key, Iterable<Text> values, 
			Context context
	) throws IOException, InterruptedException {
		
		int nrSamedist = 0;
		double sumZ = 0.0;
		
		
		//iterate for each original one line values
		for (Text val : values) {
			nrSamedist++;
			
			/*String delims = " ";
			StringTokenizer itr = new StringTokenizer(val.toString(), delims);*/
			
			sumZ += Double.parseDouble(val.toString());
		}
		System.out.println("sumZ : " + sumZ);
		
		
		result.set( String.valueOf(sumZ / (nrSamedist*2)) +"\t"+
				String.valueOf(distance[Integer.parseInt(key.toString())]) +"\t"+
				String.valueOf(nrSamedist));
		
		context.write(new Text(key.toString()), result);	
	}
}
