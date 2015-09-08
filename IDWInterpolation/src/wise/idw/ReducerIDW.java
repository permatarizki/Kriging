package wise.idw;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerIDW extends Reducer<Text,Text,Text,DoubleWritable> {
	public void reduce(Text key, Iterable<Text> values, 
			Context context) throws IOException, InterruptedException {
		//System.out.println("[Reducer] <key: "+key+"> ; <mapper : value "+values.toString()+">");

		double numerator = 0;
		double denumerator = 0;
		double weight = 0;
		for (Text val : values) {
			//System.out.println("in for val : "+val.toString());				
			String[] elements = val.toString().split(" ");
			numerator = numerator + Double.parseDouble(elements[0]);
			denumerator = denumerator + Double.parseDouble(elements[1]);
		}

		if (denumerator == 0) {
			weight = 0;
		} else {
			weight = (double) (numerator / denumerator);
		}

		DecimalFormat df = new DecimalFormat("#.##");
		String sweight = df.format(weight);

		DoubleWritable l = new DoubleWritable();
		l.set(Double.parseDouble(sweight));

		context.write(key, l);
	}
}
