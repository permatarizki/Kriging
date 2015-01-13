package wise.kriging;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class GriddingMapper extends Mapper<Object, Text, Text, Text> {
	int deltaX;
	int deltaY;
	int min_X;
	int min_Y;
	int max_X;
	int max_Y;
	int gridRadius;
	int gridXrange, gridYrange;

	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		deltaX = Integer.parseInt(conf.get("deltaX"));// 1 grid size
		deltaY = Integer.parseInt(conf.get("deltaY"));
		min_X = Integer.parseInt(conf.get("min_X"));
		min_Y = Integer.parseInt(conf.get("min_Y"));
		max_X = Integer.parseInt(conf.get("max_X"));
		max_Y = Integer.parseInt(conf.get("max_Y"));
		gridRadius = conf.getInt("gridRadius", 0);
		gridXrange = conf.getInt("gridXrange", 0); // grid range of x axis
		gridYrange = conf.getInt("gridYrange", 0);
	}

	public void map(Object key, Text value, Context context)
	throws IOException, InterruptedException {
		// System.out.println("[Mapper] <key,value> : <"+key+","+value+">");

		// System.out.println("min_X:"+min_X);
		// System.out.println("min_Y:"+min_Y);
		// System.out.println("max_X:"+max_X);
		// System.out.println("max_Y:"+max_Y);
		// System.out.println("deltaX:"+deltaX);
		// System.out.println("deltaY:"+deltaY);

		String delims = " ";
		StringTokenizer itr = new StringTokenizer(value.toString(), delims);

		double[] oneLidarPoint = {
				Double.parseDouble(itr.nextElement().toString()),
				Double.parseDouble(itr.nextElement().toString()) };

		// grid point (integer)
		int i = (int) (oneLidarPoint[0] - min_X);
		int j = (int) (oneLidarPoint[1] - min_Y);

		// Lets make anchor point is multiple of gridsize
		/*
		while((oneLidarPoint[0]%deltaX) != 0){
			oneLidarPoint[0]=oneLidarPoint[0]-1;
			System.out.println("[Mapper] <key,value> : <"+key+","+value+">");
		}
		while((oneLidarPoint[0]%deltaX) != 0){
			oneLidarPoint[1]=oneLidarPoint[1]-1;
			System.out.println("[Mapper] <key,value> : <"+key+","+value+">");
		}
		*/

		// real grid point
		double realXGridPoints = 0;
		double realYGridPoints = 0;
		realXGridPoints = (double) i + deltaX / (double) 2 + min_X;
		realYGridPoints = (double) j + deltaY / (double) 2 + min_Y;

		int k, p;
		double radX = 0, radY = 0; // This is the GridPoint which is located inside square of GridRadius
		Text t = new Text();

		if(gridRadius>deltaX/2 && gridRadius>deltaY/2 ){
			radX = realXGridPoints - (gridRadius) + 1;
			radY = realYGridPoints - (gridRadius) + 1;
			for (k = 0; k < 2 * ((int)(gridRadius/deltaX)) - 1; k++) {
				radX = radX + k*deltaX;
				for (p = 0; p < 2 * ((int)(gridRadius/deltaY)) - 1; p++) {
					radY = radY + p*deltaY;
					if (radX >= min_X && radX < min_X + gridXrange && radY >= min_Y
							&& radY < min_Y + gridYrange){
						//System.out.println("rad x:"+radX+", rad y:"+radY+", value: "+value);
						t.set(Double.toString(radX) + "," + Double.toString(radY));
						context.write(t, value);				
					}
				}
			}			
		}else{
			radX = realXGridPoints ;
			radY = realYGridPoints ;
			if (radX >= min_X && radX < min_X + gridXrange && radY >= min_Y
					&& radY < min_Y + gridYrange){
				//System.out.println("rad x:"+radX+", rad y:"+radY+", value: "+value);
				t.set(Double.toString(radX) + "," + Double.toString(radY));
				context.write(t, value);				
			}
		}



		/*
		radX = realXGridPoints - gridRadius + 1;
		for (k = 0; k < 2 * gridRadius - 1; k++) {
			radY = realYGridPoints - gridRadius + 1;
			for (p = 0; p < 2 * gridRadius - 1; p++) {

				if (radX >= min_X && radX < min_X + gridXrange && radY >= min_Y
						&& radY < min_Y + gridYrange) {
//					 System.out.println("rad x:"+radX+", rad y:"+radY+", value: "+value);
					t.set(Double.toString(radX) + "," + Double.toString(radY));
					context.write(t, value);
				}
				radY++;
			}
			radX++;
		}
		 */

	}

}
