package wise.idw;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperIDW  extends Mapper<Object, Text, Text, Text> {
	double deltaX;
	double deltaY;
	int min_X;
	int min_Y;
	int max_X;
	int max_Y;
	int gridRadius;
	int gridXrange, gridYrange;

	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		deltaX = Double.parseDouble(conf.get("deltaX"));// 1 grid size
		deltaY = Double.parseDouble(conf.get("deltaY"));
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
//		 System.out.println("[Mapper] <key,value> : <"+key+","+value+">");

//		 System.out.println("min_X:"+min_X);
//		 System.out.println("min_Y:"+min_Y);
//		 System.out.println("max_X:"+max_X);
//		 System.out.println("max_Y:"+max_Y);
//		 System.out.println("deltaX:"+deltaX);
//		 System.out.println("deltaY:"+deltaY);

        String[] elements = value.toString().split(" ");
        double xpoint = Double.parseDouble(elements[0]);
        double ypoint = Double.parseDouble(elements[1]);
        double zpoint = Double.parseDouble(elements[2]);
        double xpoint_floor = Math.floor(xpoint);
        double ypoint_floor = Math.floor(ypoint);
        double start_x = xpoint_floor - gridRadius;
        double start_y = ypoint_floor - gridRadius;
        double anchorx = start_x;
        double anchory = start_y;
        
        Text t = new Text();
        while(start_x <= (anchorx+2*gridRadius)){
            while(start_y <= (anchory+2*gridRadius)) {
                if((start_x>=min_X)&&(start_y>=min_Y)&&(start_x<=max_X)&&(start_y<=max_Y)){
                    //calculate distance start_x with xpoint
                    double deltaX = start_x - xpoint;
                    double deltaY = start_y - ypoint;
                    double distance = Math.sqrt(deltaX * deltaX + deltaY * deltaY);
                    //filter based on radius range
                    if (distance < gridRadius) {
                        //calculate nominator & denominator
                        if (distance == 0) {
                            double numerator = 0;
                            double denominator = 0;
                            t.set(Double.toString(start_x) + " " +
                                    Double.toString(start_y));
    						context.write(t, new Text(Double.toString(numerator)+" "+Double.toString(denominator)));
                        } else {
                            double numerator = (double) (zpoint / distance);
                            double denominator = (double) (1 / distance);
                            t.set(Double.toString(start_x) + " " +
                                    Double.toString(start_y));
    						context.write(t, new Text(Double.toString(numerator)+" "+Double.toString(denominator)));
                        }
                    }
                }
                start_y = start_y + gridRadius;
            }
            start_y = anchory;
            start_x = start_x + gridRadius;
        }
	}
}
