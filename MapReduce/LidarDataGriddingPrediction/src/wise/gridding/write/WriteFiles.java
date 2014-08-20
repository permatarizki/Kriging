package wise.gridding.write;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

public class WriteFiles {

	public static void run(String[] args) throws IOException {
		if (args.length != 2) {
			System.err
					.println("Usage: WriteFiles <inputPath> <outputPath>");
			System.exit(2);
		}
		
		String input = args[0];
		String output = args[1];
		
		int min_X = 607000;
		int max_X = 607999;
		int min_Y = 5245000;
		int max_Y = 5245999;
		
		//start file read from HDFS
		Path pt=new Path(input+"/part-r-00000"  ); 
		FileSystem fin = FileSystem.get(new Configuration());
		
		BufferedReader br=new BufferedReader(new InputStreamReader(fin.open(pt)));
		
		br.readLine();
		
		String sCurrentLine = "";
		for (int i = 0; (sCurrentLine = br.readLine()) != null ; i++) {
			StringTokenizer inputMinMax = new StringTokenizer(sCurrentLine, "\t");
			
			inputMinMax.nextToken();
			if (i==0) min_X = (int) Math.floor(Double.parseDouble(inputMinMax.nextToken()));
			else if (i==1) max_X = (int) Math.ceil(Double.parseDouble(inputMinMax.nextToken()));
			else if (i==2) min_Y = (int) Math.floor(Double.parseDouble(inputMinMax.nextToken()));
			else if (i==3) max_Y = (int) Math.ceil(Double.parseDouble(inputMinMax.nextToken()));
		}
		br.close(); fin.close();
		 
		//start file write from HDFS
		Path ptGrid=new Path(output+"/writegrid_output"); 
		FileSystem fin_grid = FileSystem.get(new Configuration());
		
		BufferedWriter foutGrid =new BufferedWriter (new OutputStreamWriter(fin_grid.create(ptGrid)));
		for (int i=min_X ; i <= max_X ; i++){
			for (int j=min_Y ; j <= max_Y ; j++){
				//write each value to files
				foutGrid.write(Integer.toString(i)+","+Integer.toString(j)+"\n");
			}
		}
		foutGrid.close(); fin_grid.close();
		
		//start file read from local
		/*FileInputStream f = new FileInputStream(input + "/part-r-00000");
		Scanner s = new Scanner(f);
		
		s.nextLine();
		for (int i = 0; s.hasNext() ; i++) {
			StringTokenizer inputMinMax = new StringTokenizer(s.nextLine(), "\t");
			
			inputMinMax.nextToken();
			if (i==0) min_X = (int) Math.floor(Double.parseDouble(inputMinMax.nextToken()));
			else if (i==1) max_X = (int) Math.ceil(Double.parseDouble(inputMinMax.nextToken()));
			else if (i==2) min_Y = (int) Math.floor(Double.parseDouble(inputMinMax.nextToken()));
			else if (i==3) max_Y = (int) Math.ceil(Double.parseDouble(inputMinMax.nextToken()));
		}

		try { 
			File file = new File(output + "/writegrid_output");
			
			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}
 
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			
			for (int i=min_X ; i <= max_X ; i++){
				for (int j=min_Y ; j <= max_Y ; j++){
					//write each value to files
					bw.write(Integer.toString(i)+","+Integer.toString(j)+"\n");
				}
			}
			
			bw.close();
 
			System.out.println("Write Grid point Done");
 
		} catch (IOException e) {
			e.printStackTrace();
		}*/
	}
}
