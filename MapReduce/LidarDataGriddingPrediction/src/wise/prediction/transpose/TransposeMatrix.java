package wise.prediction.transpose;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;

public class TransposeMatrix {
	private String det_0 = "1.0";
	private String[][] inputMatrix;
	
	public void runforSimpleKriging(Path localPath, int rows, int cols, Path matrix_tmp)
			throws IOException {
		
		FileInputStream fin = new FileInputStream(localPath.toString());
		Scanner s = new Scanner(fin);
		inputMatrix = new String[rows][cols];

		
		/*//read output and make matrix(inputdata_testDET)
		for (int i = 0; s.hasNext() && i < rows; i++) { // rows
			StringTokenizer output = new StringTokenizer(s.nextLine(), ",");
			for (int j = 0 ; output.hasMoreTokens() && j < cols ; j++){
				inputMatrix[i][j] = output.nextToken();
				System.out.println("token: " + inputMatrix[i][j]);
			}
		}*/
		
		
		for (int i = 0; s.hasNext() && i < rows; i++) { // rows
			StringTokenizer output = new StringTokenizer(s.nextLine(), " ");
			output.nextToken();
			if (i==0){
				for (int j = 0 ; output.hasMoreTokens() && j < rows ; j++){
					inputMatrix[j][cols-1]=output.nextToken();
					System.out.println("token: " + inputMatrix[j][cols-1]);
				}
				inputMatrix[rows-1][rows-1]="0";
			}else{
				inputMatrix[i-1][i-1] = "0.0";
				for (int j = i; output.hasMoreTokens() && j < rows; j++) {
					String tmp = output.nextToken();
			
					inputMatrix[i-1][j] = tmp;
					inputMatrix[j][i-1] = tmp;
					System.out.println("token: " + inputMatrix[i-1][j]);				
				}
			}
		}
		s.close();
		fin.close();
		
		FileOutputStream fout = new FileOutputStream(matrix_tmp.toString());

		// write file about det_0
		fout.write("0, ".getBytes());
		for (int i = 0; i < rows; i++) {
			for (int j = 0; j < rows; j++) {
				String tmp = inputMatrix[i][j] + ", ";
				System.out.println(tmp);
				fout.write(tmp.getBytes());
			}
		}
		fout.write("\n".getBytes());

		// write file about det with z
		boolean isflag = true;
		for (int z = 0; z < rows; z++) {
			fout.write((Integer.toString(z + 1) + ", ").getBytes());
			for (int j = 0; j < rows; j++) {
				if (j == z) {
					isflag = true;
				}
				for (int i = 0; i < rows; i++) {
					String tmp = "";
					if (isflag == true) {
						tmp = inputMatrix[i][cols-1] + ", ";

					} else {
						tmp = inputMatrix[i][j] + ", ";
					}
					System.out.println("transposeMatrix : " + tmp);
					fout.write(tmp.getBytes());
				}
				isflag = false;
			}
			fout.write("\n".getBytes());
		}
		fout.close();
	}
	
	public void runforOrdinaryKriging(Path localPath, int rows, int cols, Path matrix_tmp)
		throws IOException {
		FileInputStream fin = new FileInputStream(localPath.toString());
		Scanner s = new Scanner(fin);
		inputMatrix = new String[rows+1][cols+1];

		//read output and make matrix
		for (int i = 0; s.hasNext() && i < rows; i++) { // rows
			StringTokenizer output = new StringTokenizer(s.nextLine(), " ");
			output.nextToken();
			if (i==0){
				for (int j = 0 ; output.hasMoreTokens() && j<rows ; j++){
					inputMatrix[j][cols]=output.nextToken();
					System.out.println("token: " + inputMatrix[j][cols]);
				}
				inputMatrix[rows][cols]="1";
			}else{
				inputMatrix[i-1][i-1] = "0.0";
				for (int j = i; output.hasMoreTokens() && j < rows; j++) {
					String tmp = output.nextToken();
			
					inputMatrix[i-1][j] = tmp;
					inputMatrix[j][i-1] = tmp;
					System.out.println("token: " + inputMatrix[i-1][j]);	
				}		
				inputMatrix[i-1][cols-1] = "-1";
			}
		}
		
		inputMatrix[rows-1][cols-1] = "-1";
		inputMatrix[rows-1][cols-2] = "0";
		
		for (int j=0 ; j<rows ; j++ ){
			inputMatrix[rows][j] = "1";
		}
		inputMatrix[rows][cols-1] = "0";
		s.close();
		fin.close();
		
		for (int i=0 ; i<rows+1 ; i++){
			for (int j=0 ; j<cols+1 ; j++){
				System.out.print(inputMatrix[i][j]+", ");
			}
			System.out.println();
		}
		
		FileOutputStream fout = new FileOutputStream(matrix_tmp.toString());

		// write file about det_0
		fout.write("0, ".getBytes());
		for (int i = 0; i < rows+1; i++) {
			for (int j = 0; j < rows+1; j++) {
				String tmp = inputMatrix[i][j] + ", ";
				System.out.println(tmp);
				fout.write(tmp.getBytes());
			}
		}
		fout.write("\n".getBytes());

		// write file about det with z
		boolean isflag = true;
		for (int z = 0; z < rows+1; z++) {
			fout.write((Integer.toString(z + 1) + ", ").getBytes());
			for (int j = 0; j < rows+1; j++) {
				if (j == z) {
					isflag = true;
				}
				for (int i = 0; i < rows+1; i++) {
					String tmp = "";
					if (isflag == true) {
						tmp = inputMatrix[i][cols] + ", ";

					} else {
						tmp = inputMatrix[i][j] + ", ";
					}
					System.out.println("transposeMatrix : " + tmp);
					fout.write(tmp.getBytes());
				}
				isflag = false;
			}
			fout.write("\n".getBytes());
		}
		fout.close();
	}
	
	public double calDet0forSimple (int rows){
		return new CalDet().determinant(inputMatrix, rows);
	}
	public double calDet0forOrdinary (int rows){
		return new CalDet().determinant(inputMatrix, rows+1);
	}
}
