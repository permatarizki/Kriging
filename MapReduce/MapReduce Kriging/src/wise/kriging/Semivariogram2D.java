package wise.kriging;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;
import java.util.StringTokenizer;


public class Semivariogram2D {
	
	private int rows = 0;
	
//	private double input[][];// 0:X, 1:Y, 2:Z
	private double distance[];
	private double[] occIdxdistBins;
	private double[] sumSqurZ;
	private double delta;
	private double[][] ptopDistance;

	private int nrbins = 0;
	private double maxdist = 0.0;
	
	private double semibin;
	private double lagbin;
	
	//private double min Least Squared;
	private double [] minLS;
	private double [] tmpLS;
	
	int bins;
	
	//initail value : b[0] = range, b[1] = sill, b[2] = nugget
	private double[] b0 = new double[3];
	
	private long semiTime;
	private long fitTime;
	
	public Semivariogram2D(String args[], double input[][], double minX, double minY, double maxX, double maxY) throws IOException{
		
		nrbins = Integer.parseInt(args[0]);
		rows = Integer.parseInt(args[1]);
		
		////////////////////////////////////////////////////
		////////////////////////////////////////////////////
		//start initialize cal semivar
		long startTime = System.currentTimeMillis();
		
		occIdxdistBins = new double[nrbins];
		sumSqurZ = new double[nrbins];
		distance = new double [nrbins];
		ptopDistance = new double[rows+1][rows+1];

		double predist = Math.sqrt( Math.pow(minX-maxX, 2)
				+ Math.pow(minY-maxY, 2));
		maxdist = predist/2;

		delta = maxdist/nrbins;
		
		//System.out.println("maxdist : "+maxdist);
		//run()
		for (int i = 0; i < rows; i++) {
			for (int j = i+1; j < rows; j++) {
				ptopDistance[i][j] = Math.sqrt((input[i][0] - input[j][0])
						* (input[i][0] - input[j][0])
						+ (input[i][1] - input[j][1])
						* (input[i][1] - input[j][1]));
				// System.out.println("d: "+d);
				if (ptopDistance[i][j] < maxdist) {
					
					int idx = (int) Math.floor(ptopDistance[i][j] / delta);
					//System.out.println("idx : "+idx);
					occIdxdistBins[idx] = occIdxdistBins[idx]+1;
					double squrZ = (input[i][2] - input[j][2])
							* (input[i][2] - input[j][2]);
					sumSqurZ[idx] = sumSqurZ[idx]+squrZ;
				}
			}
		}
		
		for (int i = 0; i < nrbins; i++) {
			if (occIdxdistBins[i] == 0.0){
				sumSqurZ[i] = 0.0;
			}else{
				sumSqurZ[i] = sumSqurZ[i]/(2*occIdxdistBins[i]);
			}
		}
		
		distance[0]=delta/2;
		for (int i=1 ; i<nrbins ; i++){
			distance[i] = distance[i-1]+delta;
		}
		
		long endTime = System.currentTimeMillis();
		semiTime = endTime-startTime;
		
		//////////////////////////////////////////////////////////
		//////////////////////////////////////////////////////////
		//end calculate experimental variogram
		//////////////////////////////////////////////////////////
		//////////////////////////////////////////////////////////
		
//		System.out.println("Semivariogram : ");
//		for (int i = 0 ; i<nrbins; i++){
//			System.out.println(sumSqurZ[i]+" "+distance[i]);
//		}
		
		startTime = System.currentTimeMillis();
		
		b0 [0] = distance[nrbins-1] * 2 / 3;
		b0 [1] = findMaxSemivar(sumSqurZ, nrbins);
		
		minLS = new double[3];
		minLS[0] = Double.MAX_VALUE;
		tmpLS = new double[3];
		bins=10;
		lagbin = b0[0] / bins;
		semibin = b0[1]/ bins;
		
/*		for (int i=0 ; i<nrbins ; i++){
			System.out.println("semivar : "+semivar[i]);
		}*/
		for (int i=0 ; i<bins ; i++){
			for (int j=0 ; j<bins ; j++){
				b0[0] -= lagbin;
				//tmpLS[0] = leastsquaredSumFunc(sumSqurZ, b0, distance, nrbins);
				
				for (int k=0 ; k<nrbins ; k++){
					if (distance[k] < b0[0]){
						tmpLS[0]+=( b0[1]*(  ((3*distance[k]/(2*b0[0]))-1/2*(distance[k]/b0[0])) *  ((3*distance[k]/(2*b0[0]))-1/2*(distance[k]/b0[0])) * ((3*distance[k]/(2*b0[0]))-1/2*(distance[k]/b0[0])) ) -sumSqurZ[k] ) 
								* ( b0[1]*(  ((3*distance[k]/(2*b0[0]))-1/2*(distance[k]/b0[0])) *  ((3*distance[k]/(2*b0[0]))-1/2*(distance[k]/b0[0])) * ((3*distance[k]/(2*b0[0]))-1/2*(distance[k]/b0[0])) ) -sumSqurZ[k]);
					}else{
						tmpLS[0]+=(b0[1]-sumSqurZ[k]) * (b0[1]-sumSqurZ[k]);
					}
				}
				
				tmpLS[1]= b0[0];
				tmpLS[2]= b0[1];
				
				if(minLS[0] > tmpLS[0]) {
					minLS[0] = tmpLS[0];
					minLS[1] = b0[0];
					minLS[2] = b0[1];
				}
				tmpLS[0]=0.0;
			}
			b0 [0] = distance[nrbins-1] * 2 / 3;
			b0 [1] -= semibin;
		}

		if (minLS[1] < 0.0001){
			minLS[1] = distance[0];
		}
		
		endTime = System.currentTimeMillis();
		fitTime = endTime-startTime;
	}
	
	public double[][] getptopDist(){
		return ptopDistance;
	}
	public double[] getSemivar(){
		return sumSqurZ;
	}
	public double[] getDistance(){
		return distance;
	}
	
	public double getRange(){
		return minLS[1];
	}
	public double getSill(){
		return minLS[2];
	}
	
	public long getSemiTime(){
		return semiTime;
	}
	public long getFitTime(){
		return fitTime;
	}
	
	
	private double findMaxSemivar (double[] semivar, int nrbins){
		double max=0.0;
		for (int i=0 ; i<nrbins ; i++){
			if (semivar[i] > max){
				max = semivar[i];
			}
		}
		return max;
	}
}

