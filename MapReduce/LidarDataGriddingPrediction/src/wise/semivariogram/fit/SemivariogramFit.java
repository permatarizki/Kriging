package wise.semivariogram.fit;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.StringTokenizer;


public class SemivariogramFit {
	//input from Semivariogram2D.java
		private double[] semivar;
		private double[] distance;
		private double[] num;
		private int nrbins;
		
		private int nrSubbins = 100;
		private double semibin;
		private double lagbin;
		private double maxVar;
		private double maxDis;
		
		//private double min Least Squared;
		double [][] minLS = new double[nrSubbins][nrSubbins];
		double [][] a0 = new double[nrSubbins][nrSubbins];
		double [][] c0 = new double[nrSubbins][nrSubbins];
		
		//b0[0] = range
		//b0[1] = sill
		//b0[2] = nugget
		double[] b0 = new double[3];
		
		SemivariogramFit (double[] semivar, double[] distance, double[] num, int nrbins){
			this.semivar = semivar;
			this.distance = distance;
			this.nrbins = nrbins;
			this.num = num;
			
			//b0[0] = initial range
			//b0[1] = initial sill
			//b0[2] = initial nugget	
			maxDis = findMax(distance, nrbins);
			b0 [0] = maxDis * 2 / 3;
			maxVar = findMax(semivar, nrbins);
			b0 [1] = maxVar;
			//b0 [2] = semivar[0];
			
			lagbin = b0[0]/nrSubbins;
			semibin = b0[1]/nrSubbins;
		}
		
		public void run(){
			
			for (int i=0 ; i<nrSubbins ; i++){
				for (int j=0 ; j<nrSubbins ; j++){
					b0[0] -= lagbin;
					minLS[i][j] = leastsquaredSumFunc(semivar, b0, distance, nrbins);
					a0[i][j]= b0[0];
					c0[i][j]= b0[1];
				}
				b0 [0] = maxDis * 2 / 3;
				b0 [1] -= semibin;
			}
			
			double min=9999999;
			double a=0;
			double c=0;
			
			for (int i=0 ; i<nrSubbins ; i++){
				for (int j=0 ; j<nrSubbins ; j++){
					if (i==0 && j==0){
						min = minLS[i][j];
					}else{
						if (min > minLS[i][j]){
							min = minLS[i][j];
							a = a0[i][j];
							c = c0[i][j];
						}
					}
				}
			}
			
			System.out.println("a = " +a);
			System.out.println("c = " +c);
		}
		
		
		private double sphericalModel (double[] b0, double h){
			return b0[1]*Math.pow(  (3*h/(2*b0[0])) - 1/2*(h/b0[0]) , 3);
		}
		
		private double weight ( ){
			return 0.0;
		}
		
		private double leastsquaredSumFunc (double[] semivar, double[] b0, double[] h, int nrbins){
			double sum=0.0;
			for (int i=0 ; i<nrbins ; i++){
				if (h[i] < b0[0]){
					sum+=Math.pow(sphericalModel(b0, h[i])-semivar[i], 2);//*weight(b0);
				}else{
					sum+=Math.pow(b0[1]-semivar[i], 2);//*weight(b0);
				}
			}
			return sum;
		}
		
		private double findMax (double[] data, int nrbins){
			double max=0.0;
			for (int i=0 ; i<nrbins ; i++){
				if (data[i] > max){
					max = data[i];
				}
			}
			return max;
		}

		public static void main(String[] args) throws FileNotFoundException{
			
			double [] d;
			double [] semivar;
			double [] num;
			int cntLine=0;
			
			String pathIntermediate = "/home/seop/workspace/VariogramMR/intermediate";
			String pathInput = "/home/seop/workspace/VariogramMR/output";
			
			FileInputStream fline = new FileInputStream(pathIntermediate + "/variogram_test_nrbins.txt");
			Scanner sline = new Scanner(fline);
			
			for (int i = 0; sline.hasNext() ; i++) {
				//StringTokenizer input = new StringTokenizer(sline.nextLine(), " ");
				cntLine = Integer.parseInt(sline.next());
			}
			
			d=new double [cntLine];
			semivar=new double [cntLine];
			num=new double [cntLine];
			
			FileInputStream fin = new FileInputStream(pathInput + "/part-r-00000");
			Scanner s = new Scanner(fin);
			
			for (int i = 0; s.hasNext() && i<cntLine ; i++) {
				StringTokenizer input = new StringTokenizer(s.nextLine(), "\t");
				
				System.out.println(input.nextToken());
				semivar[i]=Double.parseDouble(input.nextToken());
				d[i]=Double.parseDouble(input.nextToken());
				num[i]=Double.parseDouble(input.nextToken());
			}
			 
			/////////////////////////////////
			//semivariogram fit start////
			/////////////////////////////////
			SemivariogramFit sf = new SemivariogramFit(semivar,d,num,cntLine);
			sf.run();
		}
}
