package wise.kriging;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.DecompositionSolver;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;

public class OrdinaryKriging {
	 
	public double run(double [][] points, double range, double sill, double gridX, double gridY, int numPoints, double[][] ptopDistance){
		int N = numPoints+1;
		//double [][] gamma;
		double [][] a;
		double [] b;
		double [][] distance;
		double predict=0.0;
		
		distance = calDist(points, N, gridX, gridY, ptopDistance);
		
		a = new double[N][N];
		b = new double[N];
		
		for(int i=0;i<N;i++){
			for(int j=i;j<N;j++){
				if(j==N-1) {
					a[i][j] = -1.0;
					if (distance [i][j] < range)
						b[i] = sphericalModel(range, sill, distance[i][j]);
					else
						b[i] = sill;
				}
				else{
					if (distance [i][j] < range)
						a[i][j] = sphericalModel(range, sill, distance[i][j]);
					else
						a[i][j] = sill;
					a[j][i] = a[i][j];
					a[N-1][j] = 1.0;
				}
			}
		} 
		a[N-1][N-1] = 0.0;
		b[N-1] = 1.0;
		
		RealMatrix coefficients = new Array2DRowRealMatrix(a,false);
		DecompositionSolver solver = new LUDecomposition(coefficients).getSolver();
		RealVector constants = new ArrayRealVector(b, false);
		RealVector solution=null;
		if (solver.isNonSingular())
			solution = solver.solve(constants);
		
		predict = calPrediction(points, solution, N);
		
		return predict;
	}
	
	private double[][] calDist(double[][] points, int N, double gridX, double gridY, double[][] ptopDistance){
		//double [][] distance = new double[N][N];
		
//		for (int i=0 ; i<N-1 ; i++){
//			for (int j=i+1 ; j<N-1 ; j++){
//				distance[i][j] = Math.sqrt((points[i][0] - points[j][0])
//						* (points[i][0] - points[j][0]) + (points[i][1] - points[j][1])
//						* (points[i][1] - points[j][1]));
//			}
//		}
		
		for (int i=0 ; i<N-1 ; i++){
			ptopDistance[i][N-1] = Math.sqrt((points[i][0]-gridX) * (points[i][0]-gridX) + (points[i][1]-gridY) * (points[i][1]-gridY) );
		}
		
		return ptopDistance; 
	}
	
/*	
	private double[][] calGamma(double[][] distance, int N, double range, double sill){
		double [][] gamma = new double[N][N+1];
		
		
		return gamma;
	}
	
	private double[][] calGausePivot (double[][] a, int n){
		int i,j,k,o,u,q,w=0,c;
		double temp1,temp2,temp3,max,num;
	
		for (i=0 ; i<n ; i++){
			for (j=0 ; j<n ;j++){
				DEBUG_PREDICTION_PRINT1 ("%lf ", a[i][j]);
			}
		}
		DEBUG_PREDICTION_PRINT1("\nWith Pivot \n");
	
		for(i=0;i<n-1;i++) {

	  //pivot
			max=Math.abs(a[i][i]);
			for (q=i;q<n;q++) {
				num = Math.abs(a[q][i]);
				//DEBUG_PREDICTION_PRINT4 ("num : %lf\n", num);
				if( num > max ) {
					//DEBUG_PREDICTION_PRINT4("num>max : num: %lf\n", num);
					max = num;
					w = q;
				}
			}
	  //˜
			//DEBUG_PREDICTION_PRINT4 ("max : %lf, a[i][i] : %lf\n", max, a[i][i]);
			if(Math.abs(a[i][i]) != max) {
				for(c=0;c<n+1;c++) {
					temp3 = a[i][c];
					a[i][c] = a[w][c];
					a[w][c] = temp3;
				}

//				//DEBUG_PREDICTION_PRINT4("Pivoting ì¶œë ¥ \n");
//				for(o=0; o< n; o++) {
//					for(u=0; u< n+1; u++ ) {
//						//DEBUG_PREDICTION_PRINT4("%13.4lf\t", a[o][u]);
//					}
//					//DEBUG_PREDICTION_PRINT4("\n");
//				}
			}

	  //
			for(j=i+1; j<n; j++) {
				temp2 = a[j][i]/a[i][i];
				for(k=0 ; k<n+1; k++) {
					a[j][k] = a[j][k] - (temp2 * a[i][k]);
				}
			}

//			//DEBUG_PREDICTION_PRINT4("ê³¼ì • \n");
//			for(o=0; o< n; o++) {
//				for(u=0; u< n+1; u++ ) {
//					//DEBUG_PREDICTION_PRINT4("%13.4lf\t", a[o][u]);
//				}
//				//DEBUG_PREDICTION_PRINT4("\n");
//			}

		}

	 //˜
		for(i=n-1; i>=0; i--)
		{
			for(k=i+1; k<n; k++) {
				a[i][n]=a[i][n]-a[i][k]*a[k][n];
			}
			a[i][n]=a[i][n]/a[i][i];
		}

		//DEBUG_PREDICTION_PRINT4("\ní•´ ” \n");

		for(i=0; i<n; i++)
			DEBUG_PREDICTION_PRINT4("x%: %13.4lf\t\n", i+1, a[i][n]);
	
		return a;
	}
*/
	private double calPrediction (double[][] points, RealVector sol, int N){
		double predict=0.0;
		//double weightsum=0.0;
		if (sol == null) return -1.0;
		
		for (int i=0; i<N-1 ; i++){
			//weightsum += gamma[i][N];
			predict += points[i][2] * sol.getEntry(i);
		}
		//System.out.println("weight sum : "+weightsum);
		
		return predict;
	}
	
	private double sphericalModel (double range, double sill, double h){
		return sill*Math.pow( (3*h/(2*range)) - 1/2*(h/range) , 3);
	}
}
