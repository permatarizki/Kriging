package wise.kriging;

public class SemivariogramFit2D {

	//input from Semivariogram2D.java
	private double[] semivar;
	private double[] distance;
	private int nrbins;
	
	private double semibin;
	private double lagbin;
	
	//private double min Least Squared;
	private double [] minLS;
	private double [] tmpLS;
	
	int bins;
	
	//initail value : b[0] = range, b[1] = sill, b[2] = nugget
	private double[] b0 = new double[3];
	
	public SemivariogramFit2D (double[] semivar, double[] distance, int nrbins){
		this.semivar = semivar;
		this.distance = distance;
		this.nrbins = nrbins;

		b0 [0] = distance[nrbins-1] * 2 / 3;
		b0 [1] = findMaxSemivar(semivar, nrbins);
		
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
				tmpLS[0] = leastsquaredSumFunc(semivar, b0, distance, nrbins);
				tmpLS[1]= b0[0];
				tmpLS[2]= b0[1];
				
				if(minLS[0] > tmpLS[0]) {
					minLS[0] = tmpLS[0];
					minLS[1] = b0[0];
					minLS[2] = b0[1];
				}
			}
			b0 [0] = distance[nrbins-1] * 2 / 3;
			b0 [1] -= semibin;
		}

		if (minLS[1] < 0.0001){
			minLS[1] = distance[0];
		}
	}
	
	public void run(){
		
		
		
		/*System.out.println("Semivariogram Fitting value : ");
		System.out.println("Range : "+minLS[1]+" Sill : "+minLS[2]);*/
	}
	
	public double getRange(){
		return minLS[1];
	}
	public double getSill(){
		return minLS[2];
	}
	
	private double sphericalModel (double[] b0, double h){
		return b0[1]*Math.pow(  (3*h/(2*b0[0])) - 1/2*(h/b0[0]) , 3);
	}
	
	private double leastsquaredSumFunc (double[] semivar, double[] b0, double[] h, int nrbins){
		double sum=0.0;
		for (int i=0 ; i<nrbins ; i++){
			if (h[i] < b0[0]){
				sum+=Math.pow(sphericalModel(b0, h[i])-semivar[i], 2);
			}else{
				sum+=Math.pow(b0[1]-semivar[i], 2);
			}
		}
		return sum;
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
