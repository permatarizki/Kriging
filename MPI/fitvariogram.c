#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <mpi.h>
#include <malloc.h>

#define FOR_DEBUG_PRINT
#define FOR_CONFIRM_PRINT 

#define DBL_MAX 99999

double findMax();
double sphericalModel ();
double leastsquaredSumFunc ();


void FitSemivariogram(double* sum_SqurZ, double* distDelta, int nrbins, double* range, double* sill) {
	//minLS[0] = least squared value
	//minLS[1] = expect range
	//minLS[2] = expect sill	
	double maxVario, tmpLS[3], minLs[3] = {DBL_MAX,0,0}; // For fit
	double lagbin, semibin;
	int i,j,k;
	double b0[3] = {0};
	int bins = 10;
	
	maxVario = findMax(sum_SqurZ, nrbins);
	
	//b0[0] = range
	//b0[1] = sill
	b0[0] = distDelta[nrbins-1] * 2 /3;
	b0[1] = maxVario;
	lagbin = b0[0] / bins;
	semibin = b0[1]/ bins;

	
	for (i=0 ; i<bins ; i++){
		for (j=0 ; j<bins ; j++){
			b0[0] -= lagbin;
			tmpLS[0] = leastsquaredSumFunc(sum_SqurZ, b0, distDelta, nrbins);
			tmpLS[1]= b0[0];
			tmpLS[2]= b0[1];
			if(minLs[0] > tmpLS[0]) {
				minLs[0] = tmpLS[0];
				minLs[1] = b0[0];
				minLs[2] = b0[1];
			}
			
		}
		b0 [0] = distDelta[nrbins-1] * 2 / 3;
		b0 [1] -= semibin;
	}

	if (minLs[1] < 0.0001){
		minLs[1] = distDelta[0];
	}
	
	//for(i=0;i<3;i++){
	//	printf("b - %lf t - %lf min - %lf\n", b0[i], tmpLS[i], minLs[i]);
	//}
	*range = minLs[1];
	*sill = minLs[2];
	return;
}


double findMax(double *data, int nrbins){
	double max = 0.0;
	int i=0;
	
	for(i=0;i<nrbins;i++){
		if(data[i]>max)
			max = data[i];
	}
	return max;
}
double sphericalModel (double range, double sill, double h){
	return sill*pow( (3*h/(2*range)) - 1/2*(h/range) , 3);
}

double leastsquaredSumFunc (double *semivar, double* b0, double* h, int nrbins){
	double sum=0.0;
	int i;
	/*for (i=0 ; i<nrbins ; i++){
		printf("semivar: %lf, distBins: %lf\n", semivar[i], h[i]);
	}
	printf("b: %lf,%lf,  nrbins: %d\n", b0[0], b0[1], nrbins);*/
	for (i=0 ; i<nrbins ; i++){
		if (h[i] < b0[0]){
			sum += pow( b0[1]*((3*h[i]/(2*b0[0])) - 1/2*(h[i]/b0[0]))*((3*h[i]/(2*b0[0])) - 1/2*(h[i]/b0[0]))*((3*h[i]/(2*b0[0])) - 1/2*(h[i]/b0[0]))-semivar[i], 2);
			//sum+=pow(sphericalModel(b0[0], b0[1], h[i])-semivar[i], 2);
			FOR_DEBUG_PRINT("index < %lf\n",sum);
		}else{
			sum+=pow(b0[1]-semivar[i], 2);
			FOR_DEBUG_PRINT("index > %lf\n",sum);
		}
		
	}
	FOR_DEBUG_PRINT("sum %lf\n",sum);
	return sum;
}/**/
