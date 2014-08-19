#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <malloc.h>

#include "Semivariogram2DFit.h"
//#define  DEBUG_SEMIVARIOGRAM_LEVEL1
//#define  DEBUG_SEMIVARIOGRAM_LEVEL2

double findMax(double *data, int nrbins){
	double max = 0.0;
	int i=0;
	
	for(i=0;i<nrbins;i++){
		if(data[i]>max)
			max = data[i];
	}
	return max;
}
double sphericalModel (double* b0, double h){
	return b0[1]*pow( (3*h/(2*b0[0])) - 1/2*(h/b0[0]) , 3);
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
			sum+=pow(sphericalModel(b0, h[i])-semivar[i], 2);
			//printf("index < %lf\n",sum);
		}else{
			sum+=pow(b0[1]-semivar[i], 2);
			//printf("index > %lf\n",sum);
		}
		
	}
	DEBUG_PRINT2("least squared sum %lf\n",sum);
	return sum;
}


void fitSemivariogram(double *sum_SqurZ, double *distance, int nrbins){
	// Semivariogram fit
	//
	//minLS[0] = least squared value
	//minLS[1] = expect range
	//minLS[2] = expect sill	
	double maxVario, minLs[3] = {9999999,0,0}, tmpLS[3]; // For fit
	double lagbin, semibin;
	int i,j,k;
	double b0[3] = {0};
	FILE *fout;

	maxVario = findMax(sum_SqurZ, nrbins);
	
	b0[0] = distance[nrbins-1] * 2 /3;
	b0[1] = maxVario;
	lagbin = b0[0] / nrbins;
	semibin = b0[1]/ nrbins;
	
	for (i=0 ; i<nrbins ; i++){
		for (j=0 ; j<nrbins ; j++){
			b0[0] -= lagbin;
			tmpLS[0] = leastsquaredSumFunc(sum_SqurZ, b0, distance, nrbins);
			tmpLS[1]= b0[0];
			tmpLS[2]= b0[1];
			DEBUG_PRINT2("minLs[0] %lf, minLs[1] %lf minLs[2] %lf\n", tmpLS[0], tmpLS[1], tmpLS[2]);
			if(minLs[0] > tmpLS[0]) {
				minLs[0] = tmpLS[0];
				minLs[1]= b0[0];
				minLs[2]= b0[1];
			}
			
		}
		b0 [0] = distance[nrbins-1] * 2 / 3;
		b0 [1] -= semibin;
	}

	//write range and sill value to file
	fout = fopen ("SemivariogramModelParameter.txt", "w");
	if (fout==NULL){
		fprintf(stderr,"[Semivariogram2DFit.c] cannot open parameter file\n");
		exit(EXIT_FAILURE);
	}
	printf("least squared value %lf, range %lf sill %lf\n", minLs[0], minLs[1], minLs[2]);
	fprintf(fout, "%lf %lf", minLs[1], minLs[2] );
	fclose(fout);

	return;
}
