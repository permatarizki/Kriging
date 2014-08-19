#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <float.h>


#include "semivariogram2D.h"
#include "gridding.h"

double leastsquaredSumFunc();

double findMax(double *data, int nrbins){
	double max = 0.0;
	int i=0;
	
	for(i=0;i<nrbins;i++){
		if(data[i]>max)
			max = data[i];
	}
	return max;
}


void calculateSemivariogram(struct Distnode *Head_distfromGrid, double *sum_SqurZ, int nrbins, double** ptop_distance, double *in_semi_time, double *range, double *sill, double maxDistance, double delta, int de_i, int de_j, int numOfpoints ){
	
	//////////////////////////////////////////////////////////////////////////////////////
	// START TO CALCULATE EXPERIMENTAL SEMIVARIOGRAM
	//////////////////////////////////////////////////////////////////////////////////////

	//printf("Calculate Semivariogram ...\n");

	clock_t semi_tick;
	
	//Read input file and put in into Array
	double atof(const char* str);
	char line[100];
	char lidarxyz[30];
	int counter_line=0;


	double* distance = (double*)malloc(sizeof(double)*(nrbins+2));
	double *occurance_idx_distBins = (double*)malloc(sizeof(double)*(nrbins+2));
	
	int k;
	for (k=0 ; k< nrbins+2; k++){
		occurance_idx_distBins[k] = 0;
		sum_SqurZ[k] = 0;
	}

	struct Distnode *ptr1, *ptr2, *to_be_free;
	ptr1 = Head_distfromGrid;

	//printf ("before cal dist\n");
	//semi_tick = clock();
	int i=0, j=0;
	while(ptr1){
		ptr2 = ptr1->next;
		j=i+1;
		while(ptr2){
			//calculate distance between ptr1 & ptr2
			ptop_distance[i][j] = sqrt(pow( (ptr1->coord->X-ptr2->coord->X) ,2) + pow( (ptr1->coord->Y-ptr2->coord->Y),2));
			//printf ("distance: %f i : %d j : %d\n", ptop_distance[i][j], i ,j );
			//index to distBins if distance is less than maxDistance
			if(ptop_distance[i][j]<maxDistance){

				int idx_distbins = (int)floor (ptop_distance[i][j]/delta);
				//printf("range distbins %f - ",distBins[idx_distbins]);
				occurance_idx_distBins[idx_distbins] =occurance_idx_distBins[idx_distbins]+1;
				DEBUG_PRINT1 ("occ : %d\n", occurance_idx_distBins[idx_distbins]);
				//printf("%f \n",distBins[idx_distbins+1]);

				//calculate SqurZ
				double squrl = pow(ptr1->coord->Z-ptr2->coord->Z,2);
				sum_SqurZ[idx_distbins] = sum_SqurZ[idx_distbins]+squrl;
			}
			ptr2= ptr2->next;
			j++;
		}
/*		to_be_free = ptr1;
		ptr1=ptr1->next;
		free(to_be_free);*/
		ptr1=ptr1->next;
		i++;
	}
	//(*in_semi_time) += (double) ( clock() - semi_tick) / CLOCKS_PER_SEC;
	//check sum of total squrl for each idx_distbins which is has equal value
	// for(k=0;k<nrbins;k++){
	// 	printf("sum_SqurZ[%d]: %f\n",k,sum_SqurZ[k]);
	// 	printf("occurance_idx_distBins[%d] : %lf\n",k,occurance_idx_distBins[k]);
	// }


	// total sum SqurZ divide by 2*occurance
	//printf ("semivairogram :\n");
	for(k=0;k<nrbins;k++){
		if(sum_SqurZ[k] != 0){
			if (occurance_idx_distBins[k] == 0) {
				printf ("[err] occ-idx is 0 at k : %d\n", k);
				sum_SqurZ[k] = 0;
			}else{
				sum_SqurZ[k] = sum_SqurZ[k]/(2*occurance_idx_distBins[k]);
			}
		}

		//printf ("%d : %1f\n",k, sum_SqurZ[k]);
	}
	// Final variable is sum_SqurZ
	distance[0]=delta/2;
	for (k=1;k<nrbins;k++){
		distance[k] = distance[k-1] + delta;
	}	
	//printf("Finished Calculate Semivariogram\n");

	// for(k=0;k<nrbins;k++){
	//  	printf("sum_SqurZ[%d]: %f distance[%d] : %f\n",k,sum_SqurZ[k],k,distance[k]);
	//  	//printf("occurance_idx_distBins[%d] : %lf\n",k,occurance_idx_distBins[k]);
	// }

	/*if(de_i<71 && de_j <167) {
		FILE* f = fopen("sumsqureZ.txt","a");
		
		for(i=0;i<nrbins;i++) {
			fprintf(f,"%lf %lf\n", sum_SqurZ[i], distance[i]);
		}
		fclose(f);
	}*/
	
	free(occurance_idx_distBins);
	/*minXX=0.0;
	minYY=0.0;
	maxXX=0.0;
	maxYY=0.0;*/

	//////////////////////////////////////////////////////////////////////////////////////
	// START FITTING SEMIVARIOGRAM //
	//////////////////////////////////////////////////////////////////////////////////////

	double maxVario, minLs[3] = {DBL_MAX,0,0}, tmpLS[3]; // For fit
	double lagbin, semibin;
	//int i,j,k;
	double b0[3] = {0};
	//FILE *fout;

	maxVario = findMax(sum_SqurZ, nrbins);
	int bins = 10;
	//b0[0] = range
	//b0[1] = sill
	b0[0] = distance[nrbins-1] * 2 /3;
	b0[1] = maxVario;
	lagbin = b0[0] / bins;
	semibin = b0[1]/ bins;

	
	double ls_tick = clock();
	for (i=0 ; i<bins ; i++){
		for (j=0 ; j<bins ; j++){
			b0[0] -= lagbin;
			tmpLS[0] = leastsquaredSumFunc(sum_SqurZ, b0, distance, nrbins);
			tmpLS[1]= b0[0];
			tmpLS[2]= b0[1];
			if(minLs[0] > tmpLS[0]) {
				minLs[0] = tmpLS[0];
				minLs[1] = b0[0];
				minLs[2] = b0[1];
			}
			
		}
		b0 [0] = distance[nrbins-1] * 2 / 3;
		b0 [1] -= semibin;
	}
	(*in_semi_time) += (double) ( clock() - ls_tick) / CLOCKS_PER_SEC;

	if (minLs[1] < 0.0001){
		minLs[1] = distance[0];
	}

	//write range and sill value to file
	// fout = fopen ("SemivariogramModelParameter.txt", "w");
	// if (fout==NULL){
	// 	fprintf(stderr,"[Semivariogram2DFit.c] cannot open parameter file\n");
	// 	exit(EXIT_FAILURE);
	// }
	//printf("least squared value %lf, range %lf sill %lf\n", minLs[0], minLs[1], minLs[2]);
	//fprintf(fout, "%lf %lf", minLs[1], minLs[2] );
	//fclose(fout);

	*range = minLs[1];
	*sill = minLs[2];
	
	free(distance);

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
			sum += pow( ( b0[1]*(3*h[i]/(2*b0[0])- 1/2*(h[i]/b0[0]))*(3*h[i]/(2*b0[0])- 1/2*(h[i]/b0[0]))*(3*h[i]/(2*b0[0])- 1/2*(h[i]/b0[0]))-semivar[i]) ,2);
			//sum+=pow(sphericalModel(b0[0], b0[1], h[i])-semivar[i], 2);
		}else{
			sum+=pow(b0[1]-semivar[i], 2);
		}
		
	}
	return sum;
}
