#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <malloc.h>
#include <string.h>

#include "gridding.h"

#define SEND_DETA 10
#define CAL_WGT 20
#define FOR_DEBUG_PRINT 
#define FOR_CONFIRM_PRINT

void Euclidean();
//double sphericalModel (double, double, double);

void File_out_countZero (int rankId, double coord_gridX, double coord_gridY, FILE* fPrediction) {
	//FILE *fPrediction=NULL;
	//char path[70]="/nfs/code/ksy/PredictionPerGrid";
	//char fileType[] = ".txt";
	
	//strcat(path,itoa(rankId+1,10));
	//strcat(path,fileType);
	//fPrediction = fopen(path,"a");
	//fprintf (fPrediction, "points\n");
	fprintf (fPrediction, "[%2.2lf][%2.2lf], 0, 0, 0\n",coord_gridX, coord_gridY);
	//fclose(fPrediction);
}
//void Prediction(Distnode* Head_distfromGrid, int pCnt, double range, double sill, int rankId, double** distance, double coord_gridX, double coord_gridY, int filePointX, int filePointY, double** buffer, int buf_num){
void Prediction(Distnode* Head_distfromGrid, int pCnt, double range, double sill, int rankId, double** distance, double coord_gridX, double coord_gridY, FILE* fPrediction){
//void Prediction(Distnode* Head_distfromGrid, int pCnt, double range, double sill, int rankId, double** distance, double coord_gridX, double coord_gridY){
	int i,j,k;
	int N;
	double **gamma, **varioVal; //distance : Use Euclidean, gamma : Applied gamma function;
	//double  *varVal, *temp, *weight, *subz; 
	double detm=0.0, detA, *wgt;
	int count_row=0,wgt_idx;
	double subpredict=0, predict=0;
	//FILE *fPrediction=NULL;
	char path[70]="/nfs/code/ksy/LU/PredictionPerGrid";
	char fileType[] = ".txt";
	
	FOR_CONFIRM_PRINT("start Prediction\n");
	FOR_CONFIRM_PRINT("number of point %d\n", pCnt+1);
	Distnode* ptr1 = NULL;
	N = pCnt;
	

	ptr1 = Head_distfromGrid;
	Euclidean(ptr1,distance,N, coord_gridX, coord_gridY);

	
	double *a;
	double **rptr;
	a = malloc(N * N * sizeof(double));
	rptr = malloc(N * sizeof(double *));
	for (k = 0; k < N; k++)
	{
		rptr[k] = a + (k * N);
	}
	double *b = (double *)malloc(sizeof(double)*(N));
	int *pivot = (int*)malloc (sizeof(int)*N);
	double *sol = (double *)malloc(sizeof(double)*(N));

	//printf ("before cal gamma\n");
	//----------- Applied Gamma Function for Ordinary Kriging
	for(i=0;i<N;i++){
		for(j=i;j<N;j++){
			if(j==N-1) {
				rptr[i][j] = -1.0;

				if (distance [i][j] < range){
					b[i] = sill*((3*distance[i][j]/(2*range)) - 1/2*(distance[i][j]/range))*((3*distance[i][j]/(2*range)) - 1/2*(distance[i][j]/range))*((3*distance[i][j]/(2*range)) - 1/2*(distance[i][j]/range));
					//b[i] = sphericalModel(range, sill, distance[i][j]);
				}
				else{
					b[i] = sill;
				}
			}else{
				if (distance [i][j] < range){
					rptr[i][j] = sill*((3*distance[i][j]/(2*range)) - 1/2*(distance[i][j]/range))*((3*distance[i][j]/(2*range)) - 1/2*(distance[i][j]/range))*((3*distance[i][j]/(2*range)) - 1/2*(distance[i][j]/range));
					//rptr[i][j] = sphericalModel(range, sill, distance[i][j]);
				}
				else{
					rptr[i][j] = sill;
				}
				rptr[j][i] = rptr[i][j];
				rptr[N-1][j]=1.0;
			}
		}
	} 
	rptr[N-1][N-1] = 0.0;
	b[N-1] = 1.0;
	
	int err = Doolittle_LU_Decomposition_with_Pivoting(a, pivot,  N); 
	//fprintf (fPrediction,"%d\t", err);
	//printf ("after decom\n");
	if (err < 0) {} //printf(" Matrix A is singular\n");                     
	else {                                                               
		err = Doolittle_LU_with_Pivoting_Solve(a, b, pivot, sol, N);
		//fprintf (fPrediction,"%d\n", err);		
	}
	//fprintf (fPrediction,"\n======================\n");
	
	//strcat(path,itoa(rankId+1,10));
	//strcat(path,fileType);
	//fPrediction = fopen(path,"a");
	
	double weightsum=0.0;
	predict = 0.0;
	//calculate predict value in grid or radius or nearest points 
	ptr1 = Head_distfromGrid;
	for (i=0 ; i<N-1 ; i++){
		predict += ptr1->coord->z * sol[i];
		weightsum += sol[i];
		ptr1=ptr1->next;
	}
	
	free(a);
    	free(rptr);
	free(b);
    	free (pivot);
	free(sol);
	//printf ("grid point [%2.2lf][%2.2lf] , predict value : %lf , nrPoints in radius : %d\n",ptr1->coord->x, ptr1->coord->y, predict, N);
	//buffer[buf_num][0] = filePointX;
	//buffer[buf_num][1] = filePointY;
	//buffer[buf_num][2] = predict;
	/*ptr1 = Head_distfromGrid;
	fprintf (fPrediction,"points\n");
	while(ptr1!=NULL){
		fprintf (fPrediction, "%lf\t%lf\n", ptr1->coord->x,  ptr1->coord->y);
		ptr1 = ptr1->next;
	}*/
	FOR_CONFIRM_PRINT("start file in\n");
	//fprintf (fPrediction, "[%d][%d] , %lf , %d, %lf\n",filePointX, filePointY, predict, N-1, weightsum);

	//fprintf (fPrediction, "[%2.2lf][%2.2lf] , %lf , %d, %lf\n",filePointX, filePointY, predict, N-1, weightsum);
	fprintf (fPrediction, "%lf %lf %lf\n",coord_gridX, coord_gridY, predict);
	
}
void Euclidean(Distnode* ptr1, double** ecd, int n, double coord_gridX, double coord_gridY) {

	Distnode *ptr2;
	int i=0, j=0, N;
	double sumPow;
	N = n;

	//if(rankId==1)  {
	while(ptr1) {
		ecd[i][N-1] = sqrt(pow(ptr1->coord->x - coord_gridX,2) + pow(ptr1->coord->y - coord_gridY,2));
		ptr1 = ptr1->next;
		i++;
	}
	//printf("end ecu\n");
	//}
}