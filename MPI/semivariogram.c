#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <mpi.h>
#include <malloc.h>

#include "gridding.h"

#define FOR_DEBUG_PRINT
#define FOR_CONFIRM_PRINT 


void FindSemivar(Distnode* Head_distfromGrid, int nrbins, double* sum_squrZ, double maxDistance, double* distDelta, double** ptopDist){

	int i,j,k;
	int idx_distBins;
	int *idxCnt;
	double squrZ, distance, delta;
	Distnode *ptr1, *ptr2, *toFree;
		
	idxCnt = (int*)malloc(sizeof(int)*(nrbins+2));
	delta = maxDistance / nrbins; //ditance amongs h(x)
	//printf("max value is %lf and delta value is %lf", maxDistance, delta);
	
	for(i=0;i<nrbins+2;i++){
		sum_squrZ[i]=0;
		idxCnt[i]=0;
		distDelta[i]=0;
	}
	
	ptr1=Head_distfromGrid;
	i=0; j=0;
	while(ptr1) {
		ptr2 = ptr1->next;
		j=i+1;
		while(ptr2) {
			
			distance = sqrt(pow(ptr1->coord->x - ptr2->coord->x, 2)+pow(ptr1->coord->y - ptr2->coord->y, 2));
			ptopDist[i][j] = distance;
			//printf("distance: %lf i : %d j : %d\n",ptopDist[i][j],i,j);
			if(distance < maxDistance) {
				idx_distBins = (int)floor(distance/delta);
				idxCnt[idx_distBins] += 1;
				squrZ = pow(ptr1->coord->z - ptr2->coord->z,2);
				
				sum_squrZ[idx_distBins] += squrZ;
			}
			
			ptr2 = ptr2->next;
			j++;
		}
		ptr1 = ptr1->next;
		i++;
	}

	/*for(i=0;i<nrbins;i++){
		FOR_DEBUG_PRINT("sum_squrZ[%d]: %f\n",i,sum_squrZ[i]);
		FOR_DEBUG_PRINT("idxCnt[%d] : %d\n",i,idxCnt[i]);
	}*/
	distDelta[0] = delta/2;
	for(i=0,j=1;i<nrbins;i++,j++){
		if(sum_squrZ[i]!=0){
			//printf("sum_SqurZ[i]=%lf\n", sum_squrZ[i]);
			if(idxCnt[i]==0) {
				printf("[Error] idx is 0 at i\n", i);
			} else {
				sum_squrZ[i] = sum_squrZ[i] / (2*idxCnt[i]);
			}
		}
		//FOR_DEBUG_PRINT("sum_squrZ[%d] : %lf\n", i, sum_squrZ[i]);
		//FOR_DEBUG_PRINT("distDelta[%d] : %lf\n", i, distDelta[i]);
	}
	for(i=1;i<nrbins;i++) {
		distDelta[i] = distDelta[i-1] + delta;
		//distDelta[i] += delta/2;
	}
	free(idxCnt);
}
