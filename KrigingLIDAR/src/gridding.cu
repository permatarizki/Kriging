// example1.cpp : Defines the entry point for the console application.
//

//#include <stdafx.h>

#include <stdio.h>
#include <stdlib.h>
#include <cuda.h>
#include <time.h>
#include <float.h>
#include <math.h>

#include "cuPrintf.h"

void findMinMax (double x, double y);

//Maximum & Minimum variables
float minX=0.0, minY=0.0, maxX=0.0, maxY=0.0;

__device__ int Doolittle_LU_Decomposition_with_Pivoting(double *A, int pivot[], int n)
{
	int i, j, k;
	double *p_k, *p_row, *p_col;
	double max;

	//cuPrintf("nilai n %d\n",n);
	int id=0;
	for(i=0;i<n;i++){
		for(j=0;j<n;j++){
			//cuPrintf("%f; ",*(A+id));
			id++;
		}
		//cuPrintf("\n");
	}

	//         For each row and column, k = 0, ..., n-1,

	for (k = 0, p_k = A; k < n; p_k += n, k++) {

		//            find the pivot row

		pivot[k] = k;
		//cuPrintf("*(p_k+k) = %f\n",*(p_k+k));
		max = fabs( *(p_k + k) );
		for (j = k + 1, p_row = p_k + n; j < n; j++, p_row += n) {
			if ( max < fabs(*(p_row + k) )){
				max = fabs(*(p_row + k));//*(p_row + k) =  *A
				pivot[k] = j;
				p_col = p_row;
			}
		}

		//     and if the pivot row differs from the current row, then
		//     interchange the two rows.

		if (pivot[k] != k)
			for (j = 0; j < n; j++) {
				max = *(p_k + j);
				*(p_k + j) = *(p_col + j);
				*(p_col + j) = max;
			}

		//                and if the matrix is singular, return error


		if ( *(p_k + k) == 0.0 ) return -1;

		//      otherwise find the lower triangular matrix elements for column k.

		for (i = k+1, p_row = p_k + n; i < n; p_row += n, i++) {
			*(p_row + k) /= *(p_k + k);
		}

		//            update remaining matrix

		for (i = k+1, p_row = p_k + n; i < n; p_row += n, i++)
			for (j = k+1; j < n; j++)
				*(p_row + j) -= *(p_row + k) * *(p_k + j);

	}

	return 0;
}


__device__ int Doolittle_LU_with_Pivoting_Solve(double *A, double B[], int pivot[],
		double x[], int n)
{
	int i, k;
	double *p_k;
	double dum;

	//         Solve the linear equation Lx = B for x, where L is a lower
	//         triangular matrix with an implied 1 along the diagonal.

	for (k = 0, p_k = A; k < n; p_k += n, k++) {
		if (pivot[k] != k) {dum = B[k]; B[k] = B[pivot[k]]; B[pivot[k]] = dum; }
		x[k] = B[k];
		for (i = 0; i < k; i++) x[k] -= x[i] * *(p_k + i);
	}

	//         Solve the linear equation Ux = y, where y is the solution
	//         obtained above of Lx = B and U is an upper triangular matrix.

	for (k = n-1, p_k = A + n*(n-1); k >= 0; k--, p_k -= n) {
		if (pivot[k] != k) {dum = B[k]; B[k] = B[pivot[k]]; B[pivot[k]] = dum; }
		for (i = k + 1; i < n; i++) x[k] -= x[i] * *(p_k + i);
		if (*(p_k + k) == 0.0) return -1;
		x[k] /= *(p_k + k);
	}

	return 0;
}

__device__ double sphericalModel (double range, double sill, double h){
	return sill*powf( (3*h/(2*range)) - 1/2*(h/range) , 3);
	//	return 10*(1-exp(-h/3.33));
}

__device__ double leastsquaredSumFunc (double *semivar, double* b0, double* h, int nrbins){
	double sum=0.0;
	int i;
	/*for (i=0 ; i<nrbins ; i++){
		printf("semivar: %lf, distBins: %lf\n", semivar[i], h[i]);
	}
	printf("b: %lf,%lf,  nrbins: %d\n", b0[0], b0[1], nrbins);*/
	for (i=0 ; i<nrbins ; i++){
		if (h[i] < b0[0]){
			sum += powf( ( b0[1]*(3*h[i]/(2*b0[0])- 1/2*(h[i]/b0[0]))*(3*h[i]/(2*b0[0])- 1/2*(h[i]/b0[0]))*(3*h[i]/(2*b0[0])- 1/2*(h[i]/b0[0]))-semivar[i]) ,2);
			//sum+=pow(sphericalModel(b0[0], b0[1], h[i])-semivar[i], 2);
		}else{
			sum+=pow(b0[1]-semivar[i], 2);
		}

	}
	return sum;
}

__global__ void on_core_process(double *dev_debugVar,int GPUIndexStart, double* ptr_xgridpoint, double* ptr_ygridpoint,double* ptrpredictionResult,
		double *x_device, double *y_device, double *z_device, double minimumX, double minimumY, int numInputData, double gridsizes,
		int GPUIndexEnd, int dimGridX_size)
{
	/**
	  Algorithms:
	  1) Specify anchor coordinates.
	  2) We calculate distance from this point.
	  3) This point will be different from each GPU thread
	  4) Closest node are constant based on radius meters (not based on square of grid)

	 */

	/**
	 * GPU THREAD ID
	 *  GRIDX_RANGE+GRIDY_RANGE                                ...
	 *   .
	 *   .
	 *   .
	 *  GRIDX_RANGE+1 GRIDX_RANGE+2                            ...      2*1000
	 *   0             1              2              3         ...       1000
	 */

	int dimBlocksize = dimGridX_size;
	int gridID = blockIdx.x+(blockIdx.y*gridDim.x);
	//retrieve thread ID
	int threadID = ((threadIdx.y)*blockDim.x+(threadIdx.x))+gridID*gridDim.x*gridDim.y;
	int dumpsize = threadID;
	threadID = threadID+GPUIndexStart;

	//we represent dimBlocksize as X axis length
	//double y_node = (double) (1*floor((float)threadID/1000));
	double y_node = (double) (1*floor((float)threadID/dimBlocksize));

	//shift load jobs into several devices
	double x_node = (double) (threadID%dimBlocksize);

	//do each thread process here
	double weightsum;
	int idx_onRange;
	double predict=0;

	if ((threadID<GPUIndexEnd)&&(threadID>=GPUIndexStart)) {
		//Calculate distance from each LIDAR input and save it into index if the distance still on the range
		// in here we assume that we have only maximum 100 closest nodes
		float x_closestNodesIndex[100];
		float y_closestNodesIndex[100];
		float z_closestNodesIndex[100];

		int gridRadius = 3;
		idx_onRange=0;
		for(int i=0;i<numInputData;i++){ //TODO to big numInputData make this program crash
			//save the value if distance is still on range
			if(( abs((x_device[i]-(x_node+minimumX)))<gridRadius)&&(abs((y_device[i]-(y_node+minimumY)))<gridRadius)){
				x_closestNodesIndex[idx_onRange] = (float)x_device[i]; //change this with pointer to enhance performance
				y_closestNodesIndex[idx_onRange] = (float)y_device[i]; //change this with pointer to enhance performance
				z_closestNodesIndex[idx_onRange] = (float)z_device[i];
				if(idx_onRange<100){ //TODO solve this function! why more than 100 is not allowed? stack problem?
					idx_onRange++;
				}
			}
		}

		//find min & max value from closestNodesIndex variable (both X & Y)
		double min_x=999999;
		double max_x=0;
		double min_y=9999999;
		double max_y=0;
		for(int i=0;i<idx_onRange;i++){
			if(x_closestNodesIndex[i]<min_x)
				min_x = x_closestNodesIndex[i];
			if(y_closestNodesIndex[i]<min_y)
				min_y = y_closestNodesIndex[i];
			if(y_closestNodesIndex[i]>max_y)
				max_y = y_closestNodesIndex[i];
			if(x_closestNodesIndex[i]>max_x)
				max_x = x_closestNodesIndex[i];
			i++;
		}

		//Calculate Semivariogram
		int nrbins = 50; //this is a parameter
		int rows = idx_onRange;
		double occIdxdistBins[50]; //parameter : nrbins
		memset(occIdxdistBins,0,sizeof(occIdxdistBins));
		double sumSqurZ[50]; //parameter : nrbins
		memset(sumSqurZ,0,sizeof(sumSqurZ));
		double distance[50]; //parameter : nrbins
		memset(distance,0,sizeof(distance));
		float ptopDistance[100][100];
		memset(ptopDistance,0,sizeof(ptopDistance));

		float predist = sqrtf((float)(powf(max_x-min_x,2)+powf(max_y-min_y,2)));
		float maxdist = predist/2;
		float delta = maxdist/2;

		for(int i=0;i<rows;i++){
			for(int j=i+1; j<rows;j++){
				ptopDistance[i][j]= sqrt((float)(powf((float)(x_closestNodesIndex[i]-x_closestNodesIndex[j]),2.0)
						+powf((float)(y_closestNodesIndex[i]-y_closestNodesIndex[j]),2.0)));

				if(ptopDistance[i][j]<maxdist){
					int idx = (int) floorf(ptopDistance[i][j]/delta);
					occIdxdistBins[idx] = occIdxdistBins[idx]+1;
					double squrZ =powf(z_closestNodesIndex[i]-z_closestNodesIndex[j],2);
					sumSqurZ[idx]=sumSqurZ[idx]+squrZ;
				}
			}
		}

		for(int i=0;i<nrbins; i++){
			if(occIdxdistBins[i] == 0.0){
				sumSqurZ[i] = 0.0;
			}else{
				sumSqurZ[i] = sumSqurZ[i]/(2*occIdxdistBins[i]);
			}
		}

		distance[0]=delta/2;
		for(int i=0; i<nrbins;i++){
			distance[i]=distance[i-1]+delta;
		}

		//SEmivariogram process is completed here


		//Starting Fitting

		double maxVario, minLs[3] = {DBL_MAX,0,0}, tmpLS[3]; // For fit
		double lagbin, semibin;
		double b0[3] = {0};

		//find maximum variogram value
		double max = 0.0;
		for(int i=0;i<nrbins;i++){
			if(sumSqurZ[i]>max)
				max = sumSqurZ[i];
		}
		maxVario = max;

		int bins = 10;
		//b0[0] = range
		//b0[1] = sill
		b0[0] = distance[nrbins-1] * 2 /3;
		b0[1] = maxVario;
		lagbin = b0[0] / bins;
		semibin = b0[1]/ bins;
		int i,j;
		for (i=0 ; i<bins ; i++){
			for (j=0 ; j<bins ; j++){
				b0[0] -= lagbin;
				tmpLS[0] = leastsquaredSumFunc(sumSqurZ, b0, distance, nrbins);
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

		if (minLs[1] < 0.0001){
			minLs[1] = distance[0];
		}

		double range = minLs[1];
		double sill = minLs[2];
		//		range = 200;//Hard code
		//		sill = 150;

		int N = idx_onRange+1; // we need one more column  & Row to fill 0 and 1 values (Ordinary Kriging)

		//TODO calculate distance anchor node with
		int counter_idxClosestRange =0;
		while (counter_idxClosestRange < idx_onRange){
			ptopDistance[counter_idxClosestRange][N-1] = sqrt((float)(powf((float)(x_closestNodesIndex[counter_idxClosestRange]-(x_node+minimumX)),2.0)
					+powf((float)(y_closestNodesIndex[counter_idxClosestRange]-(y_node+minimumY)),2.0)));
			counter_idxClosestRange++;
		}

		double a[10201]; //101x101
		double *a_ptr = a;
		double rptr[101][101];//double **rptr;
		//		a = (double*) rptr;

		double b[101];
		int pivot[101];
		double sol[101];
		memset(rptr,0,sizeof(rptr));
		memset(b,0,sizeof(b));
		memset(pivot,0,sizeof(pivot));
		memset(sol,0,sizeof(sol));

		//----------- Applied Gamma Function for Ordinary Kriging
		for(i=0;i<N;i++){
			for(j=i;j<N;j++){
				if(j==N-1) {
					rptr[i][j] = -1.0;
					//cuPrintf("%f < %f kah?? \n",ptopDistance [i][j],range );
					if (ptopDistance [i][j] < range){
						b[i] = sphericalModel(range, sill, ptopDistance[i][j]);
					}
					else{
						b[i] = sill;
					}
					//					//cuPrintf("b[%d]:%f\n",i,b[i]);
				}else{
					if (ptopDistance [i][j] < range){
						rptr[i][j] = sphericalModel(range, sill, ptopDistance[i][j]);
					}
					else{
						rptr[i][j] = sill;
					}
					//					//cuPrintf("rptr[%d]:%f\n",i,rptr[i]);
					rptr[j][i] = rptr[i][j];
					rptr[N-1][j]=1.0;
				}
			}
		}
		rptr[N-1][N-1] = 0.0;
		b[N-1] = 1.0;

		//TODO change this with more efficient way
		for(i=0;i<N;i++){
			for(j=0;j<N;j++){
				*a_ptr = rptr[i][j];
				a_ptr++;
			}
		}

		int err = Doolittle_LU_Decomposition_with_Pivoting(a, pivot,  N);
		//printf ("after decom\n");
		if (err < 0) {//cuPrintf("matrix is Singular\n");
			//printf(" Matrix A is singular\n");
		}
		else {
			err = Doolittle_LU_with_Pivoting_Solve(a, b, pivot, sol, N);
		}

		weightsum=0.0;
		predict = 0.0;
		//calculate predict value in grid or radius or nearest points
		for (i=0 ; i<N-1 ; i++){
			predict += z_closestNodesIndex[i] * sol[i];
			weightsum += sol[i];
		}

		ptrpredictionResult[dumpsize]  = predict;
		ptr_xgridpoint[dumpsize] = (x_node*gridsizes)+minimumX;
		ptr_ygridpoint[dumpsize] = (y_node*gridsizes)+minimumY;
		dev_debugVar[dumpsize]   = threadID;
	}

}


// main routine that executes on the host
int main(void)
{
	/** Read, parse  input LIDAR data & find min,max value
        and calculate range X & Y
	 **/

	clock_t start_time = clock();
	int lineNumber=0;//1186845;
	char* inputPathLIDARdata = "data/Data4_XYZ_Ground.txt";
	//char* inputPathLIDARdata = "data/DataSample.txt";

	//Calculate number of line from input file
	static const char* filename = inputPathLIDARdata;
	FILE *filetmp = fopen ( filename, "r" );
	if ( filetmp != NULL )
	{
		char line [ 128 ];
		while ( fgets ( line, sizeof line, filetmp ) != NULL ) /* read a line */
		{
			lineNumber++; /* write the line */
		}
		fclose ( filetmp );
	}
	else
		perror ( filename );

	printf("Number of Input Data: %d\n", lineNumber);

	int N = lineNumber;
	FILE *file=NULL;
	int i=0;

	//start Gridding Process
	file=fopen(inputPathLIDARdata,"r");
	if(file==NULL){
		fprintf(stderr,"[gridding.cu] cannot open input LIDAR Data\n");
		exit(EXIT_FAILURE);
	}

	double *x=NULL;
	double *y=NULL;
	double *z=NULL;
	x = (double*)malloc(sizeof(double)*(N));
	y = (double*)malloc(sizeof(double)*(N));
	z = (double*)malloc(sizeof(double)*(N));
	for(i=0;i<N;i++){
		fscanf(file,"%lf %lf %lf", &x[i], &y[i], &z[i]);
		findMinMax(x[i], y[i]);
	}
	fclose (file);

	//round min & max value
	minY = floor(minY);
	minX = floor(minX);
	maxX = ceil(maxX);
	maxY = ceil(maxY);
	printf ("min X %lf; max X %lf\n" , minX, maxX );
	printf ("min Y %lf; max Y %lf\n" , minY, maxY );
	int gridXrange = (int) (maxX-minX );
	int gridYrange = (int) (maxY-minY );
	printf ("gridXrange original data %d (in meters) \n", gridXrange);
	printf ("gridYrange original data %d (in meters) \n", gridYrange);

	/**
	 * Calculate grid size based on desired gridding size
	 */

	double gridsize = 1; // in meter
	int numdevices = 2;  // set with 1 or 2 devices

	//set CUDA thread dimension
	int dimGridsize  = gridXrange/gridsize;
	int dimBlocksize = gridYrange/gridsize;
	int totalGrids = (dimBlocksize)*(dimGridsize);
	int numthreads_pergpu = totalGrids/numdevices;

	printf("Grid size                 : %2.2f m \n",gridsize);
	printf("DimGridsize               : %d\n",dimGridsize);
	printf("DimBlocksize              : %d\n",dimBlocksize);
	printf("Number of TOTAL GPU Grid  : %d threads\n",totalGrids);

	dim3 dimGrid(1024,1);
	dim3 dimBlock(1024,1);

	//Define variable for all CUDA devices
	double *dev_debugVar, *dev_debugVar2; //for debugger purpose
	double *dev_x, *dev_x2, *dev_y, *dev_y2, *dev_z, *dev_z2; //x,y,z store in devices
	double *predictionResult, *predictionResult2; //store prediction result here
	//pointer to result storage in host
	double *host_predictionResult  = (double*) malloc(sizeof(double)*1024*1020);
	double *host_predictionResult2 = (double*) malloc(sizeof(double)*1024*1020);
	memset(host_predictionResult, -1,sizeof(double)*1024*1020);
	memset(host_predictionResult2,-1,sizeof(double)*1024*1020);
	double *dev_y_gridpoint, *dev_x_gridpoint,*dev_y_gridpoint2, *dev_x_gridpoint2;//saving corresponding x,y point in devices
	//saving x,y correponding point in host
	double *x_gridpoint  = (double*) malloc(sizeof(double)*numthreads_pergpu);//device 1
	double *y_gridpoint  = (double*) malloc(sizeof(double)*numthreads_pergpu);//device 1
	double *x_gridpoint2 = (double*) malloc(sizeof(double)*numthreads_pergpu);//device 2
	double *y_gridpoint2 = (double*) malloc(sizeof(double)*numthreads_pergpu);//device 2
	//create debugger storage
	double *host_debugVar  = (double*) malloc(sizeof(double)*numthreads_pergpu);//device 1
	double *host_debugVar2 = (double*) malloc(sizeof(double)*numthreads_pergpu);//device 2
	memset(host_debugVar, -1,sizeof(double)*numthreads_pergpu);
	memset(host_debugVar2,-1,sizeof(double)*numthreads_pergpu);
	int device; //device ID

	int loop=0;
	while(numthreads_pergpu>(1020*1020)){
		numthreads_pergpu = numthreads_pergpu/2;
		loop++;
	}
	printf("numthreads per gpu devices: %d threads/devices\n\n",numthreads_pergpu);
	clock_t preprocessing_time;
	clock_t finished_time;

	preprocessing_time = clock();
	int p;

	printf("Running Kernel threads...\n");
	for(p=0;p<=loop;p++){
		cudaSetDevice(0);
		//cudaDeviceReset();
		cudaGetDevice(&device);
		cudaThreadSetLimit(cudaLimitMallocHeapSize,1024*1024*1024);

		cudaMalloc((void**)&dev_x,sizeof(double)*(N));
		cudaMalloc((void**)&dev_y,sizeof(double)*(N));
		cudaMalloc((void**)&dev_z,sizeof(double)*(N));
		cudaMalloc((void**)&predictionResult, sizeof(double)*numthreads_pergpu);
		cudaMalloc((void**)&dev_x_gridpoint, sizeof(double)*numthreads_pergpu);
		cudaMalloc((void**)&dev_y_gridpoint, sizeof(double)*numthreads_pergpu);
		cudaMalloc((void**)&dev_debugVar, sizeof(double)*numthreads_pergpu);
		cudaMemset((void*)predictionResult,-1,sizeof(double)*numthreads_pergpu);

		cudaMemcpy(dev_x, x, sizeof(double)*(N), cudaMemcpyHostToDevice);
		cudaMemcpy(dev_y, y, sizeof(double)*(N), cudaMemcpyHostToDevice);
		cudaMemcpy(dev_z, z, sizeof(double)*(N), cudaMemcpyHostToDevice);

		//limit threads per device


		on_core_process<<< dimGrid, dimBlock>>>(dev_debugVar,p*numthreads_pergpu,dev_x_gridpoint,dev_y_gridpoint,predictionResult, dev_x, dev_y, dev_z,
				minX,minY, N,gridsize, (p+1)*numthreads_pergpu,dimGridsize);


		printf("[Device %d] CUDA err: %s \n", device,cudaGetErrorString(cudaGetLastError()));

		//If we need second devices, then start CUDA Programming device 1
		if(numdevices==2){
			cudaSetDevice(1);
			//cudaDeviceReset();
			cudaGetDevice(&device);
			cudaThreadSetLimit(cudaLimitMallocHeapSize,1024*1024);

			cudaMalloc((void**)&dev_x2,sizeof(double)*(N));
			cudaMalloc((void**)&dev_y2,sizeof(double)*(N));
			cudaMalloc((void**)&dev_z2,sizeof(double)*(N));
			cudaMalloc((void**)&predictionResult2, sizeof(double)*numthreads_pergpu);
			cudaMalloc((void**)&dev_x_gridpoint2, sizeof(double)*numthreads_pergpu);
			cudaMalloc((void**)&dev_y_gridpoint2, sizeof(double)*numthreads_pergpu);
			cudaMalloc((void**)&dev_debugVar2, sizeof(double)*numthreads_pergpu);
			cudaMemset((void*)predictionResult2,-1,sizeof(double)*numthreads_pergpu);

			cudaMemcpy(dev_x2, x, sizeof(double)*(N), cudaMemcpyHostToDevice);
			cudaMemcpy(dev_y2, y, sizeof(double)*(N), cudaMemcpyHostToDevice);
			cudaMemcpy(dev_z2, z, sizeof(double)*(N), cudaMemcpyHostToDevice);

			on_core_process<<< dimGrid, dimBlock>>>(dev_debugVar2,(int)((p+1)*numthreads_pergpu),dev_x_gridpoint2,dev_y_gridpoint2,predictionResult2, dev_x2, dev_y2, dev_z2,
					minX,minY, N,gridsize, (p+2)*numthreads_pergpu, dimGridsize);

			printf("[Device %d] CUDA err: %s \n", device,cudaGetErrorString(cudaGetLastError()));
		}
		//cudaThreadSynchronize();
		//cudaDeviceSynchronize();

		cudaMemcpy(host_predictionResult,predictionResult,sizeof(double)*numthreads_pergpu,cudaMemcpyDeviceToHost);
		cudaMemcpy(x_gridpoint,dev_x_gridpoint,sizeof(double)*numthreads_pergpu,cudaMemcpyDeviceToHost);
		cudaMemcpy(y_gridpoint,dev_y_gridpoint,sizeof(double)*numthreads_pergpu,cudaMemcpyDeviceToHost);
		cudaMemcpy(host_debugVar,dev_debugVar,sizeof(double)*numthreads_pergpu,cudaMemcpyDeviceToHost);


		if(numdevices==2){
			cudaMemcpy(host_predictionResult2,predictionResult2,sizeof(double)*numthreads_pergpu,cudaMemcpyDeviceToHost);
			cudaMemcpy(x_gridpoint2,dev_x_gridpoint2,sizeof(double)*numthreads_pergpu,cudaMemcpyDeviceToHost);
			cudaMemcpy(y_gridpoint2,dev_y_gridpoint2,sizeof(double)*numthreads_pergpu,cudaMemcpyDeviceToHost);
			cudaMemcpy(host_debugVar2,dev_debugVar2,sizeof(double)*numthreads_pergpu,cudaMemcpyDeviceToHost);
		}

		//lets Write output value to file
		FILE *fout;

		/* open the file */
		fout = fopen("output/prediction_result.txt", "a");
		if (fout == NULL) {
			printf("I couldn't open output/prediction_result.txt for writing.\n");
			exit(0);
		}

		/* write to the file */
		printf("Writting to file ... \n");

		for (i=0; i< numthreads_pergpu; i++){
			fprintf(fout, "%2.2f  ", *(x_gridpoint+i));
			fprintf(fout, "%2.2f  ", *(y_gridpoint+i));
			fprintf(fout, "%2.2f\n", *(host_predictionResult+i));
			if(numdevices==2){
				fprintf(fout, "%2.2f  ", *(x_gridpoint2+i));
				fprintf(fout, "%2.2f  ", *(y_gridpoint2+i));
				fprintf(fout, "%2.2f\n", *(host_predictionResult2+i));
			}
		}
		/* close the file */
		fclose(fout);

		/* create debug file */
		fout = fopen("output/debug.txt", "a");
		if (fout == NULL) {
			printf("I couldn't open output/prediction_result.txt for writing.\n");
			exit(0);
		}

		/* write to the file */
		printf("Writting DEBUG file ... \n");
		fprintf(fout,"threadID written as below (one line one GPU thread): \n");
		for (i=0; i< numthreads_pergpu; i++){
			fprintf(fout, "%2.2f\n", *(host_debugVar+i));
			if(numdevices==2){
				fprintf(fout, "%2.2f\n", *(host_debugVar2+i));
			}
		}
		/* close the file */
		fclose(fout);
		printf("Finished wrote in loop %d\n",p);
	}

	finished_time = clock();
	//	printf("CUDA Synch err: %s \n", cudaGetErrorString(cudaDeviceSynchronize()));
	printf("CUDA last err: %s \n", cudaGetErrorString(cudaGetLastError()));

	clock_t postprocessing_time = clock();

	double time_preprocessing 		= ((double)(preprocessing_time-start_time))/CLOCKS_PER_SEC;
	double time_kernelProcessing 	= ((double)(finished_time-preprocessing_time))/CLOCKS_PER_SEC;
	double time_postprocessing	    = ((double)(postprocessing_time-finished_time))/CLOCKS_PER_SEC;

	printf("\ntotal Preprocessing time %2.2f \n",time_preprocessing);
	printf("total Kernel time %2.2f \n",time_kernelProcessing);
	printf("total Postprocessing time %2.2f \n",time_postprocessing);

	//de-allocate memory both in host and devices
	free(x);
	free(y);
	free(z);
	cudaFree(dev_x);
	cudaFree(dev_y);
	cudaFree(dev_z);
	cudaFree(dev_x2);
	cudaFree(dev_y2);
	cudaFree(dev_z2);
	cudaFree(predictionResult);
	cudaFree(predictionResult2);
}

void findMinMax (double x, double y){
	if (minX == 0.0 && maxX == 0.0){
		minX = x;
		minY = y;
		maxX = x;
		maxY = y;
	}

	if(minY>y)
		minY = y;
	if(minX>x)
		minX = x;
	if(maxX<x)
		maxX = x;
	if(maxY<y)
		maxY = y;
}