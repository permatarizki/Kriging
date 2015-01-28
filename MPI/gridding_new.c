#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <mpi.h>
#include <errno.h>
#include <limits.h>
#include <malloc.h>
#include <string.h>

#include "gridding.h"

#define DEBUGLEVEL0
//#define DEBUGLEVEL1

//debug mode enable printf
#ifdef DEBUGLEVEL0
#define PRINTDEBUGMODE0 printf
#else
#define PRINTDEBUGMODE0(format, args...) ((void)0)
#endif

//this debug macro is for more details
#ifdef DEBUGLEVEL1
#define PRINTDEBUGMODE1 printf
#else
#define PRINTDEBUGMODE1(format, args...) ((void)0)
#endif

#define NUM_MAX_PROCESS 12

#define BUF_SIZE 200000

void findMinMax();
char* itoa();
void CalDistPointfromGrid();
double Euclidean();

double minX=0.0, minY=0.0, maxX=0.0, maxY=0.0;

//Lidar Data mapped grid
//Info_Grid *startGrid=NULL;
PointerOfGrid **buffNodes_inOneGrid=NULL;
Info_Grid *new_list=NULL;
Info_Grid *cur=NULL;
Info_Grid *forFreeGrid=NULL;

//Calculate distance from grid point
Distnode *Head_distfromGrid=NULL;
Distnode *Cur_DistfromGrid=NULL;
Distnode *Tail_DistfromGrid=NULL;
Distnode *ForFree_DistfromGrid=NULL;

//Linked list selection sort
Distnode *Cur_Sort=NULL;
Distnode *PrevCur_Sort=NULL;
Distnode *Pivot_Sort=NULL;
Distnode *PrevPivot_Sort=NULL;
Distnode *tmp_Sort=NULL;

int main(int argc, char *argv[]){

	//check validity of input arguments
	if(argc!=4){
		PRINTDEBUGMODE0("[ERROR] Please check number of arguments!\n");
		PRINTDEBUGMODE0("Application <gridsizes(m)> <searchRadius(m)> <PathtoTheInputFile.txt>\n");
		PRINTDEBUGMODE0("argv[1]:%s\n",argv[1]);
		PRINTDEBUGMODE0("argv[2]:%s\n",argv[2]);
		PRINTDEBUGMODE0("argv[3]:%s\n",argv[3]);
		exit(0);
	}

	long temp_gridsize = atol(argv[1]);

	double GRID_SIZE = (double) temp_gridsize ;
	if(GRID_SIZE<1) GRID_SIZE = 0.5; //hard code for parsing args < 1
	PRINTDEBUGMODE1("tempgrid:%lu\n",temp_gridsize);
	PRINTDEBUGMODE1("GRIDSIZE:%2.2f\n",GRID_SIZE);
	int searchRadius = strtol(argv[2],NULL,10);//for radius
	char *filename = argv[3];

	char path_p[50]="/nfs/code/mata/output/PredictionPerGrid";

	int rankId, numProcess;
	int bufflen = 512;
	char hostname[bufflen];
	int i, j, k, p, q;
	int flag=0;
	int dsize=0;
	int nrbins = 50;
	double *x=NULL, *y=NULL, *z=NULL, **ptopDist;
	//double maxD;
	int minX_inputData, minY_inputData, maxX_inputData, maxY_inputData, gridXrange, gridYrange;
	int numberofGrids_X, numberofGrids_Y;
	double maxDistance;
	double startTime, endTime;
	double  totalTime=0.0;

	//For variogram
	double sum_SqurZ[nrbins+2], distDelta[nrbins+2];


	char fileType[] = ".txt";
	FILE *fpdata=NULL;
	FILE *fPrediction=NULL;

	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &rankId);
	MPI_Comm_size(MPI_COMM_WORLD, &numProcess);
	if(numProcess > NUM_MAX_PROCESS){
		printf("[ERROR] numProcess > NUM_MAX_PROCESS\n");
		exit(0);
	}

	startTime=MPI_Wtime();

	/*===================== Read Data =============================*/
	FILE *file = fopen ( filename, "r" );
	int inputsize=0;
	if ( file != NULL )
	{
		char line [ 128 ]; /* or other suitable maximum line size */
		while ( fgets ( line, sizeof line, file ) != NULL ) /* read a line */
		{
			//fputs ( line, stdout ); /* write the line */
			inputsize++;
		}
		fclose ( file );
	}
	else
	{
		printf("[ERROR] Invalid input Path\n");
		perror ( filename ); /* why didn't the file open? */
	}

	dsize = inputsize;
	i=0; 

	fpdata = fopen(filename,"r");
	x = (double*)malloc(sizeof(double)*dsize);
	y = (double*)malloc(sizeof(double)*dsize);
	z = (double*)malloc(sizeof(double)*dsize);
	if(x==NULL | y==NULL | z==NULL){
		printf("[error malloc] X Y Z\n");
		exit(0);
	}

	fseek(fpdata,0,SEEK_SET);
	gethostname(hostname, bufflen);
	PRINTDEBUGMODE1( "Start process %d at %s\n", rankId, hostname );

	//Preprocessing steps is here
	while(!feof(fpdata)){
		fscanf(fpdata, "%lf %lf %lf", &x[i], &y[i], &z[i]);
		findMinMax(x[i], y[i]);
		//fprintf(ferr,"data: %lf %lf %lf\n", x[i], y[i], z[i]);
		i++;
	}
	minX_inputData = (int)floor(minX);	maxX_inputData = (int)ceil(maxX);	minY_inputData = (int)floor(minY);	maxY_inputData = (int)ceil(maxY);
	gridXrange = maxX_inputData-minX_inputData; gridYrange = maxY_inputData-minY_inputData;
	numberofGrids_X = gridXrange / GRID_SIZE; numberofGrids_Y = gridYrange / GRID_SIZE;
	int totalGrids = numberofGrids_X * numberofGrids_Y;
	int numGridPerNode = ceil(totalGrids/numProcess);
	int startGrid_X[NUM_MAX_PROCESS];
	int startGrid_Y[NUM_MAX_PROCESS];
	// we create Grid Index per process
	int grid_index[NUM_MAX_PROCESS];
	// specify grid index for each process
	grid_index[rankId] = rankId*numGridPerNode;

	//we map grid_index to X,Y point to be used in the start of iteration per processor
	startGrid_X[rankId] = (int)(grid_index[rankId]%numberofGrids_X);
	startGrid_Y[rankId] = (int)floor(grid_index[rankId]/numberofGrids_X);

	if(rankId==0) {//choose the faster processor
		PRINTDEBUGMODE0("-----------------------------------------------\n");
		PRINTDEBUGMODE0(" Total processors (workers)  : %d\n", numProcess);
		PRINTDEBUGMODE0(" Number of Input LiDAR data  : %d\n",dsize);
		PRINTDEBUGMODE0("  min (%d,%d)\n  max (%d,%d)\n", minX_inputData, minY_inputData,maxX_inputData, maxY_inputData);
		PRINTDEBUGMODE0(" GRID_SIZE                   : %2.2f meter(s)\n",(double)GRID_SIZE);
		PRINTDEBUGMODE0(" GRIDrange (X,Y)             : (%d,%d)\n", gridXrange, gridYrange);
		PRINTDEBUGMODE0(" numberofGrids (X,Y)         : (%d,%d)\n", numberofGrids_X, numberofGrids_Y);
		PRINTDEBUGMODE0(" Total available Grids       : %d\n",totalGrids);
		PRINTDEBUGMODE0(" NumGridsPerNode             : %d\n",numGridPerNode);
		PRINTDEBUGMODE0("-----------------------------------------------\n");
	}
	PRINTDEBUGMODE1("grid_index[%d]:%d\n",rankId,grid_index[rankId]);
	PRINTDEBUGMODE1("startGrid[%d]:(%2.2f,%2.2f)\n",rankId,startGrid_X[rankId],startGrid_Y[rankId]);

	// Prepare buffer to store nodes in a grid within searchRange
	buffNodes_inOneGrid = (PointerOfGrid**)malloc(sizeof(PointerOfGrid*)*(numberofGrids_X+1)); // assume that boundary X axis in also grid point
	if(buffNodes_inOneGrid== NULL){
		printf("[malloc error] cannot allocate %d buffNodes_inOneGrid_X\n", numberofGrids_X+1);
		exit(0);
	}

	PRINTDEBUGMODE1("malloc buffNodes_inOneGrid %d ...\n", rankId);
	for(i=0;i<=numberofGrids_X;i++){
		buffNodes_inOneGrid[i] = (PointerOfGrid*)malloc(sizeof(PointerOfGrid)*(numberofGrids_Y+1)); // assume that boundary Y axis in also grid point
		if(buffNodes_inOneGrid[i] == NULL){
			printf("[malloc error] cannot allocate %d buffNodes_inOneGrid_Y  \n", numberofGrids_Y+1);
			exit(0);
		}
		for(j=0;j<=numberofGrids_Y;j++){
			buffNodes_inOneGrid[i][j].next=NULL;
		}
		//FOR_DEBUG_PRINT("row_num:%d\n",i);FOR_DEBUG_PRINT("Load grid on memory\n");
	}

	//optimation per process
	int tempsize = dsize;
	while((tempsize%numProcess)!=0){
		tempsize--;
	}

	PRINTDEBUGMODE1("numProcess: %d\n",numProcess);
	PRINTDEBUGMODE1("tempsize: %d\n",tempsize);

	/* lets process data input one by one :
	 * hint :  One input point will be places to only one closest grid point
	 * */

	int numInputdataPerProcessor = tempsize/numProcess; //equally divide jobs from each worker
	PRINTDEBUGMODE1("numInputdataPerProcessor: %d\n",numInputdataPerProcessor);
	for(i=0;i<numInputdataPerProcessor;i++){ //148355
		//divide input data based on rankId on each processor(worker)
		int idx_datainput = i+rankId*numInputdataPerProcessor;
		if(idx_datainput<dsize){
			//find a closest grid point from an input data
			int lower_gridX = (int)floor(x[idx_datainput]);
			int upper_gridX = (int)ceil(x[idx_datainput]);
			int lower_gridY = (int)floor(y[idx_datainput]);
			int upper_gridY = (int)ceil(y[idx_datainput]);
			double closest_gridpointX;
			double closest_gridpointY;

			if(GRID_SIZE >= 1){
				int gridsize_var= GRID_SIZE;
				while((int)(lower_gridX%gridsize_var)!=0){
					lower_gridX--;
				}

				while((int)(upper_gridX%gridsize_var)!=0){
					upper_gridX++;
				}

				while((int)(lower_gridY%gridsize_var)!=0){
					lower_gridY--;
				}

				while((int)(upper_gridY%gridsize_var)!=0){
					upper_gridY++;
				}
			}

			if((sqrt(pow(x[idx_datainput]-lower_gridX,2)+pow(y[idx_datainput]-lower_gridY,2)))<
					(sqrt(pow(x[idx_datainput]-upper_gridX,2)+pow(y[idx_datainput]-upper_gridY,2))) ){
				closest_gridpointX = lower_gridX;
				closest_gridpointY = lower_gridY;
			}else{
				closest_gridpointX = upper_gridX;
				closest_gridpointY = upper_gridY;
			}

			//TODO iterate this closest grid point within a searchRange
			PRINTDEBUGMODE1("closest_gridpointX:%lf\n",closest_gridpointX);
			PRINTDEBUGMODE1("closest_gridpointY:%lf\n",closest_gridpointY);

			//ensure that still inside boundary area
			if((closest_gridpointX>=minX_inputData)&&(closest_gridpointY>=minY_inputData)&&(closest_gridpointX<=maxX_inputData)&&(closest_gridpointY<=maxY_inputData)){

				if(((closest_gridpointX-minX_inputData)/GRID_SIZE)>= numberofGrids_X){
					PRINTDEBUGMODE1("HOREWWWW X\n");
				}
				if(((closest_gridpointY-minY_inputData)/GRID_SIZE)>= numberofGrids_Y){
					PRINTDEBUGMODE1("HOREWWWW Y\n");
				}

				int idx_x = (int)(closest_gridpointX-minX_inputData)/GRID_SIZE;
				int idx_y = (int)(closest_gridpointY-minY_inputData)/GRID_SIZE;
				PRINTDEBUGMODE1("numberofGrids_X:%d; numberofGrids_Y:%d\n", numberofGrids_X, numberofGrids_Y);

				if(buffNodes_inOneGrid[idx_x]!=NULL){

					cur = buffNodes_inOneGrid[idx_x][idx_y].next;

					if(cur==NULL){
						PRINTDEBUGMODE1("cur next NULL\n");
						new_list = (Info_Grid*)malloc(sizeof(Info_Grid));
						new_list->x = x[idx_datainput];
						new_list->y = y[idx_datainput];
						new_list->z = z[idx_datainput];
						new_list->next=NULL;

						buffNodes_inOneGrid[idx_x][idx_y].next = new_list;
					} else {
						PRINTDEBUGMODE1("cur next NOT NULL\n");
						while(cur->next!=NULL) {
							cur = cur->next;
						}
						new_list = (Info_Grid*)malloc(sizeof(Info_Grid));
						new_list->x = x[idx_datainput];
						new_list->y = y[idx_datainput];
						new_list->z = z[idx_datainput];

						new_list->next=NULL;
						cur->next = new_list;
					}
				}else{
					//there must be some bug if this happened --> ussually malloc failed
					PRINTDEBUGMODE0("[ERROR] ANOTHER BUG\n");
					PRINTDEBUGMODE0("X:%d; Y:%d\n",idx_x,idx_y);
					PRINTDEBUGMODE0("numberofGrids_X:%d; numberofGrids_Y:%d\n", numberofGrids_X, numberofGrids_Y);
				}

			}
		}
		else{
			PRINTDEBUGMODE0("idx_datainput:%d\n",idx_datainput);
		}
	}

	free(x);
	free(y);
	free(z);

	/**
	 * Gridding, Semi-Variogram, Prediction Process is here
	 */
	strcat(path_p,itoa(rankId,10));
	strcat(path_p,fileType);
	fPrediction = fopen(path_p,"w");

	PRINTDEBUGMODE1("Start Gridding Process ... \n");
	// Divide number of grids with the available process/workers
	for(p=0;p< numGridPerNode;p++){
		int idx_grid =p+rankId*numGridPerNode;
		if(idx_grid<numberofGrids_X*numberofGrids_Y){
			int idx_x = idx_grid%numberofGrids_X;
			int idx_y = (int) idx_grid/numberofGrids_Y;
			int temp_idx_x = idx_x;
			int temp_idx_y = idx_y;

			//iterate based on search range & collect in a linked list
			int points_counter = 0;
			minX=0.0, minY=0.0, maxX=0.0, maxY=0.0;
			if(GRID_SIZE<searchRadius){
				//TODO FIX this bug
				int max_loop = (int)ceil(searchRadius/GRID_SIZE);
				idx_x = idx_x-max_loop;
				idx_y = idx_y-max_loop;
				for(i=0;i<2*max_loop;i++){
					for(j=0;j<2*max_loop;j++){
						if((idx_x+i)>=0 && ((idx_x+i) < numberofGrids_X) && (idx_y+j)>=0 && (idx_y+j) < numberofGrids_Y) { //while still inside boundary of grids
							CalDistPointfromGrid(idx_x+i,idx_y+j, &points_counter);
						}
					}
				}

			}else{
				CalDistPointfromGrid(idx_x,idx_y, &points_counter);
			}

			PRINTDEBUGMODE1("point_counter :%d\n",points_counter);

			//calculate variogram
			maxDistance = sqrt(pow(maxX-minX,2)+pow(maxY-minY,2));
			maxDistance = maxDistance/2;

			points_counter = points_counter+1;
			ptopDist = (double**)malloc(sizeof(double*)*points_counter);
			for(k=0;k<points_counter;k++) {
				ptopDist[k] = (double*)malloc(sizeof(double)*points_counter);
			}
			//initialize with 0 value
			for(k=0;k<points_counter;k++) {
				for(q=0;q<points_counter;q++) {
					ptopDist[k][q] = 0.0;
				}
			}
			//calculate semivariogram
			FindSemivar(Head_distfromGrid, nrbins, sum_SqurZ, maxDistance, distDelta, ptopDist);

			double range, sill;
			//Fitting process with model
			FitSemivariogram(sum_SqurZ, distDelta, nrbins, &range, &sill);

			//Prediction
			double coord_gridX = minX_inputData + temp_idx_x*GRID_SIZE;
			double coord_gridY = minY_inputData + temp_idx_y*GRID_SIZE;
			Prediction(Head_distfromGrid, points_counter, range, sill, rankId, ptopDist, coord_gridX, coord_gridY, fPrediction);

			//free memory
			for(k=0;k<points_counter;k++) {
				free(ptopDist[k]);
			}
			free(ptopDist);

			Cur_DistfromGrid = Head_distfromGrid;
			while(Cur_DistfromGrid!=NULL) {
				ForFree_DistfromGrid = Cur_DistfromGrid;
				Cur_DistfromGrid = Cur_DistfromGrid -> next;
				free(ForFree_DistfromGrid);
			}
			Head_distfromGrid=NULL;
			Tail_DistfromGrid=NULL;

		}
	}


	endTime = MPI_Wtime();
	totalTime = endTime-startTime;
	MPI_Finalize();

	PRINTDEBUGMODE0( "Finished process %d at %s in %lf seconds\n", rankId, hostname, totalTime );

	int temp_numProc=0;
	for(i=0;i<numProcess;i++){
		temp_numProc = temp_numProc+i;
	}

	flag = flag + rankId;
	if(flag == temp_numProc){
		char path_output[]="cat ../../mata/output/* > ";
		strcat(path_output,"result.txt");
		system(path_output);
	}
	return 0;

}

void CalDistPointfromGrid(int gridIdx_X, int gridIdx_Y, int* pCnt) {
	cur = buffNodes_inOneGrid[gridIdx_X][gridIdx_Y].next;
	if(cur!=NULL) {
		PRINTDEBUGMODE1("around[%d][%d]\n",gridIdx_X,gridIdx_Y);
		//FOR_DEBUG_PRINT("x %lf, y %lf\n", (double)i+(double)GRID_SIZE/2,(double)j+(double)GRID_SIZE/2);
		while(cur!=NULL){
			findMinMax(cur->x, cur->y);
			(*pCnt)++;
			if(Head_distfromGrid==NULL) {
				Head_distfromGrid = (Distnode*)malloc(sizeof(struct Dist_Node));
				Head_distfromGrid -> coord = cur;
				Head_distfromGrid -> next = NULL;
				Tail_DistfromGrid = Head_distfromGrid;
			} else {
				Cur_DistfromGrid = (Distnode*)malloc(sizeof(struct Dist_Node));
				Cur_DistfromGrid -> coord = cur;
				Cur_DistfromGrid -> next = NULL;
				Tail_DistfromGrid-> next = Cur_DistfromGrid;
				Tail_DistfromGrid = Cur_DistfromGrid;
			}
			cur = cur->next;
		}
	} else {
		PRINTDEBUGMODE1("NO POINT IN THIS GRID \n");
	}
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

char* itoa(int val, int base) {
	// check that the base if valid
	static char buf[32] = {0};
	int i = 30;
	for(; val && i ; --i, val /= base)

		buf[i] = "0123456789abcdef"[val % base];

	return &buf[i+1];
}
