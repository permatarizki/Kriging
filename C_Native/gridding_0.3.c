#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <malloc.h>
#include <time.h>

#include "gridding.h"

//#define DEBUG_SEMIVARIOGRAM_LEVEL1

//lidar xyz points
struct LIDARnodeGridPointer **grid=NULL;
struct LIDARnodeGrid *point=NULL;
struct LIDARnodeGrid *cur=NULL;
struct LIDARnodeGrid *prev=NULL;

//calculate distance from Grid point and point xyz
struct Distnode *Head_distfromGrid=NULL;
struct Distnode *Cur_distfromGrid=NULL;
struct Distnode *Tail_distfromGrid=NULL;
struct Distnode *Prev_distfromGrid=NULL;

//relate to sort
struct Distnode *prevPivot_sort=NULL;
struct Distnode *Pivot_sort=NULL;
struct Distnode *prevCur_sort=NULL;
struct Distnode *Cur_sort=NULL;
struct Distnode *tmp=NULL;

double minX=0.0, minY=0.0, maxX=0.0, maxY=0.0;
double floorMinX = 0.0, floorMinY =0.0;
double ceilMaxX = 0.0, ceilMaxY = 0.0;

void findMinMax (double x, double y);
void linkedpointsinGrid(int i, int j, int radX, int radY, int* pointCnt);
double euclidean(double gridX, double gridY, double pointX, double pointY);
void printDistnode ();
int displayProgress(int i, int j, int gridXsize, int gridYsize, int cur_percent);

//N : Line Number. this is argument
//GridSz : Grid Size. this is argument
//numofNearestPoint : Nearest points. this is argument
//GridRadius : Grid radius this is argument
int gridding (char* inputPathLIDARdata, int N, int GridSz, int numofNearestPoint, int GridRadius, int nrbins){
	
	//cal time
	clock_t gridding_tick;
	clock_t tmp_gridding;
	clock_t semi_tick;
	clock_t fit_tick;
	clock_t predict_tick;
	double gridding_time=0.0;
	double in_semi_time=0.0;
	double semi_time=0.0;
	double fit_time=0.0;
	double predict_time=0.0;

	//read from data file
	double *x=NULL;
	double *y=NULL; 
	double *z=NULL; 
	
	//-----make grid
	int gridXrange=0; 
	int gridYrange=0;
	int gridX=0; 
	int gridY=0;
	int i=0;
	int j=0;
	int k=0;
	int p=0;
	int gridXsize=0;// gridXsize = gridXrange/GridSz
	int gridYsize=0;
	FILE *file=NULL;
	FILE *file_out=NULL;

	//-----start to find nearest points
	int cnt_point=0;
	int idx_radius=0; //search by grid radius
	int radX, radY;
	int cardinal_direction=0;//0:down, 1:right, 2:up, 3:left

	int total_points=0;

	printf("Gridding ...\n");
	//"/home/seop/git-repo/lidar-project/input/100.txt"
	file=fopen(inputPathLIDARdata,"r");
	if(file==NULL){
		fprintf(stderr,"[gridding.c] cannot open input LIDAR Data\n");
		exit(EXIT_FAILURE);
	}

	x = (double*)malloc(sizeof(double)*(N));
	y = (double*)malloc(sizeof(double)*(N));
	z = (double*)malloc(sizeof(double)*(N));
	for(i=0;i<N;i++){
		fscanf(file,"%lf %lf %lf", &x[i], &y[i], &z[i]);
		DEBUG_PRINT1("i: %d : %lf  %lf  %lf\n", i, x[i], y[i], z[i]);
		findMinMax(x[i], y[i]);
	}
	fclose (file);

	DEBUG_PRINT1 ("max x : %d\n",(int) (ceil(maxY)-  floor(minY)) );
	printf ("max min %lf %lf \n" , maxY , minY);
	
	floorMinY = floor(minY);
	floorMinX = floor(minX);
	ceilMaxX = ceil(maxX);
	ceilMaxY = ceil(maxY);

	gridXrange = (int) (ceilMaxX-  floorMinX );
	gridYrange = (int) (ceilMaxY-  floorMinY );

	printf ("gridXrange %d\n", gridXrange);
	printf ("gridYrange %d\n", gridYrange);
	DEBUG_PRINT1 ("start of load grid into memory\n");
	
	//------- make grid
	//load grid into memory
	gridXsize= (int)gridXrange /GridSz ;
	gridYsize= (int)gridYrange /GridSz ;
	grid = (struct LIDARnodeGridPointer**)malloc(sizeof(struct LIDARnodeGridPointer*) * gridXsize );
	for (i=0 ; i< gridXsize ; i++){
		grid[i] = (struct LIDARnodeGridPointer*)malloc(sizeof(struct LIDARnodeGridPointer) * gridYsize );
		for (j=0 ; j<gridYsize ; j++){
			grid[i][j].next = NULL;
		}
	}
	DEBUG_PRINT1 ("end of load grid into memory\n");
	//read point one by one and make grid
	gridding_tick = clock();
	for (i=0 ; i<N ; i++){
		gridX = (int)(x[i]-floorMinX) /GridSz;
		gridY = (int)(y[i]-floorMinY) /GridSz;

		//if x[i] or y[i] is equal to ceilMaxX or ceilMaxY, .....
		if (gridX == gridXsize) gridX--;
		if (gridY == gridYsize) gridY--;

		//printf ("gridX, gridY : %d %d \n", gridX, gridY);
		//printf ("end of cal grid coord\n");
		
		cur = grid[gridX][gridY].next;//just shipping VALUE, not address.

		//printf ("before make points\n");
		if (cur == NULL){
			//printf ("start of first make point list\n");
			point = (struct LIDARnodeGrid *)malloc(sizeof(struct LIDARnodeGrid));
			point->X = x[i];
			point->Y = y[i];
			point->Z = z[i];
			point->next = NULL;
			grid[gridX][gridY].next = point;
			//printf ("grid x, y, z , next, cur: %lf %lf %lf %d %d\n", grid[gridX][gridY].next->X, grid[gridX][gridY].next->Y, grid[gridX][gridY].next->Z, grid[gridX][gridY].next->next, grid[gridX][gridY].next);
			
			//printf ("end of first make point list\n");
		} else {
			//printf ("start of make points list\n");
			while (cur->next != NULL){
				cur = cur->next;
			}
			point = (struct LIDARnodeGrid *)malloc(sizeof(struct LIDARnodeGrid));
			point->X = x[i];
			point->Y = y[i];
			point->Z = z[i];
			point->next = NULL;
			cur->next = point;
			//printf ("end of make points list \n");
		}
	}
	gridding_time = (double)(clock() - gridding_tick) / CLOCKS_PER_SEC;
	free(x); free(y); free(z);

	/*//for debugging
	FILE *fdgrid= fopen("d_grid.txt", "w");
	int d_pointCnt=0;
	for (i=0 ; i<gridXsize ; i++){
		for (j=0 ; j<gridYsize ; j++){
			DEBUG_PRINT1 ("i j : %d %d\n", i, j);
			cur = grid[i][j].next;
			if (cur == NULL) {
				continue;
			}
			//printf ("i, j grid x, y, z , next, cur:%d %d %lf %lf %lf %d %d\n",i,j, grid[i][j].next->X, grid[i][j].next->Y, grid[i][j].next->Z, grid[i][j].next->next, grid[i][j].next);
			while (cur != NULL){
				//printf ("cur %d\n", cur);
				//printf ("cur next %d\n", cur->next);
				// printf ("i, j : %d %d\n", i, j);
				fprintf (fdgrid,"[%2.2lf][%2.2lf] %lf %lf %lf \n",i+floorMinX+0.5,j+floorMinY+0.5, cur->X, cur->Y, cur->Z);
				cur = cur->next;
				d_pointCnt++;
				// printf ("after cur change\n");
			}
			fprintf (fdgrid,"[%2.2lf][%2.2lf] pointcnt : %d\n", i+floorMinX+0.5,j+floorMinY+0.5,d_pointCnt);
			d_pointCnt=0;
			DEBUG_PRINT1 ("\n");
		}
		DEBUG_PRINT1 ("\n");
	}
	fclose(fdgrid);*/


	/*//-----start to read points by GridRadius and find NN points
	file_out=fopen("PointsinGrid.txt","w");
	if (file_out==NULL){
		fprintf(stderr,"[gridding.c] cannot open grid file\n");
		exit(EXIT_FAILURE);
	}*/

	// 1 phase : read lidar points around one grid point
	// 2 phase : find NN points by calculating and sorting by distance
	// 3 phase : write the points to file
	int cur_percent=0;
	int specfic_point=0;

	

	for (i=0 ; i<gridXsize ; i++){
		for (j=0 ; j<gridYsize ; j++){
		
			//if(i>=520 && i < 1020 && j>= 734&& j < 1020) {

			cur_percent = displayProgress( i, j, gridXsize, gridYsize, cur_percent);
			
			//initialize
			minX=0.0;
			minY=0.0;
			maxX=0.0;
			maxY=0.0;
			int pointCnt=0;

			//phase 1 : linked list points around grid
			tmp_gridding = clock();
			radX=i-GridRadius+1;
			for(k=0;k<2*GridRadius-1;k++){
				radY=j-GridRadius+1;
				for(p=0;p<2*GridRadius-1;p++){
					
					if(radX>=0 && radX < gridXsize && radY>=0 && radY < gridYsize) {
						linkedpointsinGrid(i,j,radX,radY, &pointCnt);
					}
					
					radY++;
				}
				radX++;
			}
			
			gridding_time += (double)(clock() - tmp_gridding) / CLOCKS_PER_SEC;
			total_points += pointCnt;

			
				//specfic_point = pointCnt;
			

			double* sum_SqurZ = (double*)malloc(sizeof(double)*(nrbins+2));
			
			int pointCntwithGridpoint = pointCnt+1;
			//printf ("N : %d\n", N);
			double** ptop_distance = (double**)malloc(sizeof(double*)*pointCntwithGridpoint);
			for(k=0;k<pointCntwithGridpoint;k++) {
				ptop_distance[k] = (double*)malloc(sizeof(double)*pointCntwithGridpoint);
			}
			//initailize
			for (k=0 ; k<pointCntwithGridpoint ; k++){
				for (p=0 ;p<pointCntwithGridpoint ; p++){
					ptop_distance[k][p] =0.0;
				}
			}

	
			// fprintf (fdnx,"[%2.2lf][%2.2lf]\n", floorMinX+(double)i+0.5, floorMinY+(double)j+0.5);
			// fprintf (fdnx,"minX %lf maxX %lf minY %lf maxY %lf\n" , minX, maxX, minY, maxY);

			//calculate max distance & delta
			double preDistance = sqrt(pow(maxX-minX,2)+pow(maxY-minY,2));
			
			double maxDistance = preDistance/2;
			//if (i==0 && (j == 9 || j == 7)){
				//fprintf (fdnx,"maxdistance : %lf\n", maxDistance);
			//}
			double delta = maxDistance/nrbins;
			

			//printf ("i : %d, j : %d\n", i, j);
			//calculate semivariogram
			double range;
			double sill;

			semi_tick = clock();
			calculateSemivariogram(Head_distfromGrid, sum_SqurZ, nrbins, ptop_distance, &in_semi_time, &range, &sill, maxDistance, delta, i, j, specfic_point);
			semi_time += (double)(clock() - semi_tick) / CLOCKS_PER_SEC;
		
			//printf ("range : %lf\n", range);
			//printf ("sill : %lf\n", sill);

			//fitting semivariogram
			// fit_tick = clock();
			// fitSemivariogram(sum_SqurZ, distance, nrbins, &range, &sill);
			// fit_time += (double)(clock() - fit_tick) / CLOCKS_PER_SEC;

			free(sum_SqurZ);
			

			double coord_gridX = floorMinX+(double)i+0.5;
			double coord_gridY = floorMinY+(double)j+0.5;
			//printf("[%2.2lf][%2.2lf]----------\n", coord_gridX, coord_gridY);
			//printf ("before prediction\n");
			
			
			//predict_tick = clock();
			prediction(Head_distfromGrid, numofNearestPoint, range, sill, pointCnt, i, j, coord_gridX, coord_gridY, ptop_distance, &predict_time);
			//predict_time += (double)(clock() - predict_tick) / CLOCKS_PER_SEC;
			

			//FILE *ftmp = fopen ("tmp.txt", "a");
			//Head_distfromGrid free and initialize
			Cur_distfromGrid = Head_distfromGrid;
			while (Cur_distfromGrid != NULL){
				//fprintf (ftmp, "g : [%2.2lf][%2.2lf] p : %lf %lf %lf\n",coord_gridX, coord_gridY, Cur_distfromGrid->coord->X,Cur_distfromGrid->coord->Y,Cur_distfromGrid->coord->Z);
				Prev_distfromGrid = Cur_distfromGrid;
				Cur_distfromGrid = Cur_distfromGrid->next;
				free (Prev_distfromGrid);
			}
			//fclose(ftmp);
			Head_distfromGrid = NULL;
			Tail_distfromGrid = NULL;

			for(k=0;k<pointCntwithGridpoint;k++) {
				free (ptop_distance[k]); 
			}
			free (ptop_distance);

			//initailize 		
			//cnt_point = 0;
			//idx_radius = 0;	
			DEBUG_PRINT1 ("--end find points in and around grid\n");
			//}//if

		}
	}
	
	//fclose(file_out);
	//-------end read points by GridRadius and find NN points

	printf ("total calculate points : %d\n", total_points);
	printf("gridding_time : %lf\n", gridding_time);
	printf ("fit_time : %lf\n", in_semi_time);
	printf ("semi time : %lf\n", semi_time-in_semi_time);
	//printf ("fit_time : %lf\n", fit_time);
	printf ("predict_time : %lf\n", predict_time);


	//grid[][] free
	for (i=0 ; i<gridXsize ; i++){
		for (j=0 ; j<gridYsize ; j++){
			cur = grid[i][j].next;
			if (cur == NULL) {
				continue;
			}
			while (cur != NULL){
				prev = cur;
				cur = cur->next;
				free(prev);
			}
		}
		free(grid[i]);
	}
	free(grid);

	return 0;
}

void printDistnode (){
	Cur_distfromGrid = Head_distfromGrid;
	while (Cur_distfromGrid != NULL){
		printf ("%lf -> \n", Cur_distfromGrid->distance);

		Cur_distfromGrid = Cur_distfromGrid->next;

	}
}

void linkedpointsinGrid(int i, int j, int radX, int radY, int* pointCnt){
	cur = grid[radX][radY].next;
	if (cur==NULL){
		DEBUG_PRINT1 ("no point in [%d][%d] grid\n", radX, radY);
	} else {
		while (cur != NULL){
			//DEBUG_PRINT1 ("%lf %lf %lf , " cur->X, cur->Y, cur->Z);
			findMinMax (cur->X, cur->Y);
			(*pointCnt)++;
			//make linked list which stores distance and coordinate.
			if (Head_distfromGrid == NULL){
				Head_distfromGrid= (struct Distnode *)malloc(sizeof(struct Distnode) );
				//Head_distfromGrid->distance = euclidean (floorMinX+i+1/2 , floorMinY+j+1/2 , cur->X, cur->Y);
				Head_distfromGrid->coord = cur;
				Head_distfromGrid->next = NULL;
				Tail_distfromGrid = Head_distfromGrid;
				//Prev_distfromGrid = Head_distfromGrid;
			} else {
				Cur_distfromGrid = (struct Distnode *)malloc(sizeof(struct Distnode) );
				//Cur_distfromGrid->distance = euclidean (floorMinX+i+1/2 , floorMinY+j+1/2 , cur->X, cur->Y);
				Cur_distfromGrid->coord = cur;
				Cur_distfromGrid->next = NULL;
				Tail_distfromGrid->next = Cur_distfromGrid;
				//Prev_distfromGrid = Tail_distfromGrid;
				Tail_distfromGrid = Cur_distfromGrid;
			}

			cur = cur->next;
		}
		DEBUG_PRINT1 ("\n");
	}
}

double euclidean(double gridX, double gridY, double pointX, double pointY) {
	return sqrt ( pow(gridX-pointX,2)+ pow(gridY-pointY,2) );
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

int displayProgress(int i, int j, int gridXsize, int gridYsize, int cur_percent)
{
	double a = 100*((i*gridYsize)+j)/((double)gridXsize*gridYsize);

	if (cur_percent != (int) ceil(a) ){
		cur_percent = (int) ceil(a);
		printf ("Progress : %d percent\n", cur_percent);
	}
	return cur_percent;
}
