#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <mpi.h>
#include <malloc.h>
#include <string.h>

#include "gridding.h"

#define FOR_DEBUG_PRINT
#define FOR_CONFIRM_PRINT  
#define FILE_OUT_INFO

#define MIN_MAX_X_FROM_0 10
#define MIN_MAX_X_FROM_OTHER 11
#define MIN_MAX_Y_FROM_0 12
#define MIN_MAX_Y_FROM_OTHER 13
#define Q_FROM_0 14
#define Z_VALUE_FROM_0 18

#define GRID_Y_RANGE_FROM_0 50
#define GRID_GRID_MIN_FROM_0 51

#define GRID_SIZE 1
#define BUF_SIZE 200000
#define QUOT_OF_ROWS 4
#define QUOT_OF_COLS 4

//void Euclidean();
void findMinMax();
char* itoa();
void CalDistPointfromGrid();
double Eculidean();

double minX=0.0, minY=0.0, maxX=0.0, maxY=0.0;
//Lidar Data mapped grid
//Info_Grid *startGrid=NULL;
Pointer **interval; //**: X, *: Y
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

	int rankId, numProcess;
	int bufflen = 512;
	char hostname[bufflen];
	
	int i, j, k, p;
	int dsize=0, quotient=0, extra=0, avgrows=0;
	int quotientX[QUOT_OF_ROWS], avgrowsX=0, quotientY[QUOT_OF_COLS], avgrowsY=0;// for divide X, Yrange
	int nrbins = 50, nrSubbins = 100, sameIdxCnt;
	double *x, *y, *z, *t, *distBins, **ptopDist;
	//double maxD;
	int gminX, gminY, gmaxX, gmaxY, gabXrange, gabYrange, gridXsize, gridYsize;
	int EgridX, EgridY;
	int gridX, gridY, logicalVar[2], realXpoint, realYpoint;
	int startX, startY, endX, endY, XofGrid, YofGrid;
	int NNendX, NNendY;
	int alphaGridX, alphaGridY;
	int radX, radY, idxOfRadius=0, gridRadius=3, numOfNearestPoint=10;//for radius
	int cnt_point=0, cardinal_direction;//0:x++ 1:y++ 2:x-- 3:y--
	int divYsize, totlaPoints=0;
	double delta, sumZ, maxDistance;
	double startTime, endTime, stGtime, endGtime, stSemitime, endSemitime, stFitime, endFitime, stPtime, endPtime;
	double totalGtime=0.0, totalSemitime=0.0, totalFitime=0.0, totalPtime=0.0, totalTime=0.0;
	double maxTime=0.0, maxGtime=0.0, maxSemitime=0.0, maxFitime=0.0, maxPtime=0.0;
	double Gridsqure, progrssCnt=0;
	
	double fileTime = 0.0, endfileTIme = 0.0;
	
	double buffer_grid[BUF_SIZE][3];

	//For variogram
	double sum_SqurZ[nrbins+2], distDelta[nrbins+2];
	
	char charRank[4];
	char path[50]="/home/mpiuser/Documents/PointInGrid";
	char path_p[50]="/home/mpiuser/Documents/PredictionPerGrid";
	char fileType[] = ".txt";
	FILE *fpdata=NULL, *ferr=NULL, *file_out=NULL;
	FILE *fPrediction=NULL;
	
	struct Info_vario *Info_v;
	
	int msg=0;
	
	MPI_Request req;
	MPI_Status status;
	MPI_File thefile;
	MPI_Offset fileOffset;
	
	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &rankId);
	MPI_Comm_size(MPI_COMM_WORLD, &numProcess);
	
	
	//Data2_XYZ(3000)  sample_data(100000_5)	Data3_XYZ_Ground	1186845		Data4_XYZ_Ground	4517569
	
	
	// Open file 
	//MPI_File_open(MPI_COMM_WORLD, "/nfs/code/ksy/data/sample_data(100_5).txt", MPI_MODE_RDONLY, MPI_INFO_NULL, &thefile);
	//MPI_File_set_view(thefile, fileOffset, MPI_DOUBLE, MPI_DOUBLE, "native", MPI_INFO_NULL);
	//MPI_File_read(thefile, x, dsize, (MPI_DOUBLE), &status);	
	
	//MPI_File_get_size(thefile, &filesize);
	startTime=MPI_Wtime();
 	//fpdata = fopen("/nfs/code/ksy/data/sample_data(1000_5).txt","r");
	fpdata = fopen("/nfs/code/ksy/data/Data4_XYZ_Ground.txt","r");
	//ferr = fopen("/nfs/code/ksy/log.txt","wt");
	
	/*===================== Read Data =============================*/
	i=0;
	dsize = 4517569;
	
	x = (double*)malloc(sizeof(double)*dsize);
	y = (double*)malloc(sizeof(double)*dsize);
	z = (double*)malloc(sizeof(double)*dsize);
	
	fseek(fpdata,0,SEEK_SET);
	gethostname(hostname, bufflen);
	fileTime = MPI_Wtime();
	if(rankId==0) {
		while(!feof(fpdata)){
			fscanf(fpdata, "%lf %lf %lf", &x[i], &y[i], &z[i]);
			findMinMax(x[i], y[i]);
			//fprintf(ferr,"data: %lf %lf %lf\n", x[i], y[i], z[i]);
			i++;
		}
		quotient = dsize / (numProcess);
		gminX = (int)floor(minX);	gmaxX = (int)ceil(maxX);	gminY = (int)floor(minY);	gmaxY = (int)ceil(maxY);	
		FILE_OUT_INFO(ferr, "min X is %lf max X is %lf min Y is %lf max Y is %lf\n", minX, maxX, minY, maxY);
		FOR_DEBUG_PRINT("min X is %lf max X is %lf min Y is %lf max Y is %lf\n", minX, maxX, minY, maxY);
		FOR_DEBUG_PRINT("min X is %lf max X is %lf min Y is %lf max Y is %lf\n", minX, maxX, minY, maxY);
		FILE_OUT_INFO(ferr, "GRID min X is %d max X is %d min Y is %d max Y is %d\n", gminX, gmaxX, gminY, gmaxY);
		gabXrange = gmaxX-gminX; gabYrange = gmaxY-gminY;
		FILE_OUT_INFO(ferr, "GRID gab X is %d gab Y is %d\n", gabXrange, gabYrange);
		FOR_DEBUG_PRINT("GRID gab X is %d gab Y is %d\n", gabXrange, gabYrange);
		EgridX = gabXrange / GRID_SIZE; EgridY = gabYrange / GRID_SIZE;
		
		// MAX Distance
							/*	maxDistance = sqrt(pow(maxX-minX,2)+pow(maxY-minY,2));
								maxDistance = maxDistance/2;
								delta = maxDistance / nrbins; //ditance amongs h(x)
							*/
		//printf("maxd is %lf\n", maxD);
			for(i=0;i<QUOT_OF_ROWS;i++)	{ quotientX[i]=0.0; }
			for(i=0;i<QUOT_OF_COLS;i++)	{ quotientY[i]=0.0; }
			
		//divYsize = 4;
		int extraX=0;
		int div_five_1 =0;
		int div_five_2 =0;
		int div_five_3 = 0;
		
		if(numProcess==16) {
			div_five_1 = (int) floor((double)EgridX * 0.285);//290.7
			div_five_2 = (int) ceil((double)EgridX * 0.715);//729.3
			div_five_3 = (int) ceil((double)div_five_2 * 0.65);//478.15
			div_five_2 = (int) floor((double)div_five_2 * 0.35);//251.85
			
			extraX = div_five_1 %2;
			quotientX[0] = (int)floor((double)div_five_1/ 2);
			quotientX[1] = (int)ceil((double)div_five_1/ 2);
			quotientX[2] = div_five_2;
			quotientX[3] = div_five_3;

			
			i=0;
			extra = EgridY % QUOT_OF_COLS;
			//printf("%d, %d\n", extra,EgridY);
			
			/*while(i<extra) {
				quotientY[i] = (int)ceil((double)EgridY/ QUOT_OF_COLS);
				i++;
			}
			while(i<QUOT_OF_COLS) {
				quotientY[i] = (int)floor((double)EgridY/ QUOT_OF_COLS);
				i++;
			}*/

			FOR_DEBUG_PRINT("EgridX : %d EgridY: %d\n", EgridX, EgridY);
			//printf("guotientX : %d,%d,%d,%d guotientY: %d,%d,%d,%d\n", quotientX[0],quotientX[1], quotientX[2], quotientX[3], quotientY[0],quotientY[1], quotientY[2],quotientY[3]);
			
			 // Initialize , logicalVar: ordinary grid points
			logicalVar[0]=0, logicalVar[1]=0; //[0]: x [1]: y
			
			//Send_Gridsize(numProcess, logicalVar, quotientX, quotientY);
			//for(i=1; i<numProcess; i++){
			int sendP=0;
			for(i=0;i<QUOT_OF_ROWS;i++) {
				FOR_DEBUG_PRINT("Sending to %d ...\n", i);
				//printf("logical[0] %d, logical[1] %d, gridXsize %d, gridYsize %d\n", logicalVar[0], logicalVar[1], gridXsize,gridYsize);
				for(j=0;j<QUOT_OF_COLS;j++) {
					if(i==0) {
						quotientY[0] = (int)floor((double)EgridY * 0.26);//265.2
						quotientY[1] = (int)ceil((double)EgridY * 0.21);//214.2
						quotientY[2] = (int)ceil((double)EgridY * 0.30);//306
						quotientY[3] = (int)floor((double)EgridY * 0.23);//234.6
						//printf("[%d] guotientX : %d,%d,%d,%d guotientY: %d,%d,%d,%d\n", i, quotientX[0],quotientX[1], quotientX[2], quotientX[3], quotientY[0],quotientY[1], quotientY[2],quotientY[3]);
							
					} else if(i==1) {
						quotientY[0] = (int)floor((double)EgridY * 0.23);//234.6
						quotientY[1] = (int)ceil((double)EgridY * 0.31);//316.2
						quotientY[2] = (int)ceil((double)EgridY * 0.29);//295.8
						quotientY[3] = (int)floor((double)EgridY * 0.17);//173.4
						//printf("[%d] guotientX : %d,%d,%d,%d guotientY: %d,%d,%d,%d\n", i, quotientX[0],quotientX[1], quotientX[2], quotientX[3], quotientY[0],quotientY[1], quotientY[2],quotientY[3]);
					} else if(i==2) {
						quotientY[0] = (int)floor((double)EgridY * 0.25);//255
						quotientY[1] = (int)ceil((double)EgridY * 0.29);//295.8
						quotientY[2] = (int)ceil((double)EgridY * 0.20);//204
						quotientY[3] = (int)floor((double)EgridY * 0.26);//255
						//printf("[%d] guotientX : %d,%d,%d,%d guotientY: %d,%d,%d,%d\n", i, quotientX[0],quotientX[1], quotientX[2], quotientX[3], quotientY[0],quotientY[1], quotientY[2],quotientY[3]);
					} else {
						quotientY[0] = (int)floor((double)EgridY * 0.245);//244.8
						quotientY[1] = (int)ceil((double)EgridY * 0.225);//234.6
						quotientY[2] = (int)ceil((double)EgridY * 0.23);//234.6
						quotientY[3] = (int)floor((double)EgridY * 0.30);//306
						//printf("[%d] guotientX : %d,%d,%d,%d guotientY: %d,%d,%d,%d\n", i, quotientX[0],quotientX[1], quotientX[2], quotientX[3], quotientY[0],quotientY[1], quotientY[2],quotientY[3]);
					}
				
				
					if(i==0 && j==0) { gridXsize = quotientX[0]; gridYsize = quotientY[0]; logicalVar[1] += quotientY[0]; } 
					else if(i==0 && j!=0) {
						MPI_Send(logicalVar,2, MPI_INT, sendP, Q_FROM_0, MPI_COMM_WORLD);//start row or col num
						MPI_Send(&quotientX[i],1, MPI_INT, sendP, Q_FROM_0, MPI_COMM_WORLD);//x size
						MPI_Send(&quotientY[j],1, MPI_INT, sendP, GRID_Y_RANGE_FROM_0, MPI_COMM_WORLD);//y size(column)
						logicalVar[1] += quotientY[j];
					} else if(i!=0 && j==0) {
						logicalVar[0] += quotientX[i-1];
						logicalVar[1] = 0;
						MPI_Send(logicalVar,2, MPI_INT, sendP, Q_FROM_0, MPI_COMM_WORLD);//start row or col num
						MPI_Send(&quotientX[i],1, MPI_INT, sendP, Q_FROM_0, MPI_COMM_WORLD);//x size
						MPI_Send(&quotientY[j],1, MPI_INT, sendP, GRID_Y_RANGE_FROM_0, MPI_COMM_WORLD);//y size(column)
					} else if(i!=0 && j!=0) {
						logicalVar[1] += quotientY[j-1];
						MPI_Send(logicalVar,2, MPI_INT, sendP, Q_FROM_0, MPI_COMM_WORLD);//start row or col num
						MPI_Send(&quotientX[i],1, MPI_INT, sendP, Q_FROM_0, MPI_COMM_WORLD);//x size
						MPI_Send(&quotientY[j],1, MPI_INT, sendP, GRID_Y_RANGE_FROM_0, MPI_COMM_WORLD);//y size(column)
					}
					//printf("[%d] - logical[0] %d, logical[1] %d, gridXsize %d, gridYsize %d\n", j, logicalVar[0], logicalVar[1], gridXsize, gridYsize);
					sendP++;
				}
				
			}
			
		}
		
		logicalVar[0]=0, logicalVar[1]=0;
		
		
	} else {
		while(!feof(fpdata)){
			fscanf(fpdata, "%lf %lf %lf", &x[i], &y[i], &z[i]);
			//fprintf(ferr,"data: %lf %lf %lf\n", x[i], y[i], z[i]);
			i++;
		}
		
		MPI_Recv(logicalVar,2, MPI_INT, 0, Q_FROM_0, MPI_COMM_WORLD, &status);
		MPI_Recv(&gridXsize,1, MPI_INT, 0, Q_FROM_0, MPI_COMM_WORLD, &status);
		MPI_Recv(&gridYsize,1, MPI_INT, 0, GRID_Y_RANGE_FROM_0, MPI_COMM_WORLD, &status);
		
	}
	//printf("[%d] - logical[0] %d, logical[1] %d, gridXsize %d, gridYsize %d at [%s]\n", rankId, logicalVar[0], logicalVar[1], gridXsize, gridYsize, hostname);
	endfileTIme = MPI_Wtime() - fileTime;
	fclose(fpdata);
	
	MPI_Bcast(&gminX, 1, MPI_INT, 0, MPI_COMM_WORLD);
	MPI_Bcast(&gminY, 1, MPI_INT, 0, MPI_COMM_WORLD);
	//MPI_Bcast(&maxDistance, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
		
	FOR_DEBUG_PRINT("[%d] - gminX %d, gminY %d at [%s]\n", rankId, gminX, gminY, hostname);
		
	if(rankId/4 ==0 | rankId/4 ==3) {
		alphaGridX = gridXsize+gridRadius;
	} else {
		alphaGridX = gridXsize+gridRadius+gridRadius;
	}
	if(rankId%4 ==0 | rankId%4 ==3) {
		alphaGridY = gridYsize+gridRadius;
	} else {
		alphaGridY = gridYsize+gridRadius+gridRadius;
	}
	interval = (Pointer**)malloc(sizeof(Pointer*)*(alphaGridX));
	
	FOR_CONFIRM_PRINT("malloc interval %d ...\n", rankId);
	for(i=0;i<alphaGridX;i++){
		interval[i] = (Pointer*)malloc(sizeof(Pointer)*alphaGridY);
		
		for(j=0;j<alphaGridY;j++){
			interval[i][j].next=NULL;
		}
		//FOR_DEBUG_PRINT("row_num:%d\n",i);FOR_DEBUG_PRINT("Load grid on memory\n");
	}
	FOR_DEBUG_PRINT("[%d]: X size %d- Y size %d, X - start:%d, end:%d\tY - start:%d, end:%d at[%s]\n",rankId, gridXsize, gridYsize, logicalVar[0], logicalVar[0]+gridXsize,logicalVar[1], logicalVar[1]+gridYsize, hostname);
	
	
	//////////////////////////////////////////////////////////////////////
	//																    //
	//					     Setting Grid point 					    //
	//							  										//
	//////////////////////////////////////////////////////////////////////
	
	
	// start *, end * are ordinary grid point 
	if(rankId==0) {
		startX = logicalVar[0]; startY = logicalVar[1]; endX = alphaGridX; endY = alphaGridY;
	} else if( rankId==1 || rankId==2) {
		startX = logicalVar[0]; startY = logicalVar[1]-gridRadius; endX = alphaGridX; endY = logicalVar[1]+alphaGridY-gridRadius;
	} else if(rankId==3) {
		startX = logicalVar[0]; startY = logicalVar[1]-gridRadius; endX = alphaGridX; endY = logicalVar[1]+gridYsize;
	} 
	
	else if(rankId==4 || rankId==8) {
		startX = logicalVar[0]-gridRadius; startY = logicalVar[1]; endX =logicalVar[0]+alphaGridX-gridRadius; endY = alphaGridY;
	} else if( rankId==5 || rankId==6 || rankId==9 || rankId==10) {
		startX = logicalVar[0]-gridRadius; startY = logicalVar[1]-gridRadius; endX =logicalVar[0]+alphaGridX-gridRadius; endY = logicalVar[1]+alphaGridY-gridRadius;
	} else if(rankId==7 || rankId==11) {
		startX = logicalVar[0]-gridRadius; startY = logicalVar[1]-gridRadius; endX =logicalVar[0]+alphaGridX-gridRadius; endY = logicalVar[1]+gridYsize;
	} 
	
	else if(rankId==12) {
		startX = logicalVar[0]-gridRadius; startY = logicalVar[1]; endX = logicalVar[0]+gridXsize; endY = alphaGridY;
	} else if( rankId==13 || rankId==14) {
		startX = logicalVar[0]-gridRadius; startY = logicalVar[1]-gridRadius; endX = logicalVar[0]+gridXsize; endY = logicalVar[1]+alphaGridY-gridRadius;
	} else if(rankId==15) {
		startX = logicalVar[0]-gridRadius; startY = logicalVar[1]-gridRadius; endX = logicalVar[0]+gridXsize; endY = logicalVar[1]+gridYsize;
	}
	
	Gridsqure = gridXsize*gridYsize;
	FOR_DEBUG_PRINT("[%d]: X - start:%d, end:%d\tY - start:%d, end:%d at[%s]\n",rankId, startX, endX, startY, endY, hostname);
	
	for(i=0;i<dsize;i++){
		gridX = (int)(x[i]-gminX)/GRID_SIZE;
		gridY = (int)(y[i]-gminY)/GRID_SIZE;
		if(rankId==0) {	XofGrid = gridX-logicalVar[0]; YofGrid = gridY-logicalVar[1]; } 
		else if(rankId==1 | rankId==2) { XofGrid = gridX-logicalVar[0]; YofGrid=gridY-logicalVar[1]+gridRadius; } 
		else if(rankId==3) { XofGrid = gridX-logicalVar[0]; YofGrid=gridY-logicalVar[1]+gridRadius; } 
		
		else if(rankId==4 || rankId==8 || rankId==12) { XofGrid = gridX-logicalVar[0]+gridRadius; YofGrid = gridY-logicalVar[1]; } 
		else { XofGrid = gridX-logicalVar[0]+gridRadius; YofGrid=gridY-logicalVar[1]+gridRadius; } 
		/*else if(rankId==4) { XofGrid = gridX-logicalVar[0]+gridRadius; YofGrid = gridY-logicalVar[1]; } 
		else if(rankId==5 | rankId==6) { XofGrid = gridX-logicalVar[0]+gridRadius; YofGrid=gridY-logicalVar[1]+gridRadius; } 
		else if(rankId==7) { XofGrid = gridX-logicalVar[0]+gridRadius; YofGrid=gridY-logicalVar[1]+gridRadius; } 
		
		else if(rankId==8) { XofGrid = gridX-logicalVar[0]+gridRadius; YofGrid = gridY-logicalVar[1]; } 
		else if(rankId==9 | rankId==10) { XofGrid = gridX-logicalVar[0]+gridRadius; YofGrid=gridY-logicalVar[1]+gridRadius; } 
		else if(rankId==11) { XofGrid = gridX-logicalVar[0]+gridRadius; YofGrid = gridY-logicalVar[1]+gridRadius; }*/
		//Plus logicalVar because it just start 0
		
		if(startX <= gridX && gridX< endX && startY <= gridY && gridY< endY){
			FOR_CONFIRM_PRINT("%d [%lf,%lf] gridX %d, gridY %d, XofGrid %d YofGrid %d\n", rankId, x[i],y[i],gridX, gridY, XofGrid, YofGrid);
			//printf ("gridX, gridY : %d %d \n", gridX, gridY);
			//FOR_CONFIRM_PRINT("[%d][%d]\n",XofGrid,YofGrid);
			//To prevent out of bound, minus logicalVar
			//FOR_CONFIRM_PRINT("before cur\n");
			cur = interval[XofGrid][YofGrid].next;
			
			if(cur==NULL){
				FOR_CONFIRM_PRINT ("cur is null \n");
				new_list = (Info_Grid*)malloc(sizeof(Info_Grid));
				new_list->x = x[i];
				new_list->y = y[i];
				new_list->z = z[i];
				new_list->next=NULL;
				
				interval[XofGrid][YofGrid].next = new_list;
				//fprintf(ferr,"%dth::null\n",i);
				//startGrid->x = x[i];
				//startGrid->y = y[i];
			} else {
			//	fprintf(ferr,"%dth::\n",i);
				//FOR_CONFIRM_PRINT ("cur is not null \n");
				FOR_CONFIRM_PRINT ("cur next : %d\n" , cur->next);
				while(cur->next!=NULL) {
					cur = cur->next;
				}
				
				new_list = (Info_Grid*)malloc(sizeof(Info_Grid));
				new_list->x = x[i];
				new_list->y = y[i];
				new_list->z = z[i];
				
				new_list->next=NULL;
				cur->next = new_list;
				
			}
		}
		
	}
	// For Debugging
	
	/*strcat(path,itoa(rankId,10));
	strcat(path,fileType);
	file_out = fopen(path,"w");
	for(i=0;i<gridXsize+gridRadius;i++) {
		for(j=0;j<gridYsize+gridRadius;j++) {
			fprintf(file_out,"Grid [%d][%d]\n",i,j);
			cur = interval[i][j].next;
			if(cur==NULL)
				continue;//FOR_DEBUG_PRINT("This Grid is empty");
			while(cur!=NULL) {
				fprintf(file_out,"x:%lf, y:%lf", cur->x, cur->y);
				cur = cur->next;
			}
			fprintf(file_out,"\n");
		}
		fprintf(file_out,"\n");
	}	*/
	
	
	realXpoint = logicalVar[0]+gminX;
	realYpoint = logicalVar[1]+gminY;
	
	if(rankId==0) {
		i=0; NNendX=gridXsize; NNendY=gridYsize;
	} else if(rankId==1 || rankId==2 || rankId==3) {
		i=0; NNendX=gridXsize; NNendY=gridYsize+gridRadius;
	} else if(rankId==4 || rankId==8|| rankId==12) {
		i=gridRadius; NNendX=gridXsize+gridRadius; NNendY=gridYsize;
	} else {
		i=gridRadius; NNendX=gridXsize+gridRadius; NNendY=gridYsize+gridRadius;
	}
	
	//printf("[%d], endX x: %d, NNendX x: %d, endY y: %d NNendY y: %d\t gridXsize : %d, gridYsize: %d at[%s]\n", rankId, endX, NNendX, endY, NNendY,gridXsize, gridYsize, hostname);
	
	
	//////////////////////////////////////////////////////////////////////
	//																    //
	//			Gridding, Semi-Variogram, Prediction				    //
	//							  										//
	//////////////////////////////////////////////////////////////////////
	
	int calPointX, calPointY;
	int filePointX, filePointY;
	int buf_num=0;
	strcat(path_p,itoa(rankId,10));
	strcat(path_p,fileType);
	strcat(path,itoa(rankId,10));
	strcat(path,fileType);
	file_out = fopen(path,"w");
	fPrediction = fopen(path_p,"w");
	//if(rankId==8){
	for(i;i<NNendX;i++){
		printf("Progress %.2lf%% at %d\n", (progrssCnt/Gridsqure)*100, rankId);
		if(rankId==0) {j=0; calPointX=i+realXpoint; filePointX=i+logicalVar[0];} 
		else if(rankId==1 || rankId==2 || rankId==3) {j=gridRadius; calPointX=i+realXpoint; filePointX=i+logicalVar[0];} 
		else if(rankId==4 || rankId==8 || rankId==12) {j=0; calPointX=i+realXpoint-gridRadius; filePointX=i+logicalVar[0]-gridRadius;} 
		else {j=gridRadius; calPointX=i+realXpoint-gridRadius; filePointX=i+logicalVar[0]-gridRadius;}

		
		for(j;j<NNendY;j++) {
			if(rankId==0 || rankId==4 || rankId==8 || rankId==12) {filePointY=j+logicalVar[1]; calPointY=j+realYpoint; } 
			else  {filePointY=j+logicalVar[1]-gridRadius; calPointY=j+realYpoint-gridRadius; }
			progrssCnt++;
			
			//fprintf(file_out,"[%d][%d] \n", filePointX, filePointY);	
			
			//////////////////////////////////////
			//			  Gridding			    //
			//////////////////////////////////////
			
			stGtime = MPI_Wtime();
			radX=i-gridRadius+1;
			minX=0.0, minY=0.0, maxX=0.0, maxY=0.0;
			int pCnt=0;
			
			FOR_DEBUG_PRINT("Gridding start\t");
			for(k=0;k<2*gridRadius-1;k++){
				radY=j-gridRadius+1;
				for(p=0;p<2*gridRadius-1;p++){
												
					if(radX>=0 && radX < alphaGridX && radY>=0 && radY < alphaGridY) {
							CalDistPointfromGrid(calPointX,calPointY,radX,radY, &pCnt);
					}
					
					radY++;
				}
				radX++;
			}
			FOR_DEBUG_PRINT("Gridding end\n");
			endGtime = MPI_Wtime()-stGtime;
			totalGtime = totalGtime+endGtime; 
			//fprintf(file_out,"[%d][%d], %d \n", filePointX, filePointY, pCnt);	
			//fprintf(file_out,"point: %d \n", pCnt);	
			totlaPoints += pCnt;
			
			double coord_gridX = (double)calPointX + (double)GRID_SIZE/2;
			double coord_gridY = (double)calPointY + (double)GRID_SIZE/2;
			//fprintf(file_out,"[%.2lf][%.2lf] \n", coord_gridX, coord_gridY);
			if(pCnt<1) {
				//printf("pCnt = 0\n");
				File_out_countZero(rankId,  coord_gridX,  coord_gridY, fPrediction);
				continue;
			}
			
			//////////////////////////////////////
			//			  Variogram			    //
			//////////////////////////////////////
			
			maxDistance = sqrt(pow(maxX-minX,2)+pow(maxY-minY,2));
			maxDistance = maxDistance/2;
			
			//double* sum_SqurZ, *distDelta;
			//sum_SqurZ = (double*)malloc(sizeof(double)*(nrbins+2));
			//distDelta = (double*)malloc(sizeof(double)*(nrbins+2));
			pCnt = pCnt+1;
			
			FOR_DEBUG_PRINT("ptp start\n");
			ptopDist = (double**)malloc(sizeof(double*)*pCnt);
			for(k=0;k<pCnt;k++) {
				ptopDist[k] = (double*)malloc(sizeof(double)*pCnt);
			}
			for(k=0;k<pCnt;k++) {
				for(p=0;p<pCnt;p++) {
					ptopDist[k][p] = 0.0;
				}
			}
			FOR_DEBUG_PRINT("ptp end\n");
			stSemitime = MPI_Wtime();
				FindSemivar(Head_distfromGrid, nrbins, sum_SqurZ, maxDistance, distDelta, ptopDist);
			endSemitime = MPI_Wtime()-stSemitime;
			totalSemitime += endSemitime;
			//for(k=0;k<nrbins;k++) {
			//		fprintf(fPrediction,"%lf %lf\n", sum_SqurZ[k], distDelta[k]);
			//}
			//////////////////////////////////////
			//			  Modelling			    //
			//////////////////////////////////////
			double range, sill;
			stFitime = MPI_Wtime(); 
			
			FitSemivariogram(sum_SqurZ, distDelta, nrbins, &range, &sill);
			
			endFitime = MPI_Wtime() - stFitime;
			totalFitime += endFitime;

			//free(sum_SqurZ);	free(distDelta);
			//printf("[%d][%d]: %lf\t%lf\n",i,j, range, sill);
			

			//////////////////////////////////////
			//			  Prediction		    //
			//////////////////////////////////////
			
			stPtime = MPI_Wtime(); 
				Prediction(Head_distfromGrid, pCnt, range, sill, rankId, ptopDist, coord_gridX, coord_gridY, fPrediction);
				//Prediction(Head_distfromGrid, pCnt, range, sill, rankId, ptopDist, coord_gridX, coord_gridY, filePointX, filePointY, buffer_grid, buf_num);
			endPtime = MPI_Wtime() - stPtime; 
			totalPtime += endPtime;
			
			
			//////////////////////////////////////
			//	    Memory Free at Grid			//
			//////////////////////////////////////
			
			for(k=0;k<pCnt;k++) {
				free(ptopDist[k]);
			}
			free(ptopDist);
			/**/
			Cur_DistfromGrid = Head_distfromGrid;
			while(Cur_DistfromGrid!=NULL) {
				ForFree_DistfromGrid = Cur_DistfromGrid;
				Cur_DistfromGrid = Cur_DistfromGrid -> next;
				free(ForFree_DistfromGrid);
			}
			Head_distfromGrid=NULL;
			Tail_DistfromGrid=NULL;
			FOR_DEBUG_PRINT("free end\n");
			
			buf_num++;
		}
	}
	//}
	//fclose(file_out);
	//fclose(fPrediction);
	
	/**/
	//////////////////////////////////////////////////////////////////////
	//																    //
	//					    Memory Allocation Free 					    //
	//							  										//
	//////////////////////////////////////////////////////////////////////
	for(i=0;i<alphaGridX;i++){
		//for(j=0;j<alphaGridY;j++) {
		//	cur = interval[i][j].next;
		//	while(cur!=NULL) {
		//		forFreeGrid = cur;
		//		cur = cur->next;
		//		free(forFreeGrid);
		//	}
		 //}
		free(interval[i]);
	}
	free(interval);
	free(x); free(y); free(z);
	
	endTime = MPI_Wtime();
	totalTime = endTime-startTime;
	printf("[%d]\t%lf\t%lf\t%lf\t%lf\t%lf\t%lf\t%d\t[%s]\n", rankId, endfileTIme, totalGtime, totalSemitime, totalFitime, totalPtime, totalTime,totlaPoints, hostname);	
    MPI_Finalize();
	return 0;
}

void CalDistPointfromGrid(int i, int j, int radX, int radY, int* pCnt) {
	cur = interval[radX][radY].next;
	double gridpointX=0, gridpointY=0;
	gridpointX = (double)i+(double)GRID_SIZE/2;
	gridpointY = (double)j+(double)GRID_SIZE/2;
	if(cur==NULL) {
		
	} else {
		FOR_DEBUG_PRINT("gridpoint[%d][%d] around[%d][%d]\n",i,j,radX,radY);
		//FOR_DEBUG_PRINT("x %lf, y %lf\n", (double)i+(double)GRID_SIZE/2,(double)j+(double)GRID_SIZE/2);
		while(cur!=NULL){
			findMinMax(cur->x, cur->y);
			(*pCnt)++;
			if(Head_distfromGrid==NULL) {
				Head_distfromGrid = (Distnode*)malloc(sizeof(struct Dist_Node));
				//Head_distfromGrid -> distance = Eculidean(gridpointX,gridpointY,cur->x, cur->y);
				Head_distfromGrid -> coord = cur;
				Head_distfromGrid -> next = NULL;
				Tail_DistfromGrid = Head_distfromGrid;
			} else {
				Cur_DistfromGrid = (Distnode*)malloc(sizeof(struct Dist_Node));
				//Cur_DistfromGrid -> distance = Eculidean(gridpointX,gridpointY,cur->x, cur->y);
				Cur_DistfromGrid -> coord = cur;
				Cur_DistfromGrid -> next = NULL;
				Tail_DistfromGrid-> next = Cur_DistfromGrid;
				Tail_DistfromGrid = Cur_DistfromGrid;
			}
			cur = cur->next;
			FOR_DEBUG_PRINT("[%2lf][%2lf]distance %lf\n", gridpointX, gridpointY, Tail_DistfromGrid -> distance);
			
		}
		
	}
}
double Eculidean(double gridX, double gridY, double pointX, double pointY) {
	return sqrt ( pow(gridX-pointX,2)+ pow(gridY-pointY,2) );
}

char* itoa(int val, int base) {
		// check that the base if valid
	static char buf[32] = {0};
	int i = 30;
	for(; val && i ; --i, val /= base)
	
		buf[i] = "0123456789abcdef"[val % base];

	return &buf[i+1];
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