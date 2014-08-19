#include <stdio.h>
#include <stdlib.h>
#include <time.h>

int main(){

	double total_time=0;
	clock_t current_tick;
	double result;

	//Grid radius. this is argument	
	int GridRadius = 5;	
	//Line Number. this is argument
	int N = 1186845;
	//int N = 4517569;
	//this is input path	
	char* path_LidarData = "/home/hduser/c_code/Data3_XYZ_Ground.txt";
	//char* path_LidarData= "/home/seop/git-repo/lidar-project/input/Data3_XYZ_Ground.txt";

	//int N = 1000;
	//this variables is Semivariogram settings
	int nrbins=50;

	//---GRIDDING
	printf("Running gridding->(semivariogram)->prediction ...\n");
	current_tick = clock();

	//Grid Size. this is argument
	int GridSz = 1;
	//Nearest points. this is argument, but it is NOT used now.
	int numofNearestPoint=30;

	gridding(path_LidarData, N, GridSz, numofNearestPoint, GridRadius, nrbins);

	result = (double)(clock() - current_tick) / CLOCKS_PER_SEC;
	printf("finished \n");
	printf ("::::cal Processing time : %f seconds\n", result);
	total_time += result;

	return EXIT_SUCCESS;
}
