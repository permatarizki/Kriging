/*
 * test.c
 *
 *  Created on: Feb 6, 2014
 *      Author: Mata
 */

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "Semivariogram2D.h"
//#include "DataStruct.h"

int main(){

	printf("Running testing ...\n");

	clock_t current_tick = clock();
	char* path_LidarData = "/home/hduser/c_code/Data3_XYZ_Ground.txt";
	//char* path = "/home/seop/1000.txt";
	//This variables is Semivariogram settings
	int nrbins=50;

	double sum_SqurZ[nrbins+2];
	double distance[nrbins+2];

	calculateSemivariogram(path_LidarData, sum_SqurZ, distance, nrbins);
	double result = (double)(clock() - current_tick) / CLOCKS_PER_SEC;

	printf(".:: finished testing ::. \n");
	printf ("::::cal semivariogram Processing time : %f seconds\n", result);

	fitSemivariogram(sum_SqurZ, distance, nrbins);

	return EXIT_SUCCESS;
}
