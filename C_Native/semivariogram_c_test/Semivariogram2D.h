/*
 * DataStruct.h
 *
 *  Created on: Feb 6, 2014
 *      Author: Mata
 */

#ifndef DATASTRUCT_H_
#define DATASTRUCT_H_

typedef enum {
	false,
	true
}bool;

struct LIDARnode {
	double X,Y,Z;
	struct LIDARnode *next;
};

struct LIDARPairDistanceNode {
	double X1,Y1,Z1;
	double X2,Y2,Z2;
	double distance;
	double SqurZ;
	struct LIDARPairDistanceNode *next;
	int idx_distBin;
};

#ifdef DEBUG_SEMIVARIOGRAM_LEVEL1
    #define DEBUG_PRINT1 printf
#else
    #define DEBUG_PRINT1
#endif

#ifdef DEBUG_SEMIVARIOGRAM_LEVEL2
    #define DEBUG_PRINT2 printf
#else
    #define DEBUG_PRINT2
#endif

#endif /* DATASTRUCT_H_ */
