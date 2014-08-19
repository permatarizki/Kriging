/*
 * DataStruct.h
 *
 *  Created on: Feb 6, 2014
 *      Author: Mata
 */

#ifndef PREDICTION_H_
#define PREDICTION_H_

struct Distnode {
	double distance;
	struct LIDARnodeGrid *coord;
	struct Distnode *next;
};

struct LIDARnodeGrid {
	double X,Y,Z;
	struct LIDARnodeGrid *next;
};

#ifdef DEBUG_PREDICTION_LEVEL1
    #define DEBUG_PREDICTION_PRINT1 printf
#else
    #define DEBUG_PREDICTION_PRINT1
#endif

#ifdef DEBUG_PREDICTION_LEVEL2
    #define DEBUG_PREDICTION_PRINT2 printf
#else
    #define DEBUG_PREDICTION_PRINT2
#endif

#endif /* PREDICTION_H_ */
