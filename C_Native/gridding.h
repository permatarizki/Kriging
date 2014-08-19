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

struct LIDARnodeGridPointer {
	struct LIDARnodeGrid *next;
};

struct LIDARnodeGrid {
	double X,Y,Z;
	struct LIDARnodeGrid *next;
};

struct Distnode {
	double distance;
	struct LIDARnodeGrid *coord;
	struct Distnode *next;
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
