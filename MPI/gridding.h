/*
 * DataStruct.h
 *
 *  Created on: Feb 6, 2014
 *      Author: Mata
 */

#ifndef DATASTRUCT_H_
#define DATASTRUCT_H_

typedef struct Info_Grid{
	double x,y,z;
	struct Info_Grid *next;
} Info_Grid;

typedef struct Pointer_Grid{
	struct Info_Grid *next;
} PointerOfGrid;

typedef struct Dist_Node {
	double distance;
	struct Info_Grid *coord;
	struct Dist_Node *next;
} Distnode;
/*
#ifdef DEBUG_GRIDDING_LEVEL1
    #define DEBUG_PRINT1 printf
#else
    #define DEBUG_PRINT1
#endif

#ifdef DEBUG_SEMIVARIOGRAM_LEVEL2
    #define DEBUG_PRINT2 printf
#else
    #define DEBUG_PRINT2
#endif
*/
#endif /* DATASTRUCT_H_ */
