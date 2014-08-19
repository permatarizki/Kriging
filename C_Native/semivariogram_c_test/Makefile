Semivariogram2D : test.o Semivariogram2D.o Semivariogram2DFit.o
	gcc -g3 -Wall -o Semivariogram2D test.o Semivariogram2D.o Semivariogram2DFit.o -lm

test.o : test.c 
	gcc -g3 -Wall -c test.c -lm

Semivariogram2D.o : Semivariogram2D.c
	gcc -g3 -Wall -c Semivariogram2D.c -lm

Semivariogram2DFit.o : Semivariogram2DFit.c Semivariogram2DFit.h
	gcc -g3 -Wall -c Semivariogram2DFit.c -lm
