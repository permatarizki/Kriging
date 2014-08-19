prediction_0.4_LU : test.o gridding_0.3.o prediction_0.4_LU.o semivariogram2D_0.4.o doolittle_pivot.o
	gcc -Wall -O2 -o prediction_0.4_LU test.o gridding_0.3.o prediction_0.4_LU.o semivariogram2D_0.4.o doolittle_pivot.o -lm

test.o : test.c
	gcc -Wall -O2 -c test.c -lm

gridding_0.3.o : gridding_0.3.c gridding.h
	gcc -Wall -O2 -c gridding_0.3.c -lm

prediction_0.4_LU.o : prediction_0.4_LU.c prediction.h
	gcc -Wall -O2 -c prediction_0.4_LU.c -lm

semivariogram2D_0.4.o : semivariogram2D_0.4.c semivariogram2D.h gridding.h
	gcc -Wall -O2 -c semivariogram2D_0.4.c -lm

doolittle_pivot.o : doolittle_pivot.c
	gcc -Wall -O2 -c doolittle_pivot.c -lm
