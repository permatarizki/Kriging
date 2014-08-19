TARGETS : cluster_gridding 


MPICC  = mpicc

all : $(TARGETS)

fitvariogram.o : fitvariogram.c
	$(MPICC) -O2 -c fitvariogram.c

semivariogram.o : semivariogram.c
	$(MPICC) -O2 -c semivariogram.c

prediction.o : prediction.c
	$(MPICC) -O2 -c prediction.c

doolittle_pivot.o : doolittle_pivot.c
	$(MPICC) -O2 -c doolittle_pivot.c

cluster_gridding : gridding.c gridding.h fitvariogram.o prediction.o semivariogram.o doolittle_pivot.o
	$(MPICC) -O2 gridding.c  gridding.h -o cluster_gridding \fitvariogram.o \prediction.o \semivariogram.o \doolittle_pivot.o
