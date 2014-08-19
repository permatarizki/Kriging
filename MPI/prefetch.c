#include <limits.h>
#include <stdio.h>
#include <time.h>
#include <malloc.h>

int main ()
{
   int i=0;
   double sum=0;
	FILE *fpdata=NULL;
	fpdata = fopen("/nfs/code/ksy/data/Data4_XYZ_Ground.txt","r");
	clock_t start, end;
	double dsize;
	double time;
	
	double *x, *y,*z;

	dsize = 4517569;
	x = (double*)malloc(sizeof(double)*dsize);
	y = (double*)malloc(sizeof(double)*dsize);
	z = (double*)malloc(sizeof(double)*dsize);

	while(!feof(fpdata)){
		fscanf(fpdata, "%lf %lf %lf", &x[i], &y[i], &z[i]);
		i++;
	}
		
	start = clock();
	for(i=0;i<dsize ;i++) {
	__builtin_prefetch(x+i+1, 0, 0);
	__builtin_prefetch(y+i+1, 0, 0);
	__builtin_prefetch(z+i+1, 0, 0);


	sum = sum + x[i] + y[i] + z[i];
	}
	end = clock();
	time = ((double)end-(double)start) / CLOCKS_PER_SEC;
	printf("%lf in %lf times\n", sum, time);
  return 0;
}