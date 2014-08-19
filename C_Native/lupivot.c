#include <stdio.h>
#include <math.h>

void LU_de(double (*u)[4], double (*l)[4], double B[], int n);
void U_copy(double (*u)[4], double (*a)[4], int n);
void Print_1(double a[], int n);
void Print_2(double (*a)[4], int n);
void chan(double (*a)[4], double b[], double y[], int n);
void u_chan(double (*a)[4], double y[], double x[], int n);

int main() {
  double A[4][4] = {
    {0,0.5,0.5,-1},
    {0.5,0,0.5,-1},
    {0.5,0.5,0,-1},
    {1,1,1,0}
  };
  double L[4][4] = {
    {1,0,0,0},
    {0,1,0,0},
    {0,0,1,0},
    {0,0,0,1}
  }; 
//단위행렬부터시작
  double U[4][4];
  double B[4] = {0.5,0.5,0.5,1};
  double Y[4] = {0,0,0,0};
  double X[4] = {0,0,0,0};

  U_copy(U, A, 4);

  printf("- A 입력값 -\n");
  Print_2(A, 4);
  printf("\n");

  printf("- B 입력값 -\n");
  Print_1(B, 4);
  printf("\n");

  printf("- L 시작 -\n");
  Print_2(L, 4);
  printf("\n");

  printf("- U 시작 -\n");
  Print_2(U, 4);
  printf("\n");

  printf("-------------------------------------------------------- \n");

  LU_de(U, L, B, 4);

  printf("- L 결과 - \n");
  Print_2(L, 4);
  printf("\n");

  printf("- U 결과 - \n");
  Print_2(U, 4);
  printf("\n");

  printf("- B 결과 -\n");
  Print_1(B, 4);
  printf("\n");

  chan(L, B, Y, 4);
  printf("- Y 결과 - \n");
  Print_1(Y, 4);
  printf("\n");

  printf("- X 결과 - \n");
  u_chan(U, Y, X, 4);
  Print_1(X, 4);

  return 0;
}

//LU소거법
void LU_de(double (*u)[4], double (*l)[4], double b[], int n) {
  int i,j,k,p,q;
  double temp = 0 ,max;

  for(i= 0 ; i<n-1 ; i++) {
//pivot
    max = fabs(u[i][i]);
    for(p=i ; p<n ; p++) {
      if(fabs(u[p][i]) > max) {
        max = fabs(u[p][i]);
        q = p;
      }
    }
    printf ("max : %lf \n", max);
    if(fabs(u[i][i]) != max) {
//U를 피봇
      for(p=0; p<n ; p++) {
        temp = u[i][p];
        u[i][p] = u[q][p];
        u[q][p] = temp;
      }

//L을 피봇
      for(p=0;p<i;p++){
        temp = l[i][p];
        l[i][p] = l[q][p];
        l[q][p] = temp;
      }

//B를 피봇
      temp = b[i];
      b[i] = b[q];
      b[q] = temp;
    }
    printf ("u[i][i] : %lf\n", u[i][i]);
//소거법
    for(j=i+1 ; j<n ; j++) {
      if (u[i][i] == 0.0)
        temp = 0.0;
      else
        temp = u[j][i]/u[i][i];
      l[j][i] = temp;
      for(k=0 ; k<n; k++) {
        u[j][k] = u[j][k] - (u[i][k]*temp);
      }
    }
  }
}

//정치환 함수
void chan(double (*a)[4], double b[], double y[], int n) {
  int i,j;
  double sum;
  for(i=0 ; i<n ; i++) {
    sum =0;
    for(j=0;j<i;j++){
      sum += a[i][j]*y[j];
    }
    y[i] = (b[i]-sum)/a[i][i];
  }
}

//후치환 함수
void u_chan(double (*a)[4], double y[], double x[], int n) {
 int i,j;
 double sum;
 for(i=n-1 ; i>=0 ; i--) {
  sum =0;
  for(j=n-1 ; j>i ; j--){
    sum += a[i][j]*x[j];
   }
  x[i] = (y[i]-sum)/a[i][i];
 }
}


//U복사함수
void U_copy(double (*u)[4], double (*a)[4], int n) {
  int i,j;

  for(i=0 ; i<n ; i++) {
    for(j=0 ; j<n ; j++)
      u[i][j] = a[i][j];
  }
}

//출력함수
void Print_1(double a[], int n) {
  int i;
  for(i=0; i<n ; i++)
    printf("%9.4lf ", a[i]);
  printf("\n");
}
void Print_2(double (*a)[4], int n) {
  int i,j;
  for(i=0 ; i<n ; i++) {
    for(j=0 ; j<n ; j++) {
      printf("%9.4lf ", a[i][j]);
    }
    printf("\n");
  }
}