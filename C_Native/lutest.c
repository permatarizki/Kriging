/************** LU Decomposition for solving linear equations ***********/
#include <stdlib.h>
#include <stdio.h>
#include <math.h>


int main()
{
    int n,i,k,j,p;
    float a[10][10],l[10][10]={0},u[10][10]={0},sum,b[10],z[10]={0},x[10]={0};

        int q, w, c, d;
    double max=0.0;
    double num, temp3;


    printf ("Enter the order of matrix ! \n");
    scanf ("%d", &n);
    printf ("Enter all coefficients of matrix : \n");
    for(i=1;i<=n;i++)
    {
        printf ("\nRow %d \n",i);
        for(j=1;j<=n;j++)
            scanf ("%f", &a[i][j]);
    }
    printf ("Enter elements of b matrix\n");
    for(i=1;i<=n;i++)
        scanf ("%f", &b[i]);
    //********** LU decomposition *****//
    for(k=1;k<=n;k++)
    {
        printf ("befor Pivoting\n");
        for (c=0 ; c < n+1 ; c++){
            for (d=0 ; d < n+1 ; d++){
                printf ("%lf, ", a[c][d]);
            }
            printf ("\n");
        }
        printf ("\n");

        
        //find max pivot
            max=fabs(a[k][k]);
        for (q=k;q<n+1;q++) {
            num = fabs(a[q][k]);
            //DEBUG_PREDICTION_PRINT4 ("num : %lf\n", num);
            if( num > max ) {
                //DEBUG_PREDICTION_PRINT4("num>max : num: %lf\n", num);
                max = num;
                w = q;
            }
        }
  //더 큰 것이 있다면 변환
        printf ("max : %lf, a[i][i] : %lf\n", max, a[k][k]);
        if(fabs(a[k][k]) != max) {
            for(c=1;c<n+1;c++) {
                temp3 = a[k][c];
                a[k][c] = a[w][c];
                a[w][c] = temp3;
            }

            // DEBUG_PREDICTION_PRINT4("Pivoting 출력 \n");
            // for(o=0; o< n; o++) {
            //  for(u=0; u< n+1; u++ ) {
            //      DEBUG_PREDICTION_PRINT4("%13.4lf\t", a[o][u]);
            //  }
            //  DEBUG_PREDICTION_PRINT4("\n");
            // }
        }

        printf ("after Pivoting\n");
        for (c=0 ; c < n+1 ; c++){
            for (d=0 ; d < n+1 ; d++){
                printf ("%lf, ", a[c][d]);
            }
            printf ("\n");
        }
        printf ("\n");

        u[k][k]=1;
        for(i=k;i<=n;i++)
        {
            sum=0;
            for(p=1;p<=k-1;p++)
                sum+=l[i][p]*u[p][k];
            l[i][k]=a[i][k]-sum;
        }

        for(j=k+1;j<=n;j++)
        {
            sum=0;
            for(p=1;p<=k-1;p++)
                sum+=l[k][p]*u[p][j];
            u[k][j]=(a[k][j]-sum)/l[k][k];
        }
    }
    //******** Displaying LU matrix**********//
    /* 
    cout<<endl<<endl<<"LU matrix is "<<endl;
    for(i=1;i<=n;i++)
    {
        for(j=1;j<=n;j++)
            cout<<l[i][j]<<"  ";
        cout<<endl;
    }
    cout<<endl;
    for(i=1;i<=n;i++)
    {
        for(j=1;j<=n;j++)
            cout<<u[i][j]<<"  ";
        cout<<endl;
    }
    */

    //***** FINDING Z; LZ=b*********//

    for(i=1;i<=n;i++)
    {                                        //forward subtitution method
        sum=0;
        for(p=1;p<i;p++)
        sum+=l[i][p]*z[p];
        z[i]=(b[i]-sum)/l[i][i];
    }
    //********** FINDING X; UX=Z***********//

    for(i=n;i>0;i--)
    {
        sum=0;
        for(p=n;p>i;p--)
            sum+=u[i][p]*x[p];
        x[i]=(z[i]-sum)/u[i][i];
    }

    //*********** DISPLAYING SOLUTION**************//
    printf ("Set of solution is\n");
    for(i=1;i<=n;i++)
        printf ("%f ", x[i]);


    return 0;
}
