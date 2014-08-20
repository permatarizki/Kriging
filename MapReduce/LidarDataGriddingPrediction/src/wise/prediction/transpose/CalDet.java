package wise.prediction.transpose;

import java.util.Scanner;

public class CalDet {

	double A[][];
	double m[][];
	int N;
	
	String n[][];

//	public void input() {
//		Scanner s = new Scanner(System.in);
//		System.out.println("enter dimension of matrix");
//		N = s.nextInt();
//		A = new double[N][];
//		for (int i = 0; i < N; i++) {
//			A[i] = new double[N];
//		}
//
//		System.out.println("enter the elements of matrix");
//		for (int i = 0; i < N; i++) {
//			System.out.println("enter the elements of matrix of equation"
//					+ (i + 1));
//			for (int j = 0; j < N; j++) {
//				int k = s.nextInt();
//				A[i][j] = k;
//			}
//		}
//	}

	public double determinant(double A[][], int N) {
		double det = 0;
		double res;
		if (N == 1)
			res = A[0][0];
		else if (N == 2) {
			res = A[0][0] * A[1][1] - A[1][0] * A[0][1];
		} else {
			res = 0;
			for (int j1 = 0; j1 < N; j1++) {
				m = new double[N - 1][];
				for (int k = 0; k < (N - 1); k++)
					m[k] = new double[N - 1];
				for (int i = 1; i < N; i++) {
					int j2 = 0;
					for (int j = 0; j < N; j++) {
						if (j == j1)
							continue;
						m[i - 1][j2] = A[i][j];
						j2++;
					}
				}
				res += Math.pow(-1.0, 1.0 + j1 + 1.0) * A[0][j1]
						* determinant(m, N - 1);
			}
		}
		return res;
	}
	
	public double determinant(String A[][], int N) {
		double det = 0;
		double res;
		if (N == 1)
			res = Double.parseDouble(A[0][0]);
		else if (N == 2) {
			res = Double.parseDouble(A[0][0]) * Double.parseDouble(A[1][1]) 
					- Double.parseDouble(A[1][0]) * Double.parseDouble(A[0][1]);
		} else {
			res = 0;
			for (int j1 = 0; j1 < N; j1++) {
				n = new String[N - 1][];
				for (int k = 0; k < (N - 1); k++)
					n[k] = new String[N - 1];
				for (int i = 1; i < N; i++) {
					int j2 = 0;
					for (int j = 0; j < N; j++) {
						if (j == j1)
							continue;
						n[i - 1][j2] = A[i][j];
						j2++;
					}
				}
				res += Math.pow(-1.0, 1.0 + j1 + 1.0) * Double.parseDouble(A[0][j1])
						* determinant(n, N - 1);
			}
		}
		return res;
	}
}