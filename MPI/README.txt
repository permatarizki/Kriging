Detail code is located at TURING server  /nfs/code/ksy/test_lu

some informations:          
- gridding.c : Master code. it manage everything as input, output
- execution comment : /nfs/installs/mpich-install/bin/mpiexec -f machines -np 16 ./cluster_gridding

descriptions:
/nfs/installs/mpich-install/bin/mpiexec : where install mpi
-f machines : cluster configuration
-np 16 : number which will execute process
./cluster_gridding : target name