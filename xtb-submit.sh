#! /bin/bash
#COBALT -A CSC249ADCD08 --attrs enable_ssh=1

# Load up the Python environment
#module load miniconda-3/latest
#conda activate ../../env

# Add NWChem and Psi4 to the path
#export PATH=~/software/psi4/bin:$PATH
#export OMP_NUM_THREADS=64
#export OMP_NUM_THREADS=4
export OMP_NUM_THREADS=2
export TF_NUM_INTRAOP_THREADS=2
export TF_NUM_INTEROP_THREADS=2



export KMP_INIT_AT_FORK=FALSE

mkdir -p scratch  # For the NWChem tasks
hostname
#module load atp
export MPICH_GNI_MAX_EAGER_MSG_SIZE=16384
export MPICH_GNI_MAX_VSHORT_MSG_SIZE=10000
export MPICH_GNI_MAX_EAGER_MSG_SIZE=131072
export MPICH_GNI_NUM_BUFS=300
export MPICH_GNI_NDREG_MAXSIZE=16777216
export MPICH_GNI_MBOX_PLACEMENT=nic
export MPICH_GNI_LMT_PATH=disabled
export COMEX_MAX_NB_OUTSTANDING=6
#export LD_LIBRARY_PATH=/opt/intel/compilers_and_libraries_2018.0.128/linux/compiler/lib/intel64_lin:$LD_LIBRARY_PATH

#- Start the redis-server
port=19${RANDOM::3}
./python_package_run -u /dev/shm/colmena -e env.tar.gz \
redis-server --port $port &> redis.out &
redis=$!

# Run!
./python_package_run -u /dev/shm/colmena -e env.tar.gz \
./xtb-run.sh --redisport $port $@

# Kill the redis server
kill $redis
