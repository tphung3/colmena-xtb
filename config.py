"""Functions to generate configurations"""
import os

from parsl import HighThroughputExecutor, WorkQueueExecutor
from parsl.addresses import address_by_hostname
from parsl.config import Config
from parsl.launchers import AprunLauncher, SimpleLauncher, WrappedLauncher
from parsl.providers import LocalProvider, CobaltProvider, CondorProvider, GridEngineProvider
from parsl.channels import SSHChannel
from parsl.monitoring import MonitoringHub


def theta_nwchem_config(log_dir: str, nodes_per_nwchem: int = 2, total_nodes: int = 4,
                        ml_prefetch: int = 0) -> Config:
    """Theta configuration where QC workers sit on the launch node (to be able to aprun)
    and ML workers are placed on compute nodes

    Args:
        ml_workers: Number of nodes dedicated to ML tasks
        nodes_per_nwchem: Number of nodes per NWChem computation
        log_dir: Path to store monitoring DB and parsl logs
        total_nodes: Total number of nodes available. Default: COBALT_JOBSIZE
        ml_prefetch: Number of tasks for ML workers to prefect
    Returns:
        (Config) Parsl configuration
    """

    return Config(
        executors=[
            WorkQueueExecutor(
                	label="ml",
			project_name="colmena-nwc",
			port=50055,
			shared_fs=True,
			provider=LocalProvider(
				init_blocks=0,
				max_blocks=0    # stop parsl from creating workers for us
				)
                	)
        ],
        monitoring=MonitoringHub(
            hub_address=address_by_hostname(),
            monitoring_debug=False,
            resource_monitoring_interval=10,
            logdir=log_dir,
            logging_endpoint=f'sqlite:///{os.path.join(log_dir, "monitoring.db")}'
        ),
        run_dir=log_dir,
        strategy='simple',
        max_idletime=15.
    )


def theta_xtb_config(log_dir: str, xtb_per_node: int = 1,
                     ml_tasks_per_node: int = 1, total_nodes: int = int(os.environ.get("COBALT_JOBSIZE", 1))):
    """Theta configuration where QC tasks and ML tasks run on single nodes.

    There are no MPI tasks in this configuration.

    Args:
        ml_workers: Number of nodes dedicated to ML tasks
        xtb_per_node: Number of XTB calculations
        ml_tasks_per_node: Number of ML tasks to place on each node
        log_dir: Path to store monitoring DB and parsl logs
        total_nodes: Total number of nodes available. Default: COBALT_JOBSIZE
    Returns:
        (Config) Parsl configuration
    """

    return Config(
        executors=[
            WorkQueueExecutor(
                	label="ml",
			project_name="colmena-xtb-v3",
			port=50055,
			shared_fs=True,
			autolabel=True,
			autocategory=True,
		    provider=LocalProvider(
				init_blocks=0,
				max_blocks=0    # stop parsl from creating workers for us
				)
                	)

        ],
        monitoring=MonitoringHub(
            hub_address=address_by_hostname(),
            monitoring_debug=False,
            resource_monitoring_interval=10,
            logdir=log_dir,
            logging_endpoint=f'sqlite:///{os.path.join(log_dir, "monitoring.db")}'
        ),
        run_dir=log_dir,
        strategy='simple',
        max_idletime=15.
    )
