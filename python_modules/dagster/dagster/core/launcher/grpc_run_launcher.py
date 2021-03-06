import threading
import time
import weakref

from dagster import check
from dagster.api.execute_run import sync_execute_run_grpc
from dagster.core.errors import DagsterLaunchFailedError
from dagster.core.host_representation import ExternalPipeline
from dagster.core.host_representation.handle import (
    GrpcServerRepositoryLocationHandle,
    ManagedGrpcPythonEnvRepositoryLocationHandle,
)
from dagster.core.instance import DagsterInstance
from dagster.core.instance.ref import InstanceRef
from dagster.core.origin import PipelinePythonOrigin
from dagster.core.storage.pipeline_run import PipelineRun
from dagster.grpc.server import GrpcServerProcess
from dagster.grpc.types import (
    CanCancelExecutionRequest,
    CancelExecutionRequest,
    ExecuteRunArgs,
    LoadableTargetOrigin,
)
from dagster.serdes import ConfigurableClass
from dagster.seven import multiprocessing

from .base import RunLauncher

SUBPROCESS_TICK = 0.5

GRPC_REPOSITORY_LOCATION_HANDLE_TYPES = (
    GrpcServerRepositoryLocationHandle,
    ManagedGrpcPythonEnvRepositoryLocationHandle,
)


def _ephemeral_launched_run_client(
    instance_ref, pipeline_origin, pipeline_run_id, cancellation_event
):
    '''Spins up an ephemeral client & server with two workers. This is to allow for cancellation
    to be processed as an interrupt rather than waiting for the launched run to complete.'''
    check.inst_param(instance_ref, 'instance_ref', InstanceRef)
    check.inst_param(pipeline_origin, 'pipeline_origin', PipelinePythonOrigin)
    check.str_param(pipeline_run_id, 'pipeline_run_id')
    check.inst_param(cancellation_event, 'cancellation_event', multiprocessing.synchronize.Event)

    instance = DagsterInstance.from_ref(instance_ref)
    pipeline_run = instance.get_run_by_id(pipeline_run_id)

    loadable_target_origin = LoadableTargetOrigin.from_python_origin(
        pipeline_origin.repository_origin
    )

    with GrpcServerProcess(loadable_target_origin, max_workers=2) as server_process:
        api_client = server_process.create_ephemeral_client()

        execute_run_thread = threading.Thread(
            target=sync_execute_run_grpc,
            kwargs={
                'api_client': api_client,
                'instance_ref': instance_ref,
                'pipeline_origin': pipeline_origin,
                'pipeline_run': pipeline_run,
            },
        )

        execute_run_thread.start()
        while execute_run_thread.is_alive():
            if cancellation_event.is_set():
                api_client.cancel_execution(CancelExecutionRequest(run_id=pipeline_run_id))
                execute_run_thread.join()
            time.sleep(SUBPROCESS_TICK)


class GrpcRunLauncher(RunLauncher, ConfigurableClass):
    '''Launches runs against running GRPC servers.

    During the transition period from the previous CLI-based user process strategy to GRPC, you
    should use the :py:class`dagster.DefaultRunLauncher`, which is aware of instance- and
    repository-level settings allowing it to switch between the two strategies.
    '''

    def __init__(self, inst_data=None):
        self._instance_weakref = None
        self._inst_data = inst_data
        # Conceivably this should be a weakref.WeakValueDictionary, but this is the safest way to
        # get the semantics we want for termination
        self._run_id_to_repository_location_handle_cache = {}

    @property
    def inst_data(self):
        return self._inst_data

    @classmethod
    def config_type(cls):
        return {}

    @staticmethod
    def from_config_value(inst_data, config_value):
        return GrpcRunLauncher(inst_data=inst_data)

    @property
    def _instance(self):
        return self._instance_weakref() if self._instance_weakref else None

    def initialize(self, instance):
        check.inst_param(instance, 'instance', DagsterInstance)
        check.invariant(self._instance is None, 'Must only call initialize once')
        # Store a weakref to avoid a circular reference / enable GC
        self._instance_weakref = weakref.ref(instance)

    def launch_run(self, instance, run, external_pipeline):
        '''Subclasses must implement this method.'''

        check.inst_param(run, 'run', PipelineRun)
        check.inst_param(external_pipeline, 'external_pipeline', ExternalPipeline)

        repository_location_handle = external_pipeline.repository_handle.repository_location_handle

        check.inst(
            repository_location_handle,
            GRPC_REPOSITORY_LOCATION_HANDLE_TYPES,
            'GrpcRunLauncher: Can\'t launch runs for pipeline not loaded from a GRPC server',
        )
        res = repository_location_handle.client.start_run(
            ExecuteRunArgs(
                pipeline_origin=external_pipeline.get_origin(),
                pipeline_run_id=run.run_id,
                instance_ref=self._instance.get_ref(),
            )
        )

        if not res.success:
            raise (
                DagsterLaunchFailedError(
                    res.message, serializable_error_info=res.serializable_error_info
                )
            )

        self._run_id_to_repository_location_handle_cache[run.run_id] = repository_location_handle
        return run

    def can_terminate(self, run_id):
        check.str_param(run_id, 'run_id')

        if run_id not in self._run_id_to_repository_location_handle_cache:
            return False

        res = self._run_id_to_repository_location_handle_cache[run_id].client.can_cancel_execution(
            CanCancelExecutionRequest(run_id=run_id)
        )

        return res.can_cancel

    def terminate(self, run_id):
        check.str_param(run_id, 'run_id')

        if run_id not in self._run_id_to_repository_location_handle_cache:
            return False

        res = self._run_id_to_repository_location_handle_cache[run_id].client.cancel_execution(
            CancelExecutionRequest(run_id=run_id)
        )

        return res.success
