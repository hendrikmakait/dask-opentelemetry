import logging
import os

import distributed
from dask.utils import key_split
from distributed.diagnostics.plugin import WorkerPlugin
from distributed.worker import Worker
from opentelemetry import trace
from opentelemetry.environment_variables import (
    OTEL_METRICS_EXPORTER,
    OTEL_TRACES_EXPORTER,
)
from opentelemetry.sdk._configuration import _OTelSDKConfigurator
from opentelemetry.sdk.environment_variables import (
    OTEL_EXPORTER_OTLP_PROTOCOL,
    OTEL_RESOURCE_ATTRIBUTES,
)
from opentelemetry.semconv.resource import ResourceAttributes

logger = logging.getLogger(__name__)


class TaskTracingPlugin(WorkerPlugin):
    worker: Worker | None

    def __init__(self):
        self._buffer = []

    def setup(self, worker):
        self._worker = worker
        OpenTelemetryConfigurator().configure(
            dask_worker=worker,
            **{
                OTEL_TRACES_EXPORTER: "otlp",
                OTEL_EXPORTER_OTLP_PROTOCOL: "http/protobuf",
            },
        )
        self._tracer = trace.get_tracer(__name__)

    def transition(self, key, start, finish, *args, **kwargs):
        if not (start == "executing" and finish in ("memory", "error")):
            return

        if key not in self._worker.state.tasks:
            return

        ts = self._worker.state.tasks[key]

        if not ts.startstops:
            return

        start_time = min(startstop["start"] for startstop in ts.startstops)

        with self._tracer.start_as_current_span(
            key_split(key), start_time=int(start_time * 1e9), end_on_exit=False
        ) as parent:
            parent.set_attribute("key", key)
            for startstop in ts.startstops:
                span = self._tracer.start_span(
                    startstop["action"], start_time=int(startstop["start"] * 1e9)
                )
                span.end(end_time=int(startstop["stop"] * 1e9))
            stop_time = max(startstop["stop"] for startstop in ts.startstops)
        parent.end(end_time=int(stop_time * 1e9))


class OpenTelemetryConfigurator(_OTelSDKConfigurator):
    def configure(self, dask_worker, **kwargs):
        for k, v in kwargs.items():
            if k.startswith("OTEL_"):
                os.environ[k] = v
        os.environ.setdefault(OTEL_TRACES_EXPORTER, "console")
        os.environ.setdefault(OTEL_METRICS_EXPORTER, "console")
        os.environ.setdefault(
            OTEL_RESOURCE_ATTRIBUTES,
            ",".join(
                [
                    f"{ResourceAttributes.SERVICE_NAME}=worker",
                    f"{ResourceAttributes.SERVICE_INSTANCE_ID}={dask_worker.id}",
                    f"{ResourceAttributes.SERVICE_VERSION}={distributed.__version__}",
                ]
            ),
        )
        return super().configure(**kwargs)
