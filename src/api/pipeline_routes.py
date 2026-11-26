"""Pipeline execution endpoints - run phases using settings.yaml directly."""
import asyncio
import contextvars
import io
import json
import logging
import sys
from datetime import datetime
from typing import Dict, Optional, AsyncGenerator, Callable, Any, TextIO

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from pipeline.main_orchestrator import PipelineOrchestrator
from shared.observability import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/pipeline", tags=["pipeline"])


class PhaseResult(BaseModel):
    phase: str
    status: str
    message: Optional[str] = None
    metadata: Dict = {}
    duration_seconds: Optional[float] = None


class PipelineResponse(BaseModel):
    success: bool
    phases: list[PhaseResult]
    total_duration_seconds: float
    error: Optional[str] = None


# SSE event types
class ProgressEvent(BaseModel):
    type: str  # "phase_start", "phase_progress", "phase_complete", "phase_error", "log"
    phase: Optional[str] = None
    segment: Optional[str] = None
    message: Optional[str] = ""  # Make optional with default empty string
    current: Optional[int] = None
    total: Optional[int] = None
    percentage: Optional[float] = None
    level: Optional[str] = "info"  # "info", "success", "warning", "error"
    timestamp: str


async def run_phase_safely(phase_name: str, phase_func: Callable, *args, capture_stdio: bool = False, **kwargs) -> PhaseResult:
    """
    Run a pipeline phase safely, catching all exceptions and logging them.

    Args:
        phase_name: Name of the phase for logging
        phase_func: The phase function to execute
        capture_stdio: If True, capture stdout/stderr (e.g. tqdm bars) and forward to SSE queue
        *args, **kwargs: Arguments to pass to the phase function

    Returns:
        PhaseResult with status and any error message
    """
    import asyncio as _asyncio

    start_time = datetime.now()
    logger.info(f"Starting phase: {phase_name}")

    loop = _asyncio.get_running_loop()
    old_stdout = old_stderr = None
    if capture_stdio:
        old_stdout, old_stderr = _capture_stdio(loop)

    try:
        if asyncio.iscoroutinefunction(phase_func):
            result = await phase_func(*args, **kwargs)
        else:
            # Run blocking sync phase in a thread so the event loop stays free
            # to stream live SSE log lines while the phase runs.
            result = await asyncio.to_thread(phase_func, *args, **kwargs)

        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Phase completed: {phase_name} (duration: {duration:.2f}s)")

        return PhaseResult(
            phase=phase_name,
            status="success",
            message=f"Phase {phase_name} completed successfully",
            metadata={"result": str(result) if result else "none"},
            duration_seconds=duration,
        )

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"Phase failed: {phase_name} after {duration:.2f}s - {type(e).__name__}: {e}")

        return PhaseResult(
            phase=phase_name,
            status="failed",
            message=f"{type(e).__name__}: {e}",
            metadata={"error_type": type(e).__name__},
            duration_seconds=duration,
        )
    finally:
        if capture_stdio:
            _restore_stdio(old_stdout, old_stderr)


@router.post("/run/extraction", response_model=PipelineResponse)
async def run_extraction():
    """Run extraction phase using settings.yaml."""
    orchestrator = PipelineOrchestrator({})
    start_time = datetime.now()
    phases = []

    phase_result = await run_phase_safely("extraction", orchestrator.run_extraction)
    phases.append(phase_result)

    total_duration = (datetime.now() - start_time).total_seconds()
    success = all(p.status == "success" for p in phases)

    return PipelineResponse(
        success=success,
        phases=phases,
        total_duration_seconds=total_duration,
        error=phases[0].message if not success else None,
    )


@router.post("/run/transformation", response_model=PipelineResponse)
async def run_transformation(data_layer: str = "Bronze", date: str = ""):
    """Run transformation phase using settings.yaml. Reads date from settings.yaml if not provided."""
    orchestrator = PipelineOrchestrator({})
    start_time = datetime.now()
    phases = []

    phase_result = await run_phase_safely("transformation", orchestrator.run_transformation, data_layer, date)
    phases.append(phase_result)

    total_duration = (datetime.now() - start_time).total_seconds()
    success = all(p.status == "success" for p in phases)

    return PipelineResponse(
        success=success,
        phases=phases,
        total_duration_seconds=total_duration,
        error=phases[0].message if not success else None,
    )


@router.post("/run/cloud-push", response_model=PipelineResponse)
async def run_cloud_push(data_layer: str = "Bronze", date: str = ""):
    """Run cloud push phase using settings.yaml. Reads date from settings.yaml if not provided."""
    orchestrator = PipelineOrchestrator({})
    start_time = datetime.now()
    phases = []

    phase_result = await run_phase_safely("cloud_push", orchestrator.run_cloud_push, data_layer, date)
    phases.append(phase_result)

    total_duration = (datetime.now() - start_time).total_seconds()
    success = all(p.status == "success" for p in phases)

    return PipelineResponse(
        success=success,
        phases=phases,
        total_duration_seconds=total_duration,
        error=phases[0].message if not success else None,
    )


@router.post("/run/ingestion", response_model=PipelineResponse)
async def run_ingestion(data_layer: str = "Silver", date: str = "", target_db: str = ""):
    """Run ingestion phase using settings.yaml. Reads date from settings.yaml if not provided."""
    orchestrator = PipelineOrchestrator({})
    start_time = datetime.now()
    phases = []

    phase_result = await run_phase_safely("ingestion", orchestrator.run_ingestion, data_layer, date, target_db or None)
    phases.append(phase_result)

    total_duration = (datetime.now() - start_time).total_seconds()
    success = all(p.status == "success" for p in phases)

    return PipelineResponse(
        success=success,
        phases=phases,
        total_duration_seconds=total_duration,
        error=phases[0].message if not success else None,
    )


@router.post("/run/full", response_model=PipelineResponse)
async def run_full_pipeline(
    skip_cloud_push: bool = False,
    skip_ingestion: bool = False,
    target_db: str = ""
):
    """Run full pipeline using settings.yaml. Reads dates from settings.yaml."""
    orchestrator = PipelineOrchestrator({})
    start_time = datetime.now()
    phases = []

    # Phase 1: Extraction
    phase_result = await run_phase_safely("extraction", orchestrator.run_extraction)
    phases.append(phase_result)

    # Phase 2: Bronze Cloud Push
    if not skip_cloud_push:
        phase_result = await run_phase_safely("bronze_cloud_push", orchestrator.run_cloud_push, "Bronze")
        phases.append(phase_result)

    # Phase 3: Transformation
    phase_result = await run_phase_safely("transformation", orchestrator.run_transformation, "Bronze")
    phases.append(phase_result)

    # Phase 4: Silver Cloud Push
    if not skip_cloud_push:
        phase_result = await run_phase_safely("silver_cloud_push", orchestrator.run_cloud_push, "Silver")
        phases.append(phase_result)

    # Phase 5: Ingestion
    if not skip_ingestion:
        phase_result = await run_phase_safely("ingestion", orchestrator.run_ingestion, "Silver", target_db or None)
        phases.append(phase_result)

    total_duration = (datetime.now() - start_time).total_seconds()
    success = all(p.status == "success" for p in phases)

    return PipelineResponse(
        success=success,
        phases=phases,
        total_duration_seconds=total_duration,
        error=next((p.message for p in phases if p.status == "failed"), None),
    )


class RunSelectedRequest(BaseModel):
    phases: list[str] = []
    target_db: str = ""


# In-memory log queues keyed by run_id for live SSE streaming
_run_log_queues: dict[str, asyncio.Queue] = {}
_current_run_id = contextvars.ContextVar("current_run_id", default=None)


class PipelineLogHandler(logging.Handler):
    """Logging handler that forwards log records to the active run's SSE queue.

    Runs on worker threads (phases execute via asyncio.to_thread), so puts are
    scheduled on the event loop via run_coroutine_threadsafe.
    """

    def __init__(self, loop: asyncio.AbstractEventLoop):
        super().__init__()
        self.loop = loop

    def emit(self, record: logging.LogRecord) -> None:
        try:
            run_id = _current_run_id.get()
        except LookupError:
            return
        if not run_id or run_id not in _run_log_queues:
            return
        try:
            msg = self.format(record)
            payload = {
                "type": "log",
                "level": record.levelname.lower(),
                "message": msg,
            }
            asyncio.run_coroutine_threadsafe(
                _run_log_queues[run_id].put(payload), self.loop
            )
        except Exception:
            pass


class StreamCapture:
    """Captures writes to stdout/stderr (e.g. tqdm progress bars) and forwards them
    to the active run's SSE queue. Runs on worker threads, so puts are scheduled
    on the event loop via run_coroutine_threadsafe."""

    def __init__(self, stream: TextIO, loop: asyncio.AbstractEventLoop):
        self._stream = stream
        self._loop = loop
        self._buffer = ""

    def write(self, text: str) -> int:
        if not text:
            return 0
        try:
            run_id = _current_run_id.get()
        except LookupError:
            return self._stream.write(text)
        if not run_id or run_id not in _run_log_queues:
            return self._stream.write(text)
        try:
            self._buffer += text
            while "\n" in self._buffer:
                line, self._buffer = self._buffer.split("\n", 1)
                line = line.strip()
                if line:
                    asyncio.run_coroutine_threadsafe(
                        _run_log_queues[run_id].put(
                            {"type": "log", "level": "info", "message": line}
                        ),
                        self._loop,
                    )
        except Exception:
            pass
        return self._stream.write(text)

    def flush(self) -> None:
        try:
            run_id = _current_run_id.get()
        except LookupError:
            self._stream.flush()
            return
        if run_id and run_id in _run_log_queues:
            try:
                if self._buffer.strip():
                    asyncio.run_coroutine_threadsafe(
                        _run_log_queues[run_id].put(
                            {"type": "log", "level": "info", "message": self._buffer.strip()}
                        ),
                        self._loop,
                    )
                    self._buffer = ""
            except Exception:
                pass
        self._stream.flush()

    def isatty(self) -> bool:
        return False

    def __getattr__(self, name):
        return getattr(self._stream, name)


def _capture_stdio(loop: asyncio.AbstractEventLoop) -> tuple[TextIO, TextIO]:
    """Replace sys.stdout/stderr with StreamCapture so tqdm/print output flows to SSE."""
    old_stdout, old_stderr = sys.stdout, sys.stderr
    sys.stdout = StreamCapture(sys.stdout, loop)
    sys.stderr = StreamCapture(sys.stderr, loop)
    return old_stdout, old_stderr


def _restore_stdio(old_stdout: TextIO, old_stderr: TextIO) -> None:
    sys.stdout = old_stdout
    sys.stderr = old_stderr


@router.post("/run/selected", response_model=PipelineResponse)
async def run_selected_pipeline(data: RunSelectedRequest):
    """Run only the pipeline phases the user selected, in order (non-streaming)."""
    orchestrator = PipelineOrchestrator({})
    start_time = datetime.now()
    phases = []

    phase_map = {
        "extraction": ("extraction", orchestrator.run_extraction, []),
        "cloud_push_bronze": ("bronze_cloud_push", orchestrator.run_cloud_push, ["Bronze"]),
        "transformation": ("transformation", orchestrator.run_transformation, ["Bronze"]),
        "cloud_push_silver": ("silver_cloud_push", orchestrator.run_cloud_push, ["Silver"]),
        "ingestion": ("ingestion", orchestrator.run_ingestion, ["Silver", data.target_db or None]),
    }

    # Fixed execution order regardless of selection order
    phase_order = [
        "extraction",
        "cloud_push_bronze",
        "transformation",
        "cloud_push_silver",
        "ingestion",
    ]

    for phase_key in phase_order:
        if phase_key not in data.phases:
            continue
        phase_name, phase_func, args = phase_map[phase_key]
        phase_result = await run_phase_safely(phase_name, phase_func, *args)
        phases.append(phase_result)

    total_duration = (datetime.now() - start_time).total_seconds()
    success = all(p.status == "success" for p in phases)

    return PipelineResponse(
        success=success,
        phases=phases,
        total_duration_seconds=total_duration,
        error=next((p.message for p in phases if p.status == "failed"), None),
    )


class StreamRunRequest(BaseModel):
    phases: list[str] = []
    target_db: str = ""


@router.post("/run/selected/stream")
async def stream_selected_pipeline(data: StreamRunRequest):
    """SSE stream that runs selected phases and pushes live log lines to the client."""
    import uuid

    run_id = str(uuid.uuid4())
    queue: asyncio.Queue = asyncio.Queue()
    _run_log_queues[run_id] = queue
    _current_run_id.set(run_id)

    _attached_loggers: list[logging.Logger] = []

    async def event_generator() -> AsyncGenerator[str, None]:
        loop = asyncio.get_running_loop()
        # Attach log handler to all active loggers (phase loggers use propagate:false,
        # so a root logger handler alone won't capture them). Handler runs on worker
        # threads (phases execute via asyncio.to_thread), so puts are scheduled on
        # the event loop via run_coroutine_threadsafe.
        handler = PipelineLogHandler(loop)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        for name in list(logging.Logger.manager.loggerDict.keys()):
            lg = logging.getLogger(name)
            if not any(isinstance(h, PipelineLogHandler) for h in lg.handlers):
                lg.addHandler(handler)
                _attached_loggers.append(lg)
        # Also attach to root as fallback
        if not any(isinstance(h, PipelineLogHandler) for h in logging.getLogger().handlers):
            logging.getLogger().addHandler(handler)
            _attached_loggers.append(logging.getLogger())

        orchestrator = PipelineOrchestrator({})
        start_time = datetime.now()

        phase_map = {
            "extraction": ("extraction", orchestrator.run_extraction, []),
            "cloud_push_bronze": ("bronze_cloud_push", orchestrator.run_cloud_push, ["Bronze"]),
            "transformation": ("transformation", orchestrator.run_transformation, ["Bronze"]),
            "cloud_push_silver": ("silver_cloud_push", orchestrator.run_cloud_push, ["Silver"]),
            "ingestion": ("ingestion", orchestrator.run_ingestion, ["Silver", data.target_db or None]),
        }
        phase_order = [
            "extraction",
            "cloud_push_bronze",
            "transformation",
            "cloud_push_silver",
            "ingestion",
        ]

        def sse(event_type: str, payload: dict) -> str:
            return f"event: {event_type}\ndata: {json.dumps(payload)}\n\n"

        # Separate queue for phase events emitted by _run_phases
        phase_events: asyncio.Queue = asyncio.Queue()

        def emit(event_type: str, payload: dict) -> None:
            try:
                phase_events.put_nowait((event_type, payload))
            except Exception:
                pass

        async def phase_runner() -> None:
            phases: list[PhaseResult] = []
            for phase_key in phase_order:
                if phase_key not in data.phases:
                    continue
                phase_name, phase_func, args = phase_map[phase_key]

                emit("phase_start", {"phase": phase_name, "message": f"Starting {phase_name}..."})
                phase_result = await run_phase_safely(phase_name, phase_func, *args, capture_stdio=True)
                phases.append(phase_result)
                emit(
                    "phase_complete",
                    {
                        "phase": phase_name,
                        "status": phase_result.status,
                        "message": phase_result.message,
                        "duration_seconds": phase_result.duration_seconds,
                    },
                )
                if phase_result.status == "failed":
                    emit(
                        "error",
                        {"phase": phase_name, "message": phase_result.message},
                    )
                    break

            total_duration = (datetime.now() - start_time).total_seconds()
            success = all(p.status == "success" for p in phases)
            emit(
                "done",
                {
                    "success": success,
                    "phases": [p.model_dump() for p in phases],
                    "total_duration_seconds": total_duration,
                    "error": next((p.message for p in phases if p.status == "failed"), None),
                },
            )
            # Signal end of phase events
            await phase_events.put(("__end__", {}))

        async def log_drainer() -> AsyncGenerator[str, None]:
            """Yield live log lines from the queue as they arrive."""
            runner = asyncio.create_task(phase_runner())
            while True:
                try:
                    # Wait for either a log line or a phase event
                    log_task = asyncio.create_task(queue.get())
                    event_task = asyncio.create_task(phase_events.get())
                    done, _ = await asyncio.wait(
                        [log_task, event_task], return_when=asyncio.FIRST_COMPLETED
                    )
                    if log_task in done:
                        record = log_task.result()
                        event_task.cancel()
                        yield sse("log", record)
                    else:
                        event_type, payload = event_task.result()
                        log_task.cancel()
                        if event_type == "__end__":
                            yield sse("done", payload)
                            return
                        yield sse(event_type, payload)
                except asyncio.CancelledError:
                    return

        try:
            async for log_event in log_drainer():
                yield log_event
        finally:
            for lg in _attached_loggers:
                lg.handlers = [h for h in lg.handlers if not isinstance(h, PipelineLogHandler)]
            _run_log_queues.pop(run_id, None)
            try:
                _current_run_id.set(None)
            except LookupError:
                pass

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )