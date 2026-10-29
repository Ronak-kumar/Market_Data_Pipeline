"""Pipeline execution endpoints - run phases using settings.yaml directly."""
import asyncio
import json
from datetime import datetime
from typing import Dict, Optional, AsyncGenerator

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


def run_phase_safely(phase_name: str, phase_func, *args, **kwargs) -> PhaseResult:
    """Run a pipeline phase and return structured result."""
    started_at = datetime.now()
    try:
        result = phase_func(*args, **kwargs)
        completed_at = datetime.now()
        duration = (completed_at - started_at).total_seconds()
        return PhaseResult(
            phase=phase_name,
            status="success",
            message=f"{phase_name} completed successfully",
            metadata=result if isinstance(result, dict) else {"result": str(result)},
            duration_seconds=duration,
        )
    except Exception as e:
        completed_at = datetime.now()
        duration = (completed_at - started_at).total_seconds()
        logger.error(f"{phase_name} failed: {e}")
        return PhaseResult(
            phase=phase_name,
            status="failed",
            message=str(e),
            metadata={},
            duration_seconds=duration,
        )


async def stream_progress(phase_name: str, phase_func, *args, **kwargs) -> AsyncGenerator[str, None]:
    """Stream progress events for a pipeline phase via SSE."""
    started_at = datetime.now()
    
    # Send phase start event
    yield f"data: {json.dumps(ProgressEvent(type='phase_start', phase=phase_name, message=f'Starting {phase_name}...', timestamp=datetime.now().isoformat()).model_dump())}\n\n"
    
    try:
        loop = asyncio.get_event_loop()
        progress_queue = asyncio.Queue()
        
        def progress_callback(event_type: str, **data):
            """Callback to send progress updates from sync code."""
            try:
                progress_queue.put_nowait((event_type, data))
            except Exception:
                pass
        
        def run_with_progress():
            """Run phase with progress callback injected via kwargs if supported."""
            try:
                # Try to inject progress_callback if the function supports it
                import inspect
                sig = inspect.signature(phase_func)
                if 'progress_callback' in sig.parameters:
                    result = phase_func(*args, progress_callback=progress_callback, **kwargs)
                else:
                    result = phase_func(*args, **kwargs)
                return ("success", result)
            except Exception as e:
                return ("error", str(e))
        
        # Send initial log
        yield f"data: {json.dumps(ProgressEvent(type='log', phase=phase_name, message=f'{phase_name} phase initiated', level='info', timestamp=datetime.now().isoformat()).model_dump())}\n\n"
        
        # Run the phase in executor
        future = loop.run_in_executor(None, run_with_progress)
        
        # Poll for progress updates while waiting
        last_progress_time = datetime.now()
        while not future.done():
            # Check for progress updates from queue
            try:
                while True:
                    event_type, data = progress_queue.get_nowait()
                    yield f"data: {json.dumps(ProgressEvent(type=event_type, phase=phase_name, timestamp=datetime.now().isoformat(), **data).model_dump())}\n\n"
            except asyncio.QueueEmpty:
                pass
            
            # Send periodic heartbeat/log if no progress for a while
            if (datetime.now() - last_progress_time).total_seconds() > 5:
                yield f"data: {json.dumps(ProgressEvent(type='log', phase=phase_name, message=f'{phase_name} in progress...', level='info', timestamp=datetime.now().isoformat()).model_dump())}\n\n"
                last_progress_time = datetime.now()
            
            await asyncio.sleep(0.5)
        
        # Drain any remaining progress events
        try:
            while True:
                event_type, data = progress_queue.get_nowait()
                yield f"data: {json.dumps(ProgressEvent(type=event_type, phase=phase_name, timestamp=datetime.now().isoformat(), **data).model_dump())}\n\n"
        except asyncio.QueueEmpty:
            pass
        
        status, result = await future
        
        if status == "success":
            completed_at = datetime.now()
            duration = (completed_at - started_at).total_seconds()
            yield f"data: {json.dumps(ProgressEvent(type='phase_complete', phase=phase_name, message=f'{phase_name} completed successfully in {duration:.2f}s', level='success', timestamp=datetime.now().isoformat()).model_dump())}\n\n"
        else:
            yield f"data: {json.dumps(ProgressEvent(type='phase_error', phase=phase_name, message=f'{phase_name} failed: {result}', level='error', timestamp=datetime.now().isoformat()).model_dump())}\n\n"
            
    except Exception as e:
        yield f"data: {json.dumps(ProgressEvent(type='phase_error', phase=phase_name, message=f'{phase_name} error: {str(e)}', level='error', timestamp=datetime.now().isoformat()).model_dump())}\n\n"


@router.get("/run/extraction/stream")
async def stream_extraction(start_date: str = "", end_date: str = ""):
    """Stream extraction progress via SSE."""
    orchestrator = PipelineOrchestrator({})
    return StreamingResponse(
        stream_progress("extraction", orchestrator.run_extraction, start_date, end_date),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
    )


@router.get("/run/transformation/stream")
async def stream_transformation(data_layer: str = "Bronze", date: str = ""):
    """Stream transformation progress via SSE."""
    orchestrator = PipelineOrchestrator({})
    return StreamingResponse(
        stream_progress("transformation", orchestrator.run_transformation, data_layer, date),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
    )


@router.get("/run/cloud-push/stream")
async def stream_cloud_push(data_layer: str = "Bronze", date: str = ""):
    """Stream cloud push progress via SSE."""
    orchestrator = PipelineOrchestrator({})
    return StreamingResponse(
        stream_progress("cloud_push", orchestrator.run_cloud_push, data_layer, date),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
    )


@router.get("/run/ingestion/stream")
async def stream_ingestion(data_layer: str = "Silver", date: str = "", target_db: str = ""):
    """Stream ingestion progress via SSE."""
    orchestrator = PipelineOrchestrator({})
    return StreamingResponse(
        stream_progress("ingestion", orchestrator.run_ingestion, data_layer, date, target_db or None),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
    )


@router.get("/run/full/stream")
async def stream_full_pipeline(
    start_date: str = "",
    end_date: str = "",
    skip_cloud_push: bool = False,
    skip_ingestion: bool = False,
    target_db: str = ""
):
    """Stream full pipeline progress via SSE."""
    orchestrator = PipelineOrchestrator({})
    start_time = datetime.now()
    
    async def full_pipeline_stream():
        phases_to_run = [
            ("extraction", orchestrator.run_extraction, start_date, end_date),
        ]
        
        if not skip_cloud_push:
            phases_to_run.append(("bronze_cloud_push", orchestrator.run_cloud_push, "Bronze", ""))
        
        phases_to_run.append(("transformation", orchestrator.run_transformation, "Bronze", ""))
        
        if not skip_cloud_push:
            phases_to_run.append(("silver_cloud_push", orchestrator.run_cloud_push, "Silver", ""))
        
        if not skip_ingestion:
            phases_to_run.append(("ingestion", orchestrator.run_ingestion, "Silver", "", target_db or None))
        
        all_phases = []
        
        for phase_name, phase_func, *phase_args in phases_to_run:
            async for event in stream_progress(phase_name, phase_func, *phase_args):
                yield event
        
        total_duration = (datetime.now() - start_time).total_seconds()
        yield f"data: {json.dumps(ProgressEvent(type='log', phase='full_pipeline', message=f'Full pipeline completed in {total_duration:.2f}s', level='success', timestamp=datetime.now().isoformat()).model_dump())}\n\n"
    
    return StreamingResponse(
        full_pipeline_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
    )


@router.post("/run/extraction", response_model=PipelineResponse)
async def run_extraction(start_date: str = "", end_date: str = ""):
    """Run extraction phase using settings.yaml."""
    orchestrator = PipelineOrchestrator({})
    start_time = datetime.now()
    phases = []
    
    phase_result = run_phase_safely("extraction", orchestrator.run_extraction, start_date, end_date)
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
    """Run transformation phase using settings.yaml."""
    orchestrator = PipelineOrchestrator({})
    start_time = datetime.now()
    phases = []
    
    phase_result = run_phase_safely("transformation", orchestrator.run_transformation, data_layer, date)
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
    """Run cloud push phase using settings.yaml."""
    orchestrator = PipelineOrchestrator({})
    start_time = datetime.now()
    phases = []
    
    phase_result = run_phase_safely("cloud_push", orchestrator.run_cloud_push, data_layer, date)
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
    """Run ingestion phase using settings.yaml."""
    orchestrator = PipelineOrchestrator({})
    start_time = datetime.now()
    phases = []
    
    phase_result = run_phase_safely("ingestion", orchestrator.run_ingestion, data_layer, date, target_db or None)
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
    start_date: str = "",
    end_date: str = "",
    skip_cloud_push: bool = False,
    skip_ingestion: bool = False,
    target_db: str = ""
):
    """Run full pipeline using settings.yaml."""
    orchestrator = PipelineOrchestrator({})
    start_time = datetime.now()
    phases = []
    
    # Phase 1: Extraction
    phase_result = run_phase_safely("extraction", orchestrator.run_extraction, start_date, end_date)
    phases.append(phase_result)
    
    # Phase 2: Bronze Cloud Push
    if not skip_cloud_push:
        phase_result = run_phase_safely("bronze_cloud_push", orchestrator.run_cloud_push, "Bronze", "")
        phases.append(phase_result)
    
    # Phase 3: Transformation
    phase_result = run_phase_safely("transformation", orchestrator.run_transformation, "Bronze", "")
    phases.append(phase_result)
    
    # Phase 4: Silver Cloud Push
    if not skip_cloud_push:
        phase_result = run_phase_safely("silver_cloud_push", orchestrator.run_cloud_push, "Silver", "")
        phases.append(phase_result)
    
    # Phase 5: Ingestion
    if not skip_ingestion:
        phase_result = run_phase_safely("ingestion", orchestrator.run_ingestion, "Silver", "", target_db or None)
        phases.append(phase_result)
    
    total_duration = (datetime.now() - start_time).total_seconds()
    success = all(p.status == "success" for p in phases)
    
    return PipelineResponse(
        success=success,
        phases=phases,
        total_duration_seconds=total_duration,
        error=next((p.message for p in phases if p.status == "failed"), None),
    )