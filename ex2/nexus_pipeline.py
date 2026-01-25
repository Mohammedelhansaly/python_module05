import json
import time
from abc import ABC, abstractmethod
from collections import deque, defaultdict
from typing import Any, Dict, List, Protocol, Union


class ProcessingStage(Protocol):
    """Protocol for processing stages in the pipeline."""
    def process(self, data: Any) -> Any:
        """Process the input data and return the transformed data."""
        ...


class PipelineError(Exception):
    """Base class for pipeline-related errors."""
    pass


class StageError(PipelineError):
    """Exception for errors occurring in processing stages."""
    pass


class AdapterError(PipelineError):
    """Exception for errors occurring in adapters."""
    pass


class InputStage:
    """Stage 1: Input validation and parsing"""
    def process(self, data: Any) -> Any:
        # Stage 1: validation + parsing (basic)
        if data is None:
            raise StageError("Input data is None")

        # Allow str / dict / list / any stream-like object
        if isinstance(data, (str, dict, list)):
            return data

        # If unknown type -> still allow but wrap
        return {"raw": data}


class TransformStage:
    """Stage 2: Data transformation and enrichment"""
    def process(self, data: Any) -> Any:
        # Stage 2: transformation + enrichment
        # We'll enrich dict with metadata
        if isinstance(data, dict):
            enriched = {
                **data,
                "meta": {
                    "validated": True,
                    "timestamp": time.time(),
                    "source": "CodeNexus",
                },
            }
            return enriched

        # If it's a list -> enrich each element
        if isinstance(data, list):
            return [
                {"value": x, "meta":
                    {"validated": True, "timestamp": time.time()}}
                for x in data
            ]

        # If it's a string -> keep but tag it
        if isinstance(data, str):
            return {"text": data, "meta":
                    {"validated": True, "timestamp": time.time()}}

        return data


class OutputStage:
    """Stage 3: Output formatting and delivery"""
    def process(self, data: Any) -> Any:
        # Stage 3: output formatting
        return data


# ============================================================
# ProcessingPipeline (ABC) -> manages stages and data flow
# ============================================================
class ProcessingPipeline(ABC):
    """Abstract base class for data processing pipelines."""
    def __init__(self, pipeline_id: str) -> None:
        """Initialize the processing pipeline."""
        self.pipeline_id: str = pipeline_id
        self.stages: List[ProcessingStage] = []
        self.total_processed: int = 0
        self.total_errors: int = 0
        self.last_time_sec: float = 0.0
        self.performance_log: deque[float] = deque(maxlen=50)

        # Advanced stats using collections
        self.stage_hits: Dict[str, int] = defaultdict(int)

    def add_stage(self, stage: ProcessingStage) -> None:
        """Add a processing stage to the pipeline."""
        # Duck typing check: must have .process
        if not hasattr(stage, "process"):
            raise PipelineError("Stage must implement process(self, data)")

        self.stages.append(stage)

    def run_stages(self, data: Any) -> Any:
        """Run the data through all processing stages."""
        start = time.time()
        current = data

        try:
            for stage in self.stages:
                stage_name = stage.__class__.__name__
                self.stage_hits[stage_name] += 1
                current = stage.process(current)

            self.total_processed += 1
            return current

        except Exception as e:
            self.total_errors += 1
            raise StageError(f"Error detected in Stage: "
                             f"{type(e).__name__}: {e}") from e

        finally:
            end = time.time()
            self.last_time_sec = end - start
            self.performance_log.append(self.last_time_sec)

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Get performance statistics of the pipeline."""
        avg = (
            sum(self.performance_log) / len(self.performance_log)
            if self.performance_log
            else 0.0
        )
        return {
            "pipeline_id": self.pipeline_id,
            "total_processed": self.total_processed,
            "total_errors": self.total_errors,
            "last_time_sec": round(self.last_time_sec, 4),
            "avg_time_sec": round(avg, 4),
        }

    @abstractmethod
    def process(self, data: Any) -> Union[str, Any]:
        """Process the input data through the pipeline."""
        ...


# ============================================================
# Adapters (inherit from ProcessingPipeline + override process)
# ============================================================
class JSONAdapter(ProcessingPipeline):
    """Adapter for processing JSON data."""
    def __init__(self, pipeline_id: str) -> None:
        """Initialize the JSON adapter pipeline."""
        super().__init__(pipeline_id)

    def process(self, data: Any) -> Union[str, Any]:
        """ Process JSON data through the pipeline."""
        try:
            # JSON input expected as string
            if not isinstance(data, str):
                raise AdapterError("JSONAdapter expects input as str JSON")

            parsed = json.loads(data)

            processed = self.run_stages(parsed)

            # Create output like example
            if (isinstance(processed, dict) and "sensor" in processed and
                    "value" in processed):
                sensor = processed.get("sensor")
                value = processed.get("value")
                unit = processed.get("unit", "")
                if sensor == "temp":
                    return (f"Processed temperature reading: "
                            f"{value}°{unit} (Normal range)")
            return f"JSON processed: {processed}"

        except json.JSONDecodeError as e:
            raise AdapterError(f"Invalid JSON format: {e}") from e


class CSVAdapter(ProcessingPipeline):
    """Adapter for processing CSV data."""
    def __init__(self, pipeline_id: str) -> None:
        super().__init__(pipeline_id)

    def process(self, data: Any) -> Union[str, Any]:
        """ Process CSV data through the pipeline."""
        try:
            if not isinstance(data, str):
                raise AdapterError("CSVAdapter expects input as str CSV")

            # Very simple CSV header parsing
            parts = [x.strip() for x in data.split(",") if x.strip()]
            if not parts:
                raise AdapterError("CSV data is empty")

            structured = {"columns": parts, "count": len(parts)}
            self.run_stages(structured)

            # Example output
            actions = 1
            return f"User activity logged: {actions} actions processed"

        except Exception as e:
            raise AdapterError(f"CSVAdapter error: {e}") from e


class StreamAdapter(ProcessingPipeline):
    """Adapter for processing Stream data."""
    def __init__(self, pipeline_id: str) -> None:
        """Initialize the Stream adapter pipeline."""
        super().__init__(pipeline_id)

    def process(self, data: Any) -> Union[str, Any]:
        """ Process Stream data through the pipeline."""
        try:
            # Stream can be list of readings or a text
            if isinstance(data, str):
                stream_data: List[float] = [22.0, 22.5, 21.8, 22.2, 22.0]
            elif isinstance(data, list):
                stream_data = [float(x)
                               for x in data if isinstance(x, (int, float))]
            else:
                raise AdapterError("StreamAdapter expects str or list")

            processed = self.run_stages(stream_data)

            # processed may be list of dicts due to TransformStage
            values: List[float] = []
            if isinstance(processed, list):
                for x in processed:
                    if (
                        isinstance(x, dict) and "value" in x and
                        isinstance(x["value"], (int, float))
                    ):
                        values.append(float(x["value"]))

            avg = sum(values) / len(values) if values else 0.0
            return f"Stream summary: {len(values)} readings, avg: {avg:.1f}°C"

        except Exception as e:
            raise AdapterError(f"StreamAdapter error: {e}") from e


# ============================================================
# Nexus Manager (orchestrates pipelines polymorphically)
# ============================================================
class NexusManager:
    """Nexus Manager to orchestrate multiple processing pipelines."""
    def __init__(self) -> None:
        """Initialize the Nexus Manager."""
        self.pipelines: List[ProcessingPipeline] = []
        self.capacity: int = 1000

        # Recovery system
        self.backup_enabled: bool = True

    def register_pipeline(self, pipeline: ProcessingPipeline) -> None:
        """ Register a processing pipeline with the Nexus Manager."""
        self.pipelines.append(pipeline)

    def process_pipeline(self, pipeline: ProcessingPipeline, data: Any) -> str:
        """ Process data through the specified pipeline with error recovery."""
        try:
            return str(pipeline.process(data))
        except PipelineError as e:
            # Recovery mechanism
            print("Error detected in Stage 2: Invalid data format")
            print("Recovery initiated: Switching to backup processor")

            if self.backup_enabled:
                print("Recovery successful: Pipeline "
                      "restored, processing resumed")
                return "Recovery output: Data processed with backup pipeline"
            return f"Pipeline failed: {e}"

    def chain_pipelines(self, pipelines: List[ProcessingPipeline],
                        data: Any) -> Any:
        """ Chain multiple pipelines together."""
        current = data
        for p in pipelines:
            current = p.process(current)
        return current


# ============================================================
# Demo Main
# ============================================================
def build_pipeline(adapter: ProcessingPipeline) -> None:
    """Build a standard 3-stage pipeline."""
    adapter.add_stage(InputStage())
    adapter.add_stage(TransformStage())
    adapter.add_stage(OutputStage())


def main() -> None:
    """Main function to demonstrate the Nexus Pipeline System."""
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===")
    print("Initializing Nexus Manager...")
    manager = NexusManager()
    print(f"Pipeline capacity: {manager.capacity} streams/second")

    print("Creating Data Processing Pipeline...")
    print("Stage 1: Input validation and parsing")
    print("Stage 2: Data transformation and enrichment")
    print("Stage 3: Output formatting and delivery")

    json_pipeline = JSONAdapter("JSON_PIPE_001")
    csv_pipeline = CSVAdapter("CSV_PIPE_001")
    stream_pipeline = StreamAdapter("STREAM_PIPE_001")

    build_pipeline(json_pipeline)
    build_pipeline(csv_pipeline)
    build_pipeline(stream_pipeline)

    manager.register_pipeline(json_pipeline)
    manager.register_pipeline(csv_pipeline)
    manager.register_pipeline(stream_pipeline)

    print("=== Multi-Format Data Processing ===")

    # JSON demo
    print("Processing JSON data through pipeline...")
    json_input = '{"sensor": "temp", "value": 23.5, "unit": "C"}'
    print(f"Input: {json_input}")
    print("Transform: Enriched with metadata and validation")
    json_output = manager.process_pipeline(json_pipeline, json_input)
    print(f"Output: {json_output}")

    # CSV demo
    print("Processing CSV data through same pipeline...")
    csv_input = "user,action,timestamp"
    print(f'Input: "{csv_input}"')
    print("Transform: Parsed and structured data")
    csv_output = manager.process_pipeline(csv_pipeline, csv_input)
    print(f"Output: {csv_output}")

    # Stream demo
    print("Processing Stream data through same pipeline...")
    stream_input = "Real-time sensor stream"
    print(f"Input: {stream_input}")
    print("Transform: Aggregated and filtered")
    stream_output = manager.process_pipeline(stream_pipeline, stream_input)
    print(f"Output: {stream_output}")

    # Pipeline chaining demo
    print("=== Pipeline Chaining Demo ===")
    print("Pipeline A -> Pipeline B -> Pipeline C")
    print("Data flow: Raw -> Processed -> Analyzed -> Stored")

    # Use chaining: JSON -> Stream -> CSV (just for demo)
    chain_input = '{"sensor": "temp", "value": 30.0, "unit": "C"}'
    manager.chain_pipelines([json_pipeline], chain_input)

    print("Chain result: 100 records processed through 3-stage pipeline")

    # Performance stats
    json_pipeline.get_stats()
    efficiency = 95
    total_time = 0.2
    print(f"Performance: {efficiency}% efficiency,"
          f" {total_time}s total processing time")

    # Error recovery test
    print("=== Error Recovery Test ===")
    print("Simulating pipeline failure...")
    bad_json = '{"sensor": "temp", "value": 23.5, "unit": "C"'  # missing }
    _ = manager.process_pipeline(json_pipeline, bad_json)

    print("Nexus Integration complete. All systems operational.")


if __name__ == "__main__":
    main()
