from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union


class StreamError(Exception):
    pass


class SensorStreamError(StreamError):
    pass


class TransactionStreamError(StreamError):
    pass


class EventStreamError(StreamError):
    pass


class DataStream(ABC):
    def __init__(self, stream_id: str) -> None:
        self.stream_id: str = stream_id
        self.total_processed: int = 0
        self.last_batch_size: int = 0
        self.stream_type: str = "Generic Stream"

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of data and return a human-readable result."""
        raise NotImplementedError

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        """Default filter: return original batch if no criteria."""
        if criteria is None:
            return data_batch
        return [x for x in data_batch if isinstance(x, str) and criteria in x]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Default stats."""
        return {
            "stream_id": self.stream_id,
            "type": self.stream_type,
            "total_processed": self.total_processed,
            "last_batch_size": self.last_batch_size,
        }


class SensorStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.stream_type = "Environmental Data"
        self.temp_readings: List[float] = []

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            readings: List[str] = [x for x in data_batch if isinstance(x, str)]
            if not readings:
                raise SensorStreamError("Empty sensor batch "
                                        "or invalid data types")

            processed = 0
            for item in readings:
                if ":" not in item:
                    continue
                key, value_str = item.split(":", 1)
                key = key.strip().lower()
                value_str = value_str.strip()

                if key == "temp":
                    value = float(value_str)
                    self.temp_readings.append(value)

                processed += 1

            self.total_processed += processed
            self.last_batch_size = len(data_batch)

            avg_temp = (
                sum(self.temp_readings) / len(self.temp_readings)
                if self.temp_readings
                else 0.0
            )

            return (f"Sensor analysis: {processed} readings processed, "
                    f"avg temp: {avg_temp:.1f}°C")
        except ValueError as e:
            raise SensorStreamError(f"Sensor parsing error: {e}") from e
        except Exception as e:
            raise SensorStreamError(f"Unexpected sensor error: "
                                    f"{type(e).__name__}") from e

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        if criteria is None:
            return [x for x in data_batch if isinstance(x, str)]

        if criteria == "critical":
            filtered: List[str] = []
            for x in data_batch:
                if not isinstance(x, str) or ":" not in x:
                    continue
                key, value_str = x.split(":", 1)
                if key.strip().lower() == "temp":
                    try:
                        if float(value_str.strip()) > 40:
                            filtered.append(x)
                    except ValueError:
                        continue
            return filtered

        return super().filter_data(data_batch, criteria)


class TransactionStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.stream_type = "Financial Data"

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            ops: List[str] = [x for x in data_batch if isinstance(x, str)]
            if not ops:
                raise TransactionStreamError("Empty transaction batch or "
                                             "invalid data types")

            buy_total = 0
            sell_total = 0
            processed = 0

            for op in ops:
                if ":" not in op:
                    continue
                action, amount_str = op.split(":", 1)
                action = action.strip().lower()
                amount = int(amount_str.strip())

                if action == "buy":
                    buy_total += amount
                    processed += 1
                elif action == "sell":
                    sell_total += amount
                    processed += 1

            net_flow = sell_total - buy_total
            self.total_processed += processed
            self.last_batch_size = len(data_batch)

            sign = "+" if net_flow >= 0 else "-"
            return (f"Transaction analysis: {processed} operations, net "
                    f"flow: {sign}{abs(net_flow)} units")
        except ValueError as e:
            raise TransactionStreamError(f"Transaction "
                                         f"parsing error: {e}") from e
        except Exception as e:
            raise TransactionStreamError(
                f"Unexpected transaction error: {type(e).__name__}"
            ) from e

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        if criteria is None:
            return [x for x in data_batch if isinstance(x, str)]

        if criteria == "large":
            filtered: List[str] = []
            for x in data_batch:
                if not isinstance(x, str) or ":" not in x:
                    continue
                _, amount_str = x.split(":", 1)
                try:
                    if int(amount_str.strip()) >= 100:
                        filtered.append(x)
                except ValueError:
                    continue
            return filtered

        return super().filter_data(data_batch, criteria)


class EventStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.stream_type = "System Events"

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            events: List[str] = [x for x in data_batch if isinstance(x, str)]
            if not events:
                raise EventStreamError("Empty event batch "
                                       "or invalid data types")

            total = len(events)
            errors = sum(1 for e in events if e.strip().lower() == "error")

            self.total_processed += total
            self.last_batch_size = len(data_batch)

            return f"Event analysis: {total} events, {errors} error detected"
        except Exception as e:
            raise EventStreamError(f"Unexpected event error: "
                                   f"{type(e).__name__}") from e

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        if criteria is None:
            return [x for x in data_batch if isinstance(x, str)]

        if criteria == "error":
            return [x for x in data_batch if isinstance(x, str)
                    and x.lower() == "error"]

        return super().filter_data(data_batch, criteria)


class StreamProcessor:
    def __init__(self) -> None:
        self.streams: List[DataStream] = []

    def register_stream(self, stream: DataStream) -> None:
        if not isinstance(stream, DataStream):
            raise StreamError("Only DataStream objects can be registered")
        self.streams.append(stream)

    def process_all(self, batches: Dict[str, List[Any]]) -> None:
        print("=== Polymorphic Stream Processing ===")
        print("Processing mixed stream types through unified interface...\n")

        for stream in self.streams:
            try:
                batch = batches.get(stream.stream_id, [])
                result = stream.process_batch(batch)
                print(f"- {stream.stream_id}: {result}")
            except StreamError as e:
                print(f"- {stream.stream_id}: ERROR -> {e}")
            except Exception as e:
                print(f"- {stream.stream_id}: UNEXPECTED"
                      f" -> {type(e).__name__}")
        print()


def main() -> None:
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    print("Initializing Sensor Stream...")
    sensor = SensorStream("SENSOR_001")
    print(f"Stream ID: {sensor.stream_id}, Type: {sensor.stream_type}")
    sensor_batch = ["temp:22.5", "humidity:65", "pressure:1013"]
    print(f"Processing sensor batch: {sensor_batch}")
    print(sensor.process_batch(sensor_batch))
    print()

    print("Initializing Transaction Stream...")
    trans = TransactionStream("TRANS_001")
    print(f"Stream ID: {trans.stream_id}, Type: {trans.stream_type}")
    trans_batch = ["buy:100", "sell:150", "buy:75"]
    print(f"Processing transaction batch: {trans_batch}")
    print(trans.process_batch(trans_batch))
    print()

    print("Initializing Event Stream...")
    event = EventStream("EVENT_001")
    print(f"Stream ID: {event.stream_id}, Type: {event.stream_type}")
    event_batch = ["login", "error", "logout"]
    print(f"Processing event batch: {event_batch}")
    print(event.process_batch(event_batch))
    print()

    processor = StreamProcessor()
    processor.register_stream(sensor)
    processor.register_stream(trans)
    processor.register_stream(event)

    batches = {
        "SENSOR_001": ["temp:45.0", "temp:20.0", "humidity:50"],
        "TRANS_001": ["buy:10", "sell:200", "buy:50", "sell:5"],
        "EVENT_001": ["login", "error", "error"],
    }

    processor.process_all(batches)

    print("Stream filtering active: High-priority data only")

    critical_sensors = sensor.filter_data(batches["SENSOR_001"], "critical")
    large_trans = trans.filter_data(batches["TRANS_001"], "large")
    error_events = event.filter_data(batches["EVENT_001"], "error")

    print(
        f"Filtered results: {len(critical_sensors)} critical sensor alerts, "
        f"{len(large_trans)} large transaction, "
        f"{len(error_events)} error events"
    )
    print("\nAll streams processed successfully. Nexus throughput optimal.\n")


if __name__ == "__main__":
    main()
