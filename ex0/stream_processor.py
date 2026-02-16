from abc import ABC, abstractmethod
from typing import Any, List, Dict


class DataProcessor(ABC):
    """Abstract base class for data processors."""
    @abstractmethod
    def process(self, data: Any) -> str:
        """Process the input data and return the result."""
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        """Validate the input data format."""
        pass

    def format_output(self, result: str) -> str:
        """ Format the processed data for output."""
        pass


class NumericProcessor(DataProcessor):
    """Processor for numeric data."""
    def process(self, data: Any) -> str:
        """Process numeric data by calculating sum and average."""
        if not self.validate(data):
            raise ValueError("Numeric data verification failed")
        count = len(data)
        total = sum(data)
        avg = total / count if count > 0 else 0
        return f"Processed {count} numeric values, sum={total}, avg={avg}"

    def validate(self, data: Any) -> bool:
        """ Validate that data is a list of numbers. """
        return (
            isinstance(data, list) and
            all(isinstance(x, (int, float)) for x in data)
        )

    def format_output(self, result: str) -> str:
        """ Format the processed numeric data for output. """
        return f"{result}"


class TextProcessor(DataProcessor):
    """Processor for text data."""
    def validate(self, data: Any) -> bool:
        """ Validate that data is a string. """
        return isinstance(data, str)

    def process(self, data: str) -> str:
        """ Process text data by counting characters and words. """
        if not self.validate(data):
            raise ValueError("Text data verification failed")
        chars = len(data)
        words = len(data.split())
        return f"Processed text: {chars} characters, {words} words"

    def format_output(self, result: str) -> str:
        """ Format the processed text data for output. """
        return f"{result}"


class LogProcessor(DataProcessor):
    """Processor for log data."""
    def validate(self, data: Any) -> bool:
        """ Validate that data is a log entry string. """
        return isinstance(data, str) and ":" in data

    def process(self, data: str) -> str:
        """ Process log data by extracting log level and message. """
        if not self.validate(data):
            raise ValueError("Log entry verification failed")
        level, msg = data.split(":", 1)
        return f"[{level.strip()}] level detected:{msg}"

    def format_output(self, result: str) -> str:
        """ Format the processed log data for output. """
        return f"{result}"


def main() -> None:
    """Main function to demonstrate polymorphic data processing."""
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")

    datasets: List[Dict[str, Any]] = [
        {"name": "Numeric Processor", "proc": NumericProcessor(),
         "data": [1, 2, 3, 4, 5]},
        {"name": "Text Processor", "proc": TextProcessor(),
         "data": "Hello Nexus World"},
        {"name": "Log Processor", "proc": LogProcessor(),
         "data": "ERROR: Connection timeout"}
    ]

    for entry in datasets:
        name: str = entry["name"]
        processor: DataProcessor = entry["proc"]
        data: Any = entry["data"]

        print(f"Initializing {name}...")
        print(f"Processing data: {repr(data)}")

        try:
            if processor.validate(data):
                print(f"Validation: {name.split()[0]} data verified")
                result = processor.process(data)
                print(f"Output : {processor.format_output(result)}")
            else:
                print("Validation: Failed")
        except Exception as e:
            print(f"Error during processing: {e}")
        print()

    print("=== Polymorphic Processing Demo ===")
    print("\nProcessing multiple data types through same interface...")
    processors: list[DataProcessor] = [
        NumericProcessor(),
        TextProcessor(),
        LogProcessor()
    ]
    multydata = [[1, 2, 3], "hello world", "INFO : system ready"]
    for processor, data in zip(processors, multydata):
        if processor.validate(data):
            result = processor.process(data)
            print(f"Result : {processor.format_output(result)}")


if __name__ == "__main__":
    main()
