from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional

class DataProcessor(ABC):

    @abstractmethod
    def process(self, data):
        pass

    @abstractmethod
    def validate(self, data):
        pass

    def format_output(self, data):
        pass

class NumiricProcessor(DataProcessor):

    def process(self, data: Any) -> str:
        processed_data = [int(x) for x in data]
        print(f"Processing data: {processed_data}")
        return processed_data
    def validate(self, data: Any) -> bool:
        try:
            if not all(isinstance(x, int) for x in data):
                raise ValueError("All items in data must be numeric (int).")
            print("Validation: Numeric data verified")
            return True
        except Exception as e:
            print(f"Validation error: {e}")
            return False
    def format_output(self, data: Any) -> str:
        return f"Output: Processed {len(data)} numeric values, sum={sum(data)}, avg={sum(data)/len(data) if data else 0}"

class TextProcessor(DataProcessor):
    def process(self, data: Any) -> str:
        print(f'Processing data: "{(" ").join(data)}"')
        return processed_data
    def validate(self, data: Any) -> bool:
        try:
            if not all(isinstance(x, str) for x in data):
                raise ValueError("All items in data must be strings.")
            print("Validation: Text data verified")
            return True
        except Exception as e:
            print(f"Validation error: {e}")
            return False
    def format_output(self, data: Any) -> str:
        return f"Output: Processed text: {sum(len(word) for word in data)} characters, {len(data)} words"
class LogProcessor(DataProcessor):
    def process(self, data: Any):

        
if __name__ == "__main__":
    sample_data = [1, 2, 3, 4, 5]
    processor = NumiricProcessor()
    processed_data = processor.process(sample_data)
    processor.validate(sample_data)
    print(processor.format_output(processed_data))

    text = TextProcessor()
    text_data = ["hello","world"]
    processed_text = text.process(text_data)
    text.validate(text_data)
    print(text.format_output(text_data))
