import os
import json
import csv
import pandas as pd

from typing import List, Tuple

class DataProcessor:
    """A class to handle data processing, merging, distribution, and validation"""

    def __init__(
        self, input_dir: str, output_dir: str, temp_dir: str, current_dir: str
    ):
        """Initializes the DataProcessor with input, output, temporary, and current directories."""
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.temp_dir = temp_dir
        self.current_dir = current_dir

    def load_data(self) -> Tuple[List[dict], List[dict]]:
        """Loads data from CSV and JSON files from the input directory."""
        csv_data = []
        json_data = []
        for filename in os.listdir(self.input_dir):
            file_path = os.path.join(self.input_dir, filename)
            if filename.endswith(".csv"):
                try:
                    with open(file_path, "r", newline="", encoding="utf-8") as csvfile:
                        reader = csv.DictReader(csvfile)
                        for row in reader:
                            csv_data.append(row)
                except Exception as e:
                    print(f"Error reading CSV file {filename}: {e}")
            elif filename.endswith(".json"):
                try:
                    with open(file_path, "r", encoding="utf-8") as jsonfile:
                        file_content = jsonfile.read()
                        if file_content.strip().startswith("["):
                            data = json.loads(file_content)
                            json_data.extend(data)
                        else:
                            data = json.loads(file_content)
                            json_data.append(data)
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON file {filename}: {e}")
                except Exception as e:
                    print(f"Error reading JSON file {filename}: {e}")
        return csv_data, json_data

    def merge_data(self, csv_data: List[dict], json_data: List[dict]) -> List[dict]:
        """Merges CSV and JSON data and stores it in a temporary file."""
        merged_data = csv_data + json_data
        if not os.path.exists(self.temp_dir):
            os.makedirs(self.temp_dir)
        temp_file_path = os.path.join(self.temp_dir, "merged_data.json")
        with open(temp_file_path, "w", encoding="utf-8") as temp_file:
            json.dump(merged_data, temp_file, indent=4)
        return merged_data

    def process_data(self, data: List[dict]) -> List[dict]:
        """Processes the merged data as per the requirements."""
        processed_data = []
        for player in data:
            runs = int(player.get("runs", 0) or 0)
            wickets = int(player.get("wickets", 0) or 0)
            age = int(player.get("age", 0) or 0)

            if runs > 500 and wickets >= 50:
                player_type = "All-Rounder"
            elif runs > 500 and wickets < 50:
                player_type = "Batsman"
            elif runs < 500:
                player_type = "Bowler"
            else:
                continue

            player["playerType"] = player_type
            if not (15 < age < 50):
                continue
            processed_data.append(player)
        return processed_data

    def distribute_data(self, processed_data: List[dict]):
        """Distributes the processed data to different customers based on event type."""
        odi_results = []
        test_results = []
        for player in processed_data:
            if player.get("eventType") == "ODI":
                odi_results.append(player)
            elif player.get("eventType") == "Test":
                test_results.append(player)

        if not os.path.exists(self.current_dir):
            os.makedirs(self.current_dir)

        odi_file_path = os.path.join(self.current_dir, "odi_results.csv")
        self.save_to_csv(odi_file_path, odi_results)
        test_file_path = os.path.join(self.current_dir, "test_results.csv")
        self.save_to_csv(test_file_path, test_results)

    def save_to_csv(self, file_path: str, data: List[dict]):
        """Saves data to a CSV file."""
        if data:
            keys = data[0].keys()
            with open(file_path, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=keys)
                writer.writeheader()
                writer.writerows(data)

    def validate_data(self, processed_data: List[dict]):
        """Validates the processed data against the expected output and creates a test result file."""
        test_results = []
        try:
            odi_expected_path = os.path.join(
                self.output_dir, "assignment_outputDataSet_odi.csv"
            )
            expected_odi_data = pd.read_csv(odi_expected_path).to_dict("records")
        except FileNotFoundError:
            print(f"Warning: Expected ODI results file not found: {odi_expected_path}")
            expected_odi_data = []

        try:
            test_expected_path = os.path.join(
                self.output_dir, "assignment_outputDataSet_test.csv"
            )
            expected_test_data = pd.read_csv(test_expected_path).to_dict("records")
        except FileNotFoundError:
            print(
                f"Warning: Expected Test results file not found: {test_expected_path}"
            )
            expected_test_data = []

        expected_odi_dict = {
            frozenset(item.items()): item for item in expected_odi_data
        }
        expected_test_dict = {
            frozenset(item.items()): item for item in expected_test_data
        }

        for player in processed_data:
            player_key = frozenset(player.items())
            if player.get("eventType") == "ODI":
                result = (
                    "PASS" if expected_odi_dict.get(player_key) == player else "FAIL"
                )
            elif player.get("eventType") == "Test":
                result = (
                    "PASS" if expected_test_dict.get(player_key) == player else "FAIL"
                )
            else:
                result = "FAIL"
            player_copy = player.copy()
            player_copy["Result"] = result
            test_results.append(player_copy)

        test_result_path = os.path.join(self.current_dir, "test_result.csv")
        self.save_to_csv(test_result_path, test_results)


if __name__ == "__main__":
    input_dir = "input_files"
    output_dir = "output_files"
    temp_dir = "temp"
    current_dir = "Results"

    processor = DataProcessor(input_dir, output_dir, temp_dir, current_dir)
    csv_data, json_data = processor.load_data()
    merged_data = processor.merge_data(csv_data, json_data)
    processed_data = processor.process_data(merged_data)
    processor.distribute_data(processed_data)
    processor.validate_data(processed_data)

    print("Data processing and validation completed.")

