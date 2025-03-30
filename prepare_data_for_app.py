"""
This script prepares the data for the Streamlit app.
It loads the JSONL files from the tagged_mcu directory and merges them into a single DataFrame.
The DataFrame is then saved to a parquet file.

Usage:
python prepare_data_for_app.py

"""
import os
import json
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any
import glob
import gzip
from fuzzywuzzy import fuzz
from constants import LIST_OF_EFFECTS
from tqdm import tqdm
def load_jsonl_files(directory: str) -> List[Dict[str, Any]]:
    """Load all JSONL files from a directory and return a list of dictionaries."""
    all_data = {} # key is tag type, value is list of dictionaries
    for jsonl_file in glob.glob(os.path.join(directory, "*.jsonl")):

        # Extract tag type from filename. E.g., legal_effects_tagged_mcu.jsonl -> legal_effects
        fname = os.path.basename(jsonl_file)
        tag_type = fname.replace("_tagged_mcus.jsonl", "")
        all_data[tag_type] = []
        with open(jsonl_file, 'r') as f:
            for line in f:
                data = json.loads(line.strip())
                all_data[tag_type].append(data)
        print(f"Loaded {len(all_data[tag_type])} {tag_type} tagged mcus")
    
    # Check that all tag types have the same number of mcus
    for tag_type, mcus in all_data.items():
        if len(mcus) != len(all_data["legal_effects"]):
            raise ValueError(f"Tag type {tag_type} has a different number of mcus than legal_effects")
    return all_data

def merge_tags(data: Dict[str, List[Dict[str, Any]]]) -> pd.DataFrame:
    """Merge different types of tags into a single DataFrame."""
    processed_data = []

    n_mcus = len(data["legal_effects"])
    
    for i in range(n_mcus):
        # Construct base record with common fields
        item = data["legal_effects"][i]
        path = ""
        for division in item["path"]:
            div_type = division.get("type", "")
            div_num = division.get("number", "")
            div_name = division.get("name", "")
            path += f"{div_type} {div_num} - {div_name} > "
        path = path[:-3] # Remove trailing " > "
        record = {
            'jurisdiction': item["jurisdiction"],
            'year': item["year"],
            'text': item["full_text"],
            'heading': item["full_name"],
            'path': path
        }
        
        # Add all tag types
        for tag_type in data.keys():
            record[tag_type] = data[tag_type][i][f"{tag_type}_tags"]
        
        processed_data.append(record)
    
    return pd.DataFrame(processed_data)


def map_to_effect(generated_effect: str, list_of_effects: List[str]) -> str:
    """Map a generated effect to a list of effects based on fuzzy string matching."""
    # Use fuzzy string matching to find the best match
    best_match = None
    best_match_score = 0
    
    for effect in list_of_effects:
        score = fuzz.partial_ratio(generated_effect, effect)
        if score > best_match_score:
            best_match_score = score
            best_match = effect
    print(f"Best match for {generated_effect} is {best_match} with score {best_match_score}")
    return best_match

def main():
    # File containing JSONL files corresponding to MCUs
    mcu_input_dir = "../justia-scraped/mcu_json/Alabama_2023.jsonl"
    mcu_list = [json.loads(line.strip()) for line in open(mcu_input_dir, 'r')]

    # File containing LLM-generated dictionaries
    dict_input_dir = "../justia-scraped/tagged_mcu/alabama_2023/legal_effects_output_dict.json"
    tag_output_dict = json.load(open(dict_input_dir, 'r'))
    
    # Tag output dict is a dictionary where the key is the unique_id of the mcu and the value is a string representation of a list of dictionaries
    # Each dictionary has the following keys:
    # - effect: string
    # - explanation: string
    # - sections: list of strings
    # We want to merge these dictionaries into the mcu_list
    
    n_error = 0
    new_mcu_list = [] # We only want to keep some of the fields in the mcu_list

    for mcu in mcu_list:
        str_dict = tag_output_dict[mcu["unique_id"]]
        # Convert the string representation of the list of dictionaries to a list of dictionaries
        try:
            str_dict = str_dict.replace("```json", "").replace("```", "")
            str_dict = str_dict.strip()
            
            # Remove all text after the last ]
            last_bracket_index = str_dict.rfind("]")
            str_dict = str_dict[:last_bracket_index + 1]
            tag_list = json.loads(str_dict)


            # Construct path string 
            path_string = ""
            for division in mcu["path"]:
                div_name = division['name']
                # Remove trailing period from division name
                if div_name.endswith("."):
                    div_name = div_name[:-1]

                path_string += f"{division['type']} {division['number']} - {div_name} > "
            

            # Filter out tags that are not in the list of effects
            tag_list = [tag for tag in tag_list if tag['effect'] in LIST_OF_EFFECTS]
            print(path_string, mcu["full_name"])
            new_mcu = {
                "unique_id": mcu["unique_id"],
                "full_name": path_string + " > " + mcu["full_name"],
                "path": path_string,
                "jurisdiction": mcu["jurisdiction"],
                "year": mcu["year"],
                "text": mcu["full_text"],
                "legal_effects": tag_list
            }
            new_mcu_list.append(new_mcu)
        except Exception as e:
            print(f"Error parsing tag_output_dict for mcu {mcu['unique_id']}: {e}")
            n_error += 1

    print(f"Number of errors: {n_error}")
    
    # Save the mcu_list as a compressed parquet file
    df = pd.DataFrame(new_mcu_list)
    df.to_parquet("data/mcu_list.parquet.gz", index=False, compression="gzip")
    

if __name__ == "__main__":
    main() 