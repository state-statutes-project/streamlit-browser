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
from collections import Counter


TAGS = [
    "local_preemption",
    "private_right_of_action",
    "public_meeting_requirement",
    "attorneys_fees",   
]


def main():

    # Load minimal code units from Alabama 2023
    mcu_input_dir = "../justia-scraped/mcu_json/Alabama_2023.jsonl"
    mcu_list = [json.loads(line.strip()) for line in open(mcu_input_dir, 'r')]

    # Load LLM-generated tags. We produce a dictionary of tag name -> unique_id -> tag dictionary (yes/no/unknown)
    tag_input_dir = "../justia-scraped/tags"
    tag_dict = {}
    for tag in TAGS:
        tag_fpath = os.path.join(tag_input_dir, tag, f"{tag}_results.json")
        tag_dict[tag] = json.load(open(tag_fpath, 'r'))
        print(f"Loaded {len(tag_dict[tag])} tags for {tag}")
    
    # Construct the data frame for the app. This is a dataframe with the following columns:
    # - unique_id: unique id of the mcu
    # - full_name: full name of the mcu
    # - path: path of the mcu
    # - jurisdiction: jurisdiction of the mcu
    # - year: year of the mcu
    # - text: text of the mcu
    # - tag_list: list of tags for the mcu
    # We get the tag_list from the tag_dict
    

    new_mcu_list = [] # We only want to keep some of the fields in the mcu_list
    for mcu in mcu_list:

        # We want to skip local laws for Alabama
        if {'type': 'Title', 'number': '45', 'name': 'Local Laws.'} in mcu["path"]:
            continue
        
        mcu_id = mcu["unique_id"]

        # Build the tag list, which is a list of tag names that are true for the mcu
        tag_list = []
        tag_dict_list = []
        for tag_name in tag_dict.keys():
            if mcu_id in tag_dict[tag_name] and tag_dict[tag_name][mcu_id]["answer"] == "yes":
                tag_list.append(tag_name)
                tag_dict_list.append(tag_dict[tag_name][mcu_id])

                # Check every value in tag_dict[tag_name][mcu_id] is a string
                for key, value in tag_dict[tag_name][mcu_id].items():
                    if not isinstance(value, str):
                        print(f"Value {value} is not a string for tag {tag_name} and mcu {mcu_id}")
        
        # Construct path string 
        path_string = ""
        for division in mcu["path"]:
            div_name = division['name'] if division['name'] is not None else ""
            # Remove trailing period from division name
            if div_name.endswith("."):
                div_name = div_name[:-1]

            path_string += f"{division['type']} {division['number']} - {div_name} > "
        
        new_mcu = {
            "unique_id": mcu["unique_id"],
            "full_name": path_string + " > " + mcu["full_name"],
            "path": path_string,
            "jurisdiction": mcu["jurisdiction"],
            "year": mcu["year"],
            "text": mcu["full_text"],
            "tag_list": tag_list,
            "tag_dict_list": tag_dict_list
        }
        new_mcu_list.append(new_mcu)
    
    
    # Save the mcu_list as a compressed parquet file
    df = pd.DataFrame(new_mcu_list)
    df.to_parquet("data/mcu_list.parquet.gz", index=False, compression="gzip")
    print(f"Saved {len(new_mcu_list)} MCUs to data/mcu_list.parquet.gz")
    

if __name__ == "__main__":
    main() 