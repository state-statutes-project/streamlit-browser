#!/usr/bin/env python3
"""
Data Validation Script for Statutes Data

This script checks a statutes_data.json file for structural consistency
and reports any abnormalities found.
"""

import json
import sys
import re

def validate_statute_structure(statute, index):
    """Validate the structure of a single statute entry."""
    errors = []
    
    # Check for required fields
    required_fields = ['title', 'content']
    for field in required_fields:
        if field not in statute:
            errors.append(f"  - Missing required field: '{field}'")
    
    # Validate tags structure if present
    if 'tags' in statute:
        if not isinstance(statute['tags'], dict):
            errors.append(f"  - 'tags' field is not a dictionary")
        else:
            # Check for expected specificity levels
            expected_levels = ['highly_specific', 'specific', 'moderately_specific', 'general']
            for level in statute['tags'].keys():
                if level not in expected_levels:
                    errors.append(f"  - Unexpected tag level: '{level}'")
            
            # Check that each tag list is actually a list
            for level, tags in statute['tags'].items():
                if not isinstance(tags, list):
                    errors.append(f"  - Tag level '{level}' is not a list")
                else:
                    # Check for non-string tags
                    for i, tag in enumerate(tags):
                        if not isinstance(tag, str):
                            errors.append(f"  - Non-string tag found at level '{level}', index {i}")
    
    # Check for URL field
    if 'url' not in statute:
        errors.append(f"  - Missing 'url' field (optional but recommended)")
    
    # Validate content structure (should be markdown with sections)
    if 'content' in statute:
        content = statute['content']
        if not isinstance(content, str):
            errors.append(f"  - 'content' field is not a string")
        else:
            # Check if content has proper Markdown headers
            if not re.search(r'# .+', content):
                errors.append(f"  - 'content' does not have a main title (# heading)")
            
            # Check if content has sections
            if not re.search(r'## .+', content):
                errors.append(f"  - 'content' does not have any sections (## headings)")
            
            # Check for citations
            if not re.search(r'Citation:', content):
                errors.append(f"  - 'content' appears to be missing citations")
    
    # Check for nested tags
    if 'tags' in statute and isinstance(statute['tags'], dict):
        for level, tags in statute['tags'].items():
            if isinstance(tags, dict):
                errors.append(f"  - Nested dictionary found in tags at level '{level}' (should be a list)")
    
    # Check for unexpected top-level fields
    expected_fields = ['title', 'url', 'content', 'tags']
    for field in statute.keys():
        if field not in expected_fields:
            errors.append(f"  - Unexpected field: '{field}'")
    
    return errors

def main():
    """Main validation function."""
    if len(sys.argv) > 1:
        filename = sys.argv[1]
    else:
        filename = 'statutes_data.json'
    
    print(f"Validating {filename}...")
    
    try:
        with open(filename, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: {filename} not found.")
        return 1
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in {filename}: {e}")
        return 1
    
    # Check if data is a list
    if not isinstance(data, list):
        print("Error: Data is not a list of statutes.")
        return 1
    
    print(f"Found {len(data)} statute entries.")
    
    # Validate each statute
    total_errors = 0
    for i, statute in enumerate(data):
        errors = validate_statute_structure(statute, i)
        if errors:
            print(f"Issues found in statute {i} ('{statute.get('title', 'Untitled')}'):")
            for error in errors:
                print(error)
            print()  # Add blank line between statutes with errors
            total_errors += len(errors)
    
    if total_errors == 0:
        print("Validation successful! No issues found.")
        return 0
    else:
        print(f"Validation complete. Found {total_errors} issues across {len(data)} statutes.")
        return 1

if __name__ == "__main__":
    sys.exit(main())