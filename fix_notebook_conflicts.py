"""
Fix Git merge conflicts in Jupyter notebook by keeping the 'Updated upstream' version
"""

import re
import sys

def fix_conflicts(input_file, output_file=None):
    if output_file is None:
        output_file = input_file
    
    with open(input_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    fixed_lines = []
    in_conflict = False
    conflict_type = None  # 'with_marker' or 'without_marker'
    skip_until_end = False
    
    i = 0
    while i < len(lines):
        line = lines[i]
        
        # Check for start of conflict with marker
        if line.startswith('<<<<<<< Updated upstream'):
            in_conflict = True
            conflict_type = 'with_marker'
            i += 1
            continue
        
        # Check for conflict separator
        elif line.startswith('=======') and not in_conflict:
            # This is a conflict without the upstream marker
            in_conflict = True
            conflict_type = 'without_marker'
            skip_until_end = True
            i += 1
            continue
        
        # Check for middle of conflict
        elif line.startswith('=======') and in_conflict and conflict_type == 'with_marker':
            # Skip the stashed changes part
            skip_until_end = True
            i += 1
            continue
        
        # Check for end of conflict
        elif line.startswith('>>>>>>> Stashed changes'):
            in_conflict = False
            skip_until_end = False
            conflict_type = None
            i += 1
            continue
        
        # Add line if not skipping
        if not skip_until_end:
            fixed_lines.append(line)
        
        i += 1
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.writelines(fixed_lines)
    
    print(f"Fixed conflicts in {input_file}")
    if output_file != input_file:
        print(f"Output written to {output_file}")

if __name__ == "__main__":
    input_file = r"d:\docs\MADS\696-Milestone 2\src\unsupervised.ipynb"
    fix_conflicts(input_file)
