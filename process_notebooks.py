#!/usr/bin/env python3
"""Process Jupyter notebooks to remove all emojis."""

import json
import re
from pathlib import Path

def remove_emojis(text):
    """Remove all emoji characters from text."""
    # Comprehensive emoji pattern covering most common emojis
    emoji_pattern = re.compile(
        '['
        '\U0001F300-\U0001F9FF'  # Misc Symbols and Pictographs, Emoticons, Transport
        '\U0001F600-\U0001F64F'  # Emoticons
        '\U0001F680-\U0001F6FF'  # Transport and Map symbols
        '\U00002600-\U000027BF'  # Misc symbols
        '\U0001F1E0-\U0001F1FF'  # Flags
        '\U0001FA70-\U0001FAFF'  # Symbols and Pictographs Extended-A
        '\U00002700-\U000027BF'  # Dingbats
        '\u2705'  # White heavy check mark ✅
        '\u274C'  # Cross mark ❌
        '\u2714'  # Heavy check mark ✔
        '\u2713'  # Check mark ✓
        '\u2611'  # Ballot box with check ☑
        '\u26A0'  # Warning sign ⚠
        '\u2139'  # Information source ℹ
        '\U0001F4CA'  # Bar chart 📊
        '\U0001F50D'  # Magnifying glass 🔍
        '\U0001F3AF'  # Dart 🎯
        '\U0001F3C6'  # Trophy 🏆
        '\U0001F4BE'  # Floppy disk 💾
        '\U0001F504'  # Counterclockwise arrows 🔄
        ']+',
        flags=re.UNICODE
    )
    return emoji_pattern.sub('', text)

def process_notebook(notebook_path):
    """Remove all emojis from a Jupyter notebook."""
    print(f'Processing {notebook_path}...')

    # Read notebook
    with open(notebook_path, 'r', encoding='utf-8') as f:
        nb = json.load(f)

    changes = 0
    cells_modified = 0

    for cell_idx, cell in enumerate(nb.get('cells', [])):
        source = cell.get('source', [])
        cell_changed = False

        if isinstance(source, list):
            new_source = []
            for line in source:
                cleaned_line = remove_emojis(line)
                if cleaned_line != line:
                    changes += 1
                    cell_changed = True
                new_source.append(cleaned_line)
            cell['source'] = new_source
        elif isinstance(source, str):
            cleaned_source = remove_emojis(source)
            if cleaned_source != source:
                changes += 1
                cell_changed = True
            cell['source'] = cleaned_source

        if cell_changed:
            cells_modified += 1

    # Write back
    with open(notebook_path, 'w', encoding='utf-8') as f:
        json.dump(nb, f, ensure_ascii=False, indent=1)

    print(f'  - Modified {cells_modified} cells')
    print(f'  - Removed emojis from {changes} lines')
    return changes

if __name__ == '__main__':
    base_path = Path(__file__).parent / 'src'

    notebooks = [
        'unsupervised.ipynb',
        'supervised.ipynb'
    ]

    total_changes = 0
    for notebook in notebooks:
        notebook_path = base_path / notebook
        if notebook_path.exists():
            changes = process_notebook(notebook_path)
            total_changes += changes
        else:
            print(f'Warning: {notebook_path} not found')

    print(f'\nTotal changes across all notebooks: {total_changes}')
