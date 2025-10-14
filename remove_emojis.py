import json
import re
import sys

def remove_emojis_from_notebook(notebook_path):
    """Remove all emojis from a Jupyter notebook."""
    # Read notebook
    with open(notebook_path, 'r', encoding='utf-8') as f:
        nb = json.load(f)

    # Comprehensive emoji pattern
    emoji_pattern = re.compile(
        '['
        '\U0001F300-\U0001F9FF'  # Misc Symbols and Pictographs
        '\U0001F600-\U0001F64F'  # Emoticons
        '\U0001F680-\U0001F6FF'  # Transport and Map symbols
        '\U00002600-\U000027BF'  # Misc symbols
        '\U0001F1E0-\U0001F1FF'  # Flags
        '\U0001FA70-\U0001FAFF'  # Symbols and Pictographs Extended-A
        '\U00002700-\U000027BF'  # Dingbats
        '\u2705'  # White heavy check mark
        '\u274C'  # Cross mark
        '\u2714'  # Heavy check mark
        '\u2611'  # Ballot box with check
        '\u26A0'  # Warning sign
        '\u2139'  # Information source
        ']+',
        flags=re.UNICODE
    )

    changes = 0
    for cell in nb.get('cells', []):
        source = cell.get('source', [])
        if isinstance(source, list):
            new_source = []
            for line in source:
                new_line = emoji_pattern.sub('', line)
                if new_line != line:
                    changes += 1
                new_source.append(new_line)
            cell['source'] = new_source
        elif isinstance(source, str):
            new_source = emoji_pattern.sub('', source)
            if new_source != source:
                changes += 1
            cell['source'] = new_source

    # Write back
    with open(notebook_path, 'w', encoding='utf-8') as f:
        json.dump(nb, f, ensure_ascii=False, indent=1)

    return changes

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python remove_emojis.py <notebook_path>')
        sys.exit(1)

    notebook_path = sys.argv[1]
    changes = remove_emojis_from_notebook(notebook_path)
    print(f'Removed emojis from {changes} locations in {notebook_path}')
