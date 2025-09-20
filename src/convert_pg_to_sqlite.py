#!/usr/bin/env python3
import re
import sys

def convert_pg_to_sqlite(input_file, output_file):
    print(f"Converting {input_file} to SQLite format...")
    
    # Get file size for progress tracking
    import os
    file_size = os.path.getsize(input_file)
    print(f"Original file size: {file_size:,} bytes ({file_size / (1024**3):.2f} GB)")
    
    # Process file in chunks to handle large files
    chunk_size = 1024 * 1024  # 1MB chunks
    processed_bytes = 0
    
    # PostgreSQL to SQLite conversions
    conversions = [
        # Remove PostgreSQL-specific commands
        (r'SET [^;]*;', ''),
        (r'SELECT pg_catalog\..*?;', ''),
        (r'CREATE SCHEMA [^;]*;', ''),
        (r'ALTER TABLE [^;]*? OWNER TO [^;]*;', ''),
        (r'GRANT [^;]*;', ''),
        (r'REVOKE [^;]*;', ''),
        (r'COMMENT ON [^;]*;', ''),
        (r'CREATE EXTENSION [^;]*;', ''),
        (r'DROP EXTENSION [^;]*;', ''),
        
        # Data type conversions
        (r'\bSERIAL\b', 'INTEGER PRIMARY KEY AUTOINCREMENT'),
        (r'\bBIGSERIAL\b', 'INTEGER PRIMARY KEY AUTOINCREMENT'),
        (r'\bSMALLSERIAL\b', 'INTEGER PRIMARY KEY AUTOINCREMENT'),
        (r'\bBOOLEAN\b', 'INTEGER'),
        (r'\bTEXT\[\]', 'TEXT'),
        (r'\bINTEGER\[\]', 'TEXT'),
        (r'\bTIMESTAMP WITH TIME ZONE\b', 'TIMESTAMP'),
        (r'\bTIMESTAMP WITHOUT TIME ZONE\b', 'TIMESTAMP'),
        (r'\bCHARACTER VARYING\b', 'TEXT'),
        (r'\bVARCHAR\b', 'TEXT'),
        
        # Remove constraints and indexes that might cause issues
        (r'ADD CONSTRAINT [^;]*;', ''),
        (r'ALTER TABLE [^;]*? ADD FOREIGN KEY [^;]*;', ''),
        (r'CREATE INDEX [^;]*;', ''),  # Remove for now, can add back later
        
        # Fix sequence issues
        (r"nextval\('[^']*'\)", 'NULL'),
        (r'CREATE SEQUENCE [^;]*;', ''),
        (r'ALTER SEQUENCE [^;]*;', ''),
        (r'SELECT setval\([^;]*;', ''),
        
        # Boolean values
        (r'\btrue\b', '1'),
        (r'\bfalse\b', '0'),
        
        # Remove PostgreSQL-specific functions
        (r'now\(\)', "datetime('now')"),
        (r'CURRENT_TIMESTAMP', "datetime('now')"),
        
        # Handle COPY statements (convert to INSERT)
        (r'COPY [^;]*;', '-- COPY statement removed'),
        (r'\\\.', '-- End of COPY data'),
    ]
    
    # Compile regex patterns for better performance
    compiled_conversions = [(re.compile(pattern, re.IGNORECASE | re.MULTILINE), replacement) 
                           for pattern, replacement in conversions]
    
    with open(input_file, 'r', encoding='utf-8') as input_f, \
         open(output_file, 'w', encoding='utf-8') as output_f:
        
        buffer = ""
        line_count = 0
        
        while True:
            chunk = input_f.read(chunk_size)
            if not chunk:
                break
                
            processed_bytes += len(chunk.encode('utf-8'))
            buffer += chunk
            
            # Process complete lines to avoid breaking SQL statements
            lines = buffer.split('\n')
            buffer = lines[-1]  # Keep incomplete line in buffer
            complete_lines = lines[:-1]
            
            # Apply conversions to complete lines
            for line in complete_lines:
                line_count += 1
                converted_line = line
                
                for pattern, replacement in compiled_conversions:
                    converted_line = pattern.sub(replacement, converted_line)
                
                output_f.write(converted_line + '\n')
            
            # Progress indicator
            if line_count % 10000 == 0:
                progress = (processed_bytes / file_size) * 100
                print(f"Progress: {progress:.1f}% - Processed {line_count:,} lines")
        
        # Process any remaining content in buffer
        if buffer:
            for pattern, replacement in compiled_conversions:
                buffer = pattern.sub(replacement, buffer)
            output_f.write(buffer)
    
    print(f"Conversion complete! Output saved to {output_file}")
    print(f"Processed {line_count:,} lines")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python convert_pg_to_sqlite.py input_file.sql output_file.sql")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    convert_pg_to_sqlite(input_file, output_file)