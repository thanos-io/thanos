#!/bin/bash
#
# This script removes a method from a Go source file.
# It'''s a simple implementation and might fail on complex cases,
# especially if the code is not gofmt-ed.
#
# Usage: ./scripts/remove_method.sh <file.go> <TypeName> <MethodName>

set -euo pipefail

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <file.go> <TypeName> <MethodName>"
    exit 1
fi

file="$1"
type_name="$2"
method_name="$3"

if [ ! -f "$file" ]; then
    echo "File not found: $file"
    exit 1
fi

# Find the line number where the method starts.
# This regex is flexible enough for pointer or value receivers.
# It takes the first match in case of duplicates in comments.
start_line=$(grep -n "func (.*${type_name}) ${method_name}(" "$file" | head -n 1 | cut -d: -f1)

if [ -z "$start_line" ]; then
    echo "Method '''${method_name}''' for type '''${type_name}''' not found in '''${file}'''."
    exit 0 # Exit gracefully if not found.
fi

# From the start line, find the end line by matching braces.
# This awk script starts at '''start_line''', counts opening and closing braces,
# and prints the line number where the brace count returns to zero.
end_line=$(awk -v start="${start_line}" '
    NR >= start {
        # Count braces on the current line
        brace_count += gsub(/{/, "{");
        
        # If we have not found any opening brace yet, just continue.
        # This handles the case where the opening brace is on a different line
        # than the function signature.
        if (brace_count == 0) {
            next;
        }

        brace_count -= gsub(/}/, "}");

        if (brace_count == 0) {
            print NR;
            exit;
        }
    }
' "$file")

if [ -z "$end_line" ] || [ "$end_line" -eq 0 ]; then
    echo "Error: Could not find the end of method '''${method_name}''' starting on line ${start_line}."
    exit 1
fi

# Use sed to delete the method from the file.
# Using a temp file for portability of `sed -i`.
tmp_file=$(mktemp)
sed "${start_line},${end_line}d" "$file" > "$tmp_file"
mv "$tmp_file" "$file"

echo "Successfully removed method '''${method_name}''' from '''${file}''' (lines ${start_line}-${end_line})."
