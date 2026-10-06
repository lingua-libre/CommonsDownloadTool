#!/usr/bin/env python3
import argparse
import re
import sys
import os

VERSION = "1.0.0"

MANUAL = """
titles_filterer.py — Manual

Description:
    Read a source file containing one title per line, filter lines matching a
    provided regular expression, and write matches to an output file.

Synopsis:
    python3 helpers/titles_filterer.py [OPTIONS]

Options:
    --input, -i       Source file with titles (default: ./tmp/titles.txt)
    --regex, -p, -r   Regex to search for (slashes optional) (required)
    --output, -o      Destination file for matches (default: ./tmp/filtered_titles.txt)
    --dry-run         Count matches without writing output
    --verbose, -v     Enable verbose output
    --version         Show version and exit
    --help, -h        Show this manual and exit

Examples:
    python3 helpers/titles_filterer.py -p 'LL-Q117707514' -o matches.txt
    python3 helpers/titles_filterer.py -p '/pattern/' -i /path/to/file.txt -o ./tmp/filtered_titles.txt

Notes:
    - If the REGEX is wrapped in leading and trailing slashes they'll be removed
      before compiling (e.g. /foo/ -> foo).
    - Exit codes: 0 success, 2 invalid regex, 3 source missing, 4 output write error.
"""


def normalize_pattern(pat: str) -> str:
    if len(pat) >= 2 and pat.startswith("/") and pat.endswith("/"):
        return pat[1:-1]
    return pat


def main():
    parser = argparse.ArgumentParser(description="Filter title lines by regex and write matches to a file", add_help=False)
    parser.add_argument("--input", "-i", default="./tmp/titles.txt", help="Source file with titles (one per line)")
    parser.add_argument("--regex", "-p", "-r", required=True, help="Regex to search for (slashes optional)")
    parser.add_argument("--output", "-o", default="./tmp/filtered_titles.txt", help="Destination file for matches")
    parser.add_argument("--dry-run", action="store_true", help="Count matches without writing output")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose output")
    parser.add_argument("--version", action="store_true", help="Show version and exit")
    parser.add_argument("--help", "-h", action="store_true", help="Show embedded manual and exit")
    args = parser.parse_args()

    if args.version:
        print(f"titles_filterer.py version {VERSION}")
        return 0

    if args.help:
        print(MANUAL)
        return 0

    if args.verbose:
        print(f"Verbose mode enabled")
        print(f"Input: {args.input}")
        print(f"Regex: {args.regex}")
        print(f"Output: {args.output}")

    pattern = normalize_pattern(args.regex)
    try:
        rx = re.compile(pattern)
    except re.error as e:
        print(f"Invalid regex: {e}", file=sys.stderr)
        return 2

    try:
        if not args.dry_run:
            os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
        
        with open(args.input, encoding="utf-8") as f:
            matched = 0
            if args.dry_run:
                # Just count matches
                for line in f:
                    if rx.search(line):
                        matched += 1
            else:
                with open(args.output, "w", encoding="utf-8") as out:
                    for line in f:
                        if rx.search(line):
                            matched += 1
                            out.write(line)
            
        if args.dry_run:
            print(f"[DRY RUN] Would write {matched} matches to {args.output}")
        else:
            print(f"{matched} selected records to work on.")
        
        if args.verbose:
            print(f"Total matches: {matched}")
        
        return 0
    except FileNotFoundError:
        print(f"Source file not found: {args.input}", file=sys.stderr)
        return 3
    except OSError as e:
        print(f"Unable to write output file: {e}", file=sys.stderr)
        return 4


if __name__ == "__main__":
    sys.exit(main())
