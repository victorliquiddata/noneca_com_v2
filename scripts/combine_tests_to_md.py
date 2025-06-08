#!/usr/bin/env python3
"""
Script: combine_tests_to_md.py

- Searches only the `tests` directory (relative to this script) for .py files with more than 5 lines
- Prints a numbered list of these test files
- Prompts the user to select either all files or specific ones by number
- Combines the selected test files into a single Markdown file in the `tests` folder
- Optionally runs the test suite via `comp_test.py` and appends its output
- Saves a final combined Markdown with results as `combined_tests_with_results.md` if tests run

Usage:
  cd scripts
  ./combine_tests_to_md.py [-r TEST_DIR] [-o OUTPUT] [-x EXCLUDE]
"""
import os
import argparse
import sys
import subprocess


def find_py_files(test_dir, min_lines=5, exclude_files=None):
    if exclude_files is None:
        exclude_files = set()

    py_files = []
    for dirpath, _, filenames in os.walk(test_dir):
        for name in filenames:
            if not name.endswith(".py") or name in exclude_files:
                continue
            path = os.path.join(dirpath, name)
            try:
                with open(path, "r", encoding="utf-8") as f:
                    lines = f.readlines()
                if len(lines) > min_lines:
                    rel_path = os.path.relpath(path, start=test_dir)
                    py_files.append((path, rel_path))
            except (IOError, UnicodeDecodeError) as e:
                print(f"Warning: could not read {path}: {e}")
    return py_files


def select_files(files):
    print("Found test Python files (>5 lines):")
    for idx, (_, rel) in enumerate(files, start=1):
        print(f"{idx}. {rel}")
    choice = (
        input("\nEnter 'all' to select all, or comma-separated numbers: ")
        .strip()
        .lower()
    )
    if choice == "all":
        return files

    selected = []
    for part in choice.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            num = int(part)
            if 1 <= num <= len(files):
                selected.append(files[num - 1])
            else:
                print(f"Warning: {num} is out of range, ignored.")
        except ValueError:
            print(f"Warning: '{part}' is not a valid number, ignored.")
    return selected


def combine_to_md(selected_files, output):
    try:
        with open(output, "w", encoding="utf-8") as out:
            out.write("```python\n")
            for full, rel in selected_files:
                out.write(f"# --- {rel}\n")
                with open(full, "r", encoding="utf-8") as f:
                    out.write(f.read())
                    out.write("\n")
            out.write("```\n")
        print(f"Initial Markdown created at: {output}")
    except IOError as e:
        print(f"Error writing to {output}: {e}")
        sys.exit(1)


def run_tests_and_append(initial_md, test_script, final_md, cwd):
    print(f"Running tests via: {test_script}")
    try:
        result = subprocess.run(
            [sys.executable, test_script], capture_output=True, text=True, cwd=cwd
        )
        output = result.stdout + result.stderr
    except Exception as e:
        output = f"Error running tests: {e}\n"

    try:
        with open(initial_md, "r", encoding="utf-8") as f:
            content = f.read()
        with open(final_md, "w", encoding="utf-8") as out:
            out.write(content)
            out.write("\n\n# Test Results\n```\n")
            out.write(output)
            out.write("```\n")
        print(f"Final Markdown with results saved at: {final_md}")
    except IOError as e:
        print(f"Error writing final markdown: {e}")
        sys.exit(1)


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.normpath(os.path.join(script_dir, os.pardir))
    default_test_dir = os.path.join(project_root, "tests")

    parser = argparse.ArgumentParser(
        description="Combine test .py files into Markdown and optionally append test results"
    )
    parser.add_argument(
        "-r", "--root", default=default_test_dir, help="Tests directory to search"
    )
    parser.add_argument(
        "-o",
        "--output",
        default=os.path.join(default_test_dir, "combined_tests.md"),
        help="Initial output Markdown path",
    )
    parser.add_argument(
        "-x",
        "--exclude-files",
        nargs="*",
        default=[],
        help="Specific test filenames to ignore",
    )
    args = parser.parse_args()

    if not os.path.isdir(args.root):
        print(f"Error: tests directory not found: {args.root}")
        sys.exit(1)

    files = find_py_files(args.root, exclude_files=set(args.exclude_files))
    if not files:
        print("No .py files with more than 5 lines were found under:", args.root)
        return

    selected = select_files(files)
    if not selected:
        print("No files selected. Exiting.")
        return

    initial_md = args.output
    combine_to_md(selected, initial_md)

    choice = (
        input("\nRun comp_test.py in tests and append results? (y/n): ").strip().lower()
    )
    if choice == "y":
        test_script = os.path.join(args.root, "comp_test.py")
        if not os.path.isfile(test_script):
            print(f"Test runner not found: {test_script}")
            sys.exit(1)
        final_md = os.path.join(args.root, "combined_tests_with_results.md")
        run_tests_and_append(initial_md, test_script, final_md, project_root)
    else:
        print("Skipping test run. Done.")


if __name__ == "__main__":
    main()
