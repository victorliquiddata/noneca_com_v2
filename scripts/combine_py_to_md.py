#!/usr/bin/env python3
"""
Script: combine_py_to_md.py

- Recursively finds all .py files under a given root directory, excluding specified directories (e.g., .venv, scripts, tests)
- Filters to those with more than 5 lines, excluding specific filenames
- Prints a numbered list of these files
- Prompts the user to select either all files or specific ones by number
- Combines the selected files into a single Markdown file:
  * Starts with ```python
  * Inserts each file preceded by a commented divider (# --- <filename>)
  * Ends with ```
"""
import os
import argparse


def find_py_files(root, exclude_dirs=None, exclude_files=None):
    """
    Walk through `root` and its subdirectories, returning
    a list of all .py file paths containing more than 5 lines,
    while skipping any directories in `exclude_dirs` and
    ignoring filenames in `exclude_files`.
    """
    if exclude_dirs is None:
        exclude_dirs = {".venv", "scripts", "tests"}
    if exclude_files is None:
        exclude_files = {"combine_py_to_md.py"}

    py_files = []
    for dirpath, dirnames, filenames in os.walk(root):
        # Exclude specified directories from traversal
        dirnames[:] = [d for d in dirnames if d not in exclude_dirs]

        for name in filenames:
            if not name.endswith(".py") or name in exclude_files:
                continue
            path = os.path.join(dirpath, name)
            try:
                with open(path, "r", encoding="utf-8") as f:
                    lines = f.readlines()
                if len(lines) > 5:
                    py_files.append(path)
            except (IOError, UnicodeDecodeError) as e:
                print(f"Warning: could not read {path}: {e}")
    return py_files


def select_files(files):
    """
    Display a numbered list of `files` and prompt the user to select:
    - 'all' to choose every file
    - comma-separated numbers to choose specific files
    Returns the list of selected file paths.
    """
    print("Found Python files (>5 lines):")
    for idx, file in enumerate(files, start=1):
        print(f"{idx}. {file}")
    choice = input("\nEnter 'all' to select all, or comma-separated numbers: ")
    choice = choice.strip().lower()
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
    """
    Combine `selected_files` into a single Markdown file `output`.
    Uses:
      - A python code block starting with ```python
      - Commented dividers (# --- <filename>) between files
      - Closes the code block with ```
    """
    try:
        with open(output, "w", encoding="utf-8") as out:
            out.write("```python\n")
            for file in selected_files:
                out.write(f"# --- {file}\n")
                with open(file, "r", encoding="utf-8") as f:
                    out.write(f.read())
                    out.write("\n")
            out.write("```")
        print(f"Successfully wrote combined file to: {output}")
    except IOError as e:
        print(f"Error writing to {output}: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Combine .py files into a single Markdown file"
    )
    parser.add_argument(
        "root",
        nargs="?",
        default=".",
        help="Root directory to search (default: current directory)",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="combined.md",
        help="Output Markdown file name (default: combined.md)",
    )
    parser.add_argument(
        "-e",
        "--exclude",
        nargs="*",
        default=[".venv", "scripts", "tests"],
        help="Directories to exclude from search (default: .venv, scripts, tests)",
    )
    parser.add_argument(
        "-x",
        "--exclude-files",
        nargs="*",
        default=["combine_py_to_md.py"],
        help="Python filenames to ignore (default: combine_py_to_md.py)",
    )
    args = parser.parse_args()

    files = find_py_files(args.root, set(args.exclude), set(args.exclude_files))
    if not files:
        print("No .py files with more than 5 lines were found under:", args.root)
        return
    selected = select_files(files)
    if not selected:
        print("No files selected. Exiting.")
        return
    combine_to_md(selected, args.output)


if __name__ == "__main__":
    main()
