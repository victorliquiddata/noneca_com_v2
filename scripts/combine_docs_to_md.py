#!/usr/bin/env python3
"""
Script: combine_markdown_docs.py

- Searches for Markdown files in the project root and /docs (excluding /docs/archive)
- Lists found files (README.md + eligible .md files under /docs)
- Prompts the user to select all or specific files by number
- Combines the selected Markdown files into a single file with section headers
- Saves the combined Markdown to /docs/combined_docs.md

Usage:
  cd <project_root>
  ./scripts/combine_markdown_docs.py [-r ROOT] [-o OUTPUT]
"""
import os
import argparse
import sys


def find_md_files(root_dir, docs_subdir="docs", exclude_dir="archive"):
    """
    Collect README.md at project root and .md files under docs/, excluding docs/archive.
    Returns list of (full_path, rel_path).
    """
    files = []
    # README.md at root
    readme = os.path.join(root_dir, "README.md")
    if os.path.isfile(readme):
        files.append((readme, "README.md"))

    docs_dir = os.path.join(root_dir, docs_subdir)
    if not os.path.isdir(docs_dir):
        return files

    for dirpath, dirnames, filenames in os.walk(docs_dir):
        # skip excluded directory
        # modify dirnames in-place to prevent os.walk from recursing into it
        dirnames[:] = [d for d in dirnames if d != exclude_dir]
        for name in filenames:
            if not name.endswith(".md"):
                continue
            full = os.path.join(dirpath, name)
            # compute relative path from project root
            rel = os.path.relpath(full, start=root_dir)
            files.append((full, rel))
    return files


def select_files(files):
    """
    Prompt user to select files from a numbered list.
    Accept 'all' or comma-separated indices.
    """
    if not files:
        return []

    print("Found Markdown files:")
    for idx, (_, rel) in enumerate(files, start=1):
        print(f"  {idx}. {rel}")

    choice = (
        input("\nEnter 'all' to include all, or comma-separated numbers: ")
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
            i = int(part)
            if 1 <= i <= len(files):
                selected.append(files[i - 1])
            else:
                print(f"Warning: {i} is out of range, ignored.")
        except ValueError:
            print(f"Warning: '{part}' is not a valid number, ignored.")
    return selected


def combine_to_md(selected, output_path, project_root):
    """
    Combine selected Markdown files into one, with headers.
    """
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as out:
            for full, rel in selected:
                out.write(f"## File: {rel}\n\n")
                with open(full, "r", encoding="utf-8") as f:
                    out.write(f.read().rstrip())
                    out.write("\n\n")
        print(f"Combined Markdown saved to: {output_path}")
    except IOError as e:
        print(f"Error writing combined file: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Combine selected Markdown files into a single document"
    )
    cwd = os.getcwd()
    default_root = cwd
    default_output = os.path.join(cwd, "docs", "combined_docs.md")

    parser.add_argument(
        "-r",
        "--root",
        default=default_root,
        help="Project root directory to search (default: current directory)",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=default_output,
        help="Output path for combined Markdown (default: docs/combined_docs.md)",
    )
    args = parser.parse_args()

    root = os.path.abspath(args.root)
    files = find_md_files(root)
    if not files:
        print("No Markdown files found.")
        sys.exit(0)

    selected = select_files(files)
    if not selected:
        print("No files selected. Exiting.")
        sys.exit(0)

    combine_to_md(selected, os.path.abspath(args.output), root)


if __name__ == "__main__":
    main()
