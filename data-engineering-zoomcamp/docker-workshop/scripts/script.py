from pathlib import Path

current_dir  = Path.cwd()
current_file = Path(__file__).name

print(f"file in {current_dir}")


