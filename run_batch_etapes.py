import subprocess
import os

run_id = os.getenv("RUN_ID")

commands = [
    ["python", "src/main.py", "--runId", run_id, "--step", "1"],
    ["python", "src/main.py", "--runId", run_id, "--step", "2"],
    ["python", "src/main.py", "--runId", run_id, "--step", "3"],
    ["python", "src/main.py", "--runId", run_id, "--step", "4"],
    ["python", "src/main.py", "--runId", run_id, "--step", "5"],
]

processes = []

for cmd in commands:
    p = subprocess.Popen(cmd)
    processes.append(p)

# Optionnel : attendre la fin de tous les process
for p in processes:
    p.wait()
