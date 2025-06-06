import subprocess

commands = [
    ["python", "src/main.py", "--runId", "JD01", "--step", "1"],
    ["python", "src/main.py", "--runId", "JD01", "--step", "2"],
    ["python", "src/main.py", "--runId", "JD01", "--step", "3"],
    ["python", "src/main.py", "--runId", "JD01", "--step", "4"],
    ["python", "src/main.py", "--runId", "JD01", "--step", "5"],
]

processes = []

for cmd in commands:
    p = subprocess.Popen(cmd)
    processes.append(p)

# Optionnel : attendre la fin de tous les process
for p in processes:
    p.wait()
