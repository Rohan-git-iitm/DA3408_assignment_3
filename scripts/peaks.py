import csv
import sys
from collections import defaultdict

UNITS = {"GiB": 1024, "MiB": 1, "KiB": 1 / 1024, "B": 1 / 2**20}


def to_mib(text):
    used = text.split("/")[0].strip()
    for unit, factor in UNITS.items():
        if used.endswith(unit):
            return float(used[:-len(unit)]) * factor


cpu, mem = defaultdict(float), defaultdict(float)
total_cpu, total_mem = defaultdict(float), defaultdict(float)

for ts, name, c, m in csv.reader(open(sys.argv[1])):
    c, m = float(c.rstrip("%")), to_mib(m)
    cpu[name] = max(cpu[name], c)
    mem[name] = max(mem[name], m)
    total_cpu[ts] += c
    total_mem[ts] += m

for name in sorted(cpu):
    print(f"{name}: peak CPU {cpu[name]:.0f}%, peak memory {mem[name]:.0f} MiB")
print(f"cluster: peak CPU {max(total_cpu.values()):.0f}%, peak memory {max(total_mem.values()):.0f} MiB")