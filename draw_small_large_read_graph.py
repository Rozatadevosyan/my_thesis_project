import json
import matplotlib.pyplot as plt

with open("results/small_large_read_results.json", "r") as f:
    data = json.load(f)

categories = [
    "Non-partitioned\nRead",
    "Partitioned\nRead",
    "Non-partitioned\n2nd Run",
    "Partitioned\n2nd Run"
]

small_times = data["small_times"]
large_times = data["large_times"]

x = range(len(categories))
width = 0.35

plt.figure(figsize=(10, 6))
plt.bar([i - width/2 for i in x], small_times, width=width, label="Small Dataset")
plt.bar([i + width/2 for i in x], large_times, width=width, label="Large Dataset")

plt.title("Read Time Comparison")
plt.ylabel("Time (ms)")
plt.xticks(list(x), categories)
plt.legend()

for i, v in enumerate(small_times):
    plt.text(i - width/2, v + 10, f"{v:.0f}", ha="center", fontsize=8)

for i, v in enumerate(large_times):
    plt.text(i + width/2, v + 10, f"{v:.0f}", ha="center", fontsize=8)

plt.tight_layout()
plt.savefig("graph_small_large_read_comparison.png", dpi=300)
plt.show()