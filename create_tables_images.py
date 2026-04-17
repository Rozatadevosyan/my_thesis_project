import pandas as pd
import matplotlib.pyplot as plt

TABLE_FILES = [
    ("table_dataset_overview.csv", "Table 1. Dataset Overview", "table_dataset_overview.png", (8, 2.5)),
    ("table_read_time_comparison.csv", "Table 2. Read Time Comparison", "table_read_time_comparison.png", (9, 2.8)),
    ("table_filter_time_by_diagnosis.csv", "Table 3. Filter Time by Diagnosis", "table_filter_time_by_diagnosis.png", (12, 3.2)),
    ("table_patients_by_age_group.csv", "Table 4. Patients by Age Group", "table_patients_by_age_group.png", (8, 2.5)),
    ("table_visits_by_diagnosis.csv", "Table 5. Visits by Diagnosis", "table_visits_by_diagnosis.png", (9, 2.8)),
    ("table_write_time_comparison.csv", "Table 6. Write Time Comparison", "table_write_time_comparison.png", (8, 2.3)),
]

def draw_table(csv_file, title, output_file, fig_size):
    df = pd.read_csv(csv_file)

    fig, ax = plt.subplots(figsize=fig_size)
    ax.axis("off")
    ax.set_title(title, fontsize=12, fontweight="bold", pad=12)

    table = ax.table(
        cellText=df.values,
        colLabels=df.columns,
        cellLoc="center",
        loc="center"
    )

    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.15, 1.5)

    for (row, col), cell in table.get_celld().items():
        cell.set_linewidth(0.8)
        if row == 0:
            cell.set_text_props(weight="bold")
            cell.set_height(cell.get_height() * 1.15)

    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Saved: {output_file}")

for csv_file, title, output_file, fig_size in TABLE_FILES:
    draw_table(csv_file, title, output_file, fig_size)

print("All table images generated successfully.")