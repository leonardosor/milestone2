import json

with open("supervised_training.ipynb", "r", encoding="utf-8", errors="ignore") as f:
    nb = json.load(f)

for cell in nb["cells"]:
    if cell["cell_type"] == "code" and "shap.dependence_plot" in "".join(
        cell["source"]
    ):
        for i, line in enumerate(cell["source"]):
            if "X_train," in line:
                cell["source"][i] = line.replace("X_train,", "X_train_enc,")
            if '"read_high_pct"' in line:
                cell["source"][i] = line.replace(
                    '"read_high_pct"', '"num__read_high_pct"'
                )
            if 'interaction_index="math_counts"' in line:
                cell["source"][i] = line.replace(
                    'interaction_index="math_counts"',
                    'feature_names=feature_names,\n    interaction_index="num__math_counts"',
                )
        break

with open("supervised_training.ipynb", "w", encoding="utf-8") as f:
    json.dump(nb, f, indent=1, ensure_ascii=False)

print("SHAP dependence plot fixed")
