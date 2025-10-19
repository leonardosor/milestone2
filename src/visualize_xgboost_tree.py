"""
Standalone script to visualize XGBoost tree from saved model.
Run this script to generate readable XGBoost tree visualizations.
"""

import os
import sys
import json
import joblib
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import xgboost as xgb
from sklearn.compose import ColumnTransformer

# Setup paths
PROJECT_ROOT = Path(__file__).resolve().parent
MODEL_DIR = (PROJECT_ROOT / '..' / 'supervised').resolve()

print(f"Looking for models in: {MODEL_DIR}")
print(f"=" * 70)

# Find the latest XGBoost model
xgb_models = sorted(MODEL_DIR.glob('xgboost_opt*.joblib'),
                    key=lambda p: p.stat().st_mtime,
                    reverse=True)

if not xgb_models:
    print("ERROR: No XGBoost model found!")
    print(f"Please ensure XGBoost model is saved in: {MODEL_DIR}")
    sys.exit(1)

latest_model_path = xgb_models[0]
print(f"Loading model: {latest_model_path.name}")

# Load the model
try:
    xgb_model = joblib.load(latest_model_path)
    print(f"✓ Model loaded successfully")
except Exception as e:
    print(f"ERROR loading model: {e}")
    sys.exit(1)

# Try to load metadata for feature names
meta_path = latest_model_path.with_suffix('.json')
feature_names_clean = None

if meta_path.exists():
    try:
        with open(meta_path, 'r') as f:
            metadata = json.load(f)
        print(f"✓ Metadata loaded")
    except Exception as e:
        print(f"Warning: Could not load metadata: {e}")
        metadata = {}
else:
    print(f"Warning: No metadata file found at {meta_path.name}")
    metadata = {}

# Generate feature names (fallback if not in metadata)
n_features = xgb_model.n_features_in_
print(f"Model has {n_features} features")

# Create feature names
feature_names = [f"f{i}" for i in range(n_features)]
feature_names_clean = [f"Feature {i}" for i in range(n_features)]

print(f"\n{'=' * 70}")
print("XGBoost Model Information:")
print(f"{'=' * 70}")
print(f"Total number of trees (estimators): {xgb_model.n_estimators}")
print(f"Max tree depth: {xgb_model.max_depth}")
print(f"Learning rate: {xgb_model.learning_rate}")
print(f"Number of features: {n_features}")

# Create visualizations
print(f"\n{'=' * 70}")
print("Creating XGBoost Tree Visualizations...")
print(f"{'=' * 70}")

# 1. Multiple tree views in a 2x2 grid
print("\n1. Creating 2x2 grid with multiple tree views...")
fig, axes = plt.subplots(2, 2, figsize=(24, 20))
fig.suptitle('XGBoost Decision Trees - Multiple Views',
             fontsize=20, fontweight='bold', y=0.995)

# Tree 0: Full detail
ax1 = axes[0, 0]
xgb.plot_tree(xgb_model, num_trees=0, ax=ax1, rankdir='TB', fontsize=10)
ax1.set_title('Tree 0: Full Detail View', fontsize=14, fontweight='bold', pad=10)

# Tree 1: Alternative structure
ax2 = axes[0, 1]
xgb.plot_tree(xgb_model, num_trees=1, ax=ax2, rankdir='TB', fontsize=10)
ax2.set_title('Tree 1: Alternative Tree Structure', fontsize=14, fontweight='bold', pad=10)

# Tree 2: Horizontal layout
ax3 = axes[1, 0]
xgb.plot_tree(xgb_model, num_trees=2, ax=ax3, rankdir='LR', fontsize=9)
ax3.set_title('Tree 2: Horizontal Layout', fontsize=14, fontweight='bold', pad=10)

# Tree 5 or last tree
tree_idx = min(5, xgb_model.n_estimators - 1)
ax4 = axes[1, 1]
xgb.plot_tree(xgb_model, num_trees=tree_idx, ax=ax4, rankdir='TB', fontsize=10)
ax4.set_title(f'Tree {tree_idx}: Later Tree (More Refined Splits)',
              fontsize=14, fontweight='bold', pad=10)

plt.tight_layout()
plt.subplots_adjust(top=0.98)

# Save the figure
output_path_grid = MODEL_DIR / 'xgboost_trees_grid.png'
plt.savefig(output_path_grid, dpi=150, bbox_inches='tight')
print(f"   ✓ Saved: {output_path_grid.name}")
plt.show()

# 2. Single large tree for maximum detail
print("\n2. Creating single large tree visualization...")
fig, ax = plt.subplots(1, 1, figsize=(30, 24))
xgb.plot_tree(xgb_model, num_trees=0, ax=ax, rankdir='TB', fontsize=12)
ax.set_title('XGBoost Tree 0: Maximum Detail View\n(Best Performing Model)',
             fontsize=18, fontweight='bold', pad=15)
plt.tight_layout()

output_path_large = MODEL_DIR / 'xgboost_tree0_large.png'
plt.savefig(output_path_large, dpi=150, bbox_inches='tight')
print(f"   ✓ Saved: {output_path_large.name}")
plt.show()

# 3. Feature Importance visualization
if hasattr(xgb_model, 'feature_importances_'):
    print("\n3. Creating feature importance visualization...")

    importance_df = pd.DataFrame({
        'Feature': feature_names_clean,
        'Importance': xgb_model.feature_importances_
    }).sort_values('Importance', ascending=False).head(15)

    plt.figure(figsize=(12, 8))
    bars = plt.barh(range(len(importance_df)), importance_df['Importance'].values,
                   color='steelblue', edgecolor='navy', linewidth=1.5, alpha=0.8)
    plt.yticks(range(len(importance_df)), importance_df['Feature'].values, fontsize=11)
    plt.xlabel('Importance Score', fontsize=13, fontweight='bold')
    plt.title('XGBoost Feature Importance\n(Gain-based)',
             fontsize=16, fontweight='bold', pad=15)
    plt.grid(axis='x', linestyle='--', alpha=0.4)

    # Add value labels on bars
    for idx, (bar, val) in enumerate(zip(bars, importance_df['Importance'].values)):
        plt.text(val + 0.002, bar.get_y() + bar.get_height()/2,
                f'{val:.4f}',
                ha='left', va='center', fontsize=10, fontweight='bold')

    plt.gca().invert_yaxis()
    plt.tight_layout()

    output_path_importance = MODEL_DIR / 'xgboost_feature_importance.png'
    plt.savefig(output_path_importance, dpi=150, bbox_inches='tight')
    print(f"   ✓ Saved: {output_path_importance.name}")
    plt.show()

    # Print top 10 features
    print(f"\n{'=' * 70}")
    print("Top 10 Most Important Features:")
    print(f"{'=' * 70}")
    for idx, row in importance_df.head(10).iterrows():
        print(f"  {idx+1:2d}. {row['Feature']:30s} → {row['Importance']:.4f}")

print(f"\n{'=' * 70}")
print("Visualization Complete!")
print(f"{'=' * 70}")
print(f"\nOutput files saved to: {MODEL_DIR}")
print(f"  - xgboost_trees_grid.png (2x2 grid)")
print(f"  - xgboost_tree0_large.png (single large tree)")
print(f"  - xgboost_feature_importance.png (feature importance)")
print("\nYou can open these PNG files to view the visualizations.")
