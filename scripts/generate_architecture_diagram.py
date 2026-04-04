"""
Architecture Diagram Generator
Lending Portfolio Risk Intelligence Platform

Run from 08 project/ folder:
    uv run scripts/generate_architecture_diagram.py

Output: docs/architecture.png
"""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch

fig, ax = plt.subplots(figsize=(14, 18))
ax.set_xlim(0, 14)
ax.set_ylim(0, 18)
ax.axis('off')
fig.patch.set_facecolor('white')
ax.set_facecolor('white')

# ── Colour palette ─────────────────────────────────────────────
COLORS = {
    'source':    ('#dbeafe', '#1d4ed8'),  # blue
    'infra':     ('#dcfce7', '#15803d'),  # green
    'orchestr':  ('#ede9fe', '#6d28d9'),  # purple
    'stream':    ('#fee2e2', '#b91c1c'),  # red
    'dbt':       ('#d1fae5', '#065f46'),  # teal
    'spark':     ('#fef3c7', '#92400e'),  # amber
    'dashboard': ('#dbeafe', '#1e40af'),  # blue
    'arrow':     '#374151',
    'title':     '#111827',
    'subtitle':  '#6b7280',
}

def draw_box(ax, x, y, w, h, bg, border, title, subtitle=''):
    rect = FancyBboxPatch(
        (x, y), w, h,
        boxstyle="round,pad=0.15",
        linewidth=2,
        edgecolor=border,
        facecolor=bg
    )
    ax.add_patch(rect)
    if subtitle:
        ax.text(x + w/2, y + h * 0.65, title,
                ha='center', va='center',
                fontsize=11, fontweight='bold', color=border,
                fontfamily='DejaVu Sans')
        ax.text(x + w/2, y + h * 0.28, subtitle,
                ha='center', va='center',
                fontsize=8.5, color='#374151',
                fontfamily='DejaVu Sans')
    else:
        ax.text(x + w/2, y + h/2, title,
                ha='center', va='center',
                fontsize=11, fontweight='bold', color=border,
                fontfamily='DejaVu Sans')

def draw_arrow(ax, x, y1, y2, label=''):
    ax.annotate('',
        xy=(x, y2), xytext=(x, y1),
        arrowprops=dict(
            arrowstyle='->', color=COLORS['arrow'],
            lw=1.8,
        )
    )
    if label:
        ax.text(x + 0.2, (y1 + y2) / 2, label,
                ha='left', va='center',
                fontsize=8, color=COLORS['subtitle'],
                style='italic')

def draw_split_arrow(ax, x_from, y_from, x_to, y_to):
    ax.annotate('',
        xy=(x_to, y_to), xytext=(x_from, y_from),
        arrowprops=dict(
            arrowstyle='->', color=COLORS['arrow'],
            lw=1.8,
            connectionstyle='arc3,rad=0'
        )
    )

# ══════════════════════════════════════════════════════════════
# TITLE
# ══════════════════════════════════════════════════════════════
ax.text(7, 17.5, 'Lending Portfolio Risk Intelligence Platform',
        ha='center', va='center',
        fontsize=15, fontweight='bold', color=COLORS['title'])
ax.text(7, 17.1, 'End-to-End Data Pipeline  —  DE Zoomcamp 2026  —  Daniel Jude',
        ha='center', va='center',
        fontsize=9, color=COLORS['subtitle'])

# ── Horizontal line under title ───────────────────────────────
ax.plot([0.5, 13.5], [16.85, 16.85], color='#e5e7eb', lw=1.5)

# ══════════════════════════════════════════════════════════════
# LAYER 1 — DATA SOURCE
# ══════════════════════════════════════════════════════════════
bg, bd = COLORS['source']
draw_box(ax, 3, 15.6, 8, 1.0, bg, bd,
         '📂  Data Source',
         'Kaggle Lending Club CSV  |  2.26M rows  |  2007–2011')

draw_arrow(ax, 7, 15.6, 14.95, 'download & filter')

# ══════════════════════════════════════════════════════════════
# LAYER 2 — INFRASTRUCTURE
# ══════════════════════════════════════════════════════════════
bg, bd = COLORS['infra']
draw_box(ax, 3, 14.2, 8, 1.0, bg, bd,
         '🏗️  Infrastructure  (Terraform)',
         'GCS Bucket  +  4 BigQuery Datasets  +  Partitioned Table')

draw_arrow(ax, 7, 14.2, 13.55, 'provision once')

# ══════════════════════════════════════════════════════════════
# LAYER 3 — ORCHESTRATION
# ══════════════════════════════════════════════════════════════
bg, bd = COLORS['orchestr']
draw_box(ax, 3, 12.8, 8, 1.0, bg, bd,
         '🎯  Orchestration  (Kestra)',
         'Flow 1: Ingest  |  Flow 2: Schedule (daily)  |  Flow 3: Run dbt')

# Split arrows from Kestra
draw_split_arrow(ax, 4.5, 12.8, 2.5, 11.95)   # → Streaming
draw_split_arrow(ax, 9.5, 12.8, 11.5, 11.95)  # → dbt

ax.text(1.5, 12.35, 'stream\nevents', ha='center', va='center',
        fontsize=7.5, color=COLORS['subtitle'], style='italic')
ax.text(12.5, 12.35, 'transform\ndata', ha='center', va='center',
        fontsize=7.5, color=COLORS['subtitle'], style='italic')

# ══════════════════════════════════════════════════════════════
# LAYER 4 — STREAMING (left) + DBT (right)
# ══════════════════════════════════════════════════════════════
bg, bd = COLORS['stream']
draw_box(ax, 0.4, 10.8, 5.5, 1.1, bg, bd,
         '🌊  Streaming',
         'Redpanda (Kafka)  +  PyFlink\nTumbling & Session Windows  →  PostgreSQL')

bg, bd = COLORS['dbt']
draw_box(ax, 8.1, 10.8, 5.5, 1.1, bg, bd,
         '🔄  Transformation  (dbt)',
         'stg_loans (view)\n→ fct_loans → mart_loan_risk (tables)')

# Arrow from dbt down to spark
draw_arrow(ax, 10.85, 10.8, 10.15, '')
ax.text(11.5, 10.48, 'mart\nready', ha='center', va='center',
        fontsize=7.5, color=COLORS['subtitle'], style='italic')

# ══════════════════════════════════════════════════════════════
# LAYER 5 — SPARK
# ══════════════════════════════════════════════════════════════
bg, bd = COLORS['spark']
draw_box(ax, 3, 8.9, 8, 1.1, bg, bd,
         '⚡  Batch Processing  (PySpark)',
         'Q1: Default rate by grade  |  Q2: Monthly volume\nQ3: DPD buckets (RDD API)  |  Q4: Risk tier summary')

draw_arrow(ax, 7, 8.9, 8.25, 'results')

# ══════════════════════════════════════════════════════════════
# LAYER 6 — DASHBOARD
# ══════════════════════════════════════════════════════════════
bg, bd = COLORS['dashboard']
draw_box(ax, 3, 7.1, 8, 1.1, bg, bd,
         '📊  Dashboard  (Looker Studio)',
         '3 Scorecards  |  Bar Chart  |  Pie Chart  |  Table  |  Scatter Plot\nLive link: lookerstudio.google.com')

# ══════════════════════════════════════════════════════════════
# KEY METRICS ROW
# ══════════════════════════════════════════════════════════════
ax.plot([0.5, 13.5], [6.7, 6.7], color='#e5e7eb', lw=1.5)
ax.text(7, 6.45, 'KEY RESULTS', ha='center', fontsize=9,
        fontweight='bold', color=COLORS['subtitle'])

metrics = [
    ('39,717', 'Loans'),
    ('$445M', 'Volume'),
    ('20.44%', 'Avg Default'),
    ('5.97%', 'Grade A'),
    ('31.96%', 'Grade G'),
    ('55.7%', 'Low Risk'),
    ('39.1%', 'Delinquency'),
]
box_w = 1.75
gap = 0.1
start_x = (14 - (len(metrics) * box_w + (len(metrics)-1) * gap)) / 2

for i, (val, lbl) in enumerate(metrics):
    x = start_x + i * (box_w + gap)
    rect = FancyBboxPatch((x, 5.55), box_w, 0.75,
        boxstyle="round,pad=0.08", linewidth=1.2,
        edgecolor='#93c5fd', facecolor='#eff6ff')
    ax.add_patch(rect)
    ax.text(x + box_w/2, 5.97, val,
            ha='center', va='center',
            fontsize=9.5, fontweight='bold', color='#1d4ed8')
    ax.text(x + box_w/2, 5.68, lbl,
            ha='center', va='center',
            fontsize=7.5, color='#6b7280')

# ══════════════════════════════════════════════════════════════
# TECH STACK ROW
# ══════════════════════════════════════════════════════════════
ax.plot([0.5, 13.5], [5.3, 5.3], color='#e5e7eb', lw=1.5)
ax.text(7, 5.05, 'TECHNOLOGY STACK', ha='center', fontsize=9,
        fontweight='bold', color=COLORS['subtitle'])

tech_rows = [
    [('Terraform', '#15803d'), ('Google Cloud', '#1d4ed8'), ('BigQuery', '#1d4ed8'), ('Kestra', '#6d28d9'), ('dbt', '#065f46'), ('Python 3.13', '#374151')],
    [('Redpanda', '#b91c1c'), ('PyFlink', '#b91c1c'), ('PySpark', '#92400e'), ('Looker Studio', '#1e40af'), ('Docker', '#374151'), ('uv', '#374151')],
]
t_w, t_h = 1.95, 0.45
t_gap = 0.15
t_start = (14 - (6 * t_w + 5 * t_gap)) / 2

for r, row in enumerate(tech_rows):
    for c, (name, color) in enumerate(row):
        x = t_start + c * (t_w + t_gap)
        y = 4.35 - r * (t_h + 0.12)
        rect = FancyBboxPatch((x, y), t_w, t_h,
            boxstyle="round,pad=0.06", linewidth=1,
            edgecolor=color, facecolor='#f9fafb')
        ax.add_patch(rect)
        ax.text(x + t_w/2, y + t_h/2, name,
                ha='center', va='center',
                fontsize=8.5, fontweight='bold', color=color)

# ══════════════════════════════════════════════════════════════
# FOOTER
# ══════════════════════════════════════════════════════════════
ax.plot([0.5, 13.5], [3.55, 3.55], color='#e5e7eb', lw=1)
ax.text(7, 3.25,
        'github.com/dajuctech/lending-portfolio-risk-pipeline',
        ha='center', va='center', fontsize=9,
        color='#1d4ed8', fontweight='bold')
ax.text(7, 2.95,
        'Daniel Jude  |  ML Engineer Intern — 10mg Health, Birmingham UK  |  DE Zoomcamp 2026',
        ha='center', va='center', fontsize=8, color=COLORS['subtitle'])

plt.tight_layout(pad=0.5)
plt.savefig('docs/architecture.png', dpi=150,
            bbox_inches='tight', facecolor='white')
print("✅ Architecture diagram saved to docs/architecture.png")
