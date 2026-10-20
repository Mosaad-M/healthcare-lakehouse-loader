"""Render the architecture diagram (docs/architecture.svg and .png).

Run: uv run --no-project --with matplotlib python docs/architecture.py
"""
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

plt.rcParams["font.family"] = ["Helvetica", "Arial", "DejaVu Sans"]
plt.rcParams["svg.fonttype"] = "none"

W, H = 210, 118
fig, ax = plt.subplots(figsize=(W / 10, H / 10))
ax.set_xlim(0, W)
ax.set_ylim(0, H)
ax.set_aspect("equal")
ax.axis("off")

INK = "#1f2933"
DATA = "#2563eb"
MAINT = "#d97706"
OPT = "#64748b"


def box(x, y, w, h, text, fc="white", ec="#94a3b8", fs=9, bold_first=True, dashed=False, lw=1.0, r=1.2):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle=f"round,pad=0,rounding_size={r}",
                                fc=fc, ec=ec, lw=lw, ls="--" if dashed else "-"))
    if not text:
        return
    lines = text.split("\n")
    lh = fs * 1.3 / 7.2  # line height in data units (1 unit = 0.1 in = 7.2 pt)
    top = y + h / 2 + (len(lines) - 1) * lh / 2
    for i, line in enumerate(lines):
        bold = bold_first and i == 0
        ax.text(x + w / 2, top - i * lh, line, ha="center", va="center", color=INK,
                fontsize=fs if bold or len(lines) == 1 else fs - 1.2,
                fontweight="bold" if bold else "normal")


def tag(x, y, text, color):
    ax.text(x, y, text, ha="left", va="center", fontsize=8.5, fontweight="bold", color="white",
            bbox=dict(boxstyle="round,pad=0.35,rounding_size=0.8", fc=color, ec="none"))


def arrow(p, q, color=DATA, dashed=False, both=False, rad=0.0, label=None, loff=(0, 1.2)):
    ax.add_patch(FancyArrowPatch(p, q, arrowstyle="<|-|>" if both else "-|>", mutation_scale=11,
                                 color=color, lw=1.4, ls="--" if dashed else "-",
                                 connectionstyle=f"arc3,rad={rad}", shrinkA=0, shrinkB=0))
    if label:
        ax.text((p[0] + q[0]) / 2 + loff[0], (p[1] + q[1]) / 2 + loff[1], label, ha="center",
                va="bottom", fontsize=7.5, color=color, style="italic")


# Title and legend
ax.text(2, 114, "healthcare-lakehouse-loader: architecture", fontsize=17, fontweight="bold", color=INK, va="center")
ax.text(2, 110.2, "Polaris + DuckDB + Doris + Spark + Prefect. Local: RustFS, DynamoDB Local. "
        "AWS sandbox: S3, ECS Fargate, EMR Serverless, KMS, DMS, Firehose.", fontsize=9.5, color="#475569", va="center")
lx = 140
for i, (c, d, t) in enumerate([(DATA, False, "data flow"), (MAINT, False, "maintenance"),
                                (OPT, True, "optional / later")]):
    x0 = lx + i * 24
    ax.add_patch(FancyArrowPatch((x0, 112), (x0 + 6, 112), arrowstyle="-|>", mutation_scale=10,
                                 color=c, lw=1.4, ls="--" if d else "-"))
    ax.text(x0 + 7.5, 112, t, fontsize=8.5, va="center", color=INK)

# Columns
COLS = [
    (2, 26, "Data sources", "#e0ecff", "#3b6fd8"),
    (32, 30, "Capture and landing", "#e3f4f1", "#1f8a78"),
    (66, 54, "Data processing paths", "#efe9fb", "#6d4bc4"),
    (124, 30, "Lakehouse storage and catalog", "#e7f5e4", "#3f8f3a"),
    (158, 24, "Data serving layer", "#fdeedd", "#c46a12"),
    (186, 22, "Data applications", "#eef1f5", "#55606e"),
]
for x, w, title, bg, hd in COLS:
    box(x, 31, w, 69, "", fc=bg, ec="none")
    box(x, 101.5, w, 5.5, "", fc=hd, ec="none", r=1.0)
    ax.text(x + w / 2, 104.25, title, ha="center", va="center", fontsize=10, fontweight="bold", color="white")

# 1. Sources
src = ["File drops", "Partner APIs", "Email attachments",
       "Postgres\n(CDC: Debezium / DMS)", "Event streams\n(Kafka, Firehose)"]
for i, t in enumerate(src):
    y = 87 - i * 13
    box(4, y, 22, 10, t, fs=9)

# 2. Capture and landing
box(34, 84, 26, 13, "Prefect fetch flows\npoll APIs and mailboxes,\nwatch drop folders", fs=9)
box(34, 33, 26, 13, "Change capture and\nstream sinks: files\nto raw in seconds", fs=9)
box(34, 52, 26, 26, "Raw zone\nS3 / RustFS\nbytes exactly as received\neach load reaches only\nits own source's files\nencrypted, 30-day expiry\nreplayable", fs=9.5,
    ec="#1f8a78", lw=1.4)
for i in range(3):
    arrow((26, 92 - i * 13), (34, 90))
for i in range(3, 5):
    arrow((26, 92 - i * 13), (34, 39.5))
arrow((47, 84), (47, 78))
arrow((47, 46), (47, 52))

# 3. Processing paths
tag(68, 96.5, "Batch path (default)", DATA)
box(68, 79, 50, 13, "DuckDB loader (one pinned image)\nvalidate per-source contract, type and map,\n"
    "encrypt identifiers, surrogate keys, _batch_id;\nheals a table that lost a file", fs=9.5, ec=DATA, lw=1.4)
for i, t in enumerate(["Bronze\n(replace the batch,\none commit)", "Silver\n(append, snapshot,\nwindow, cdc)",
                       "Gold\n(dbt-duckdb\nmodels)"]):
    box(68 + i * 17.5, 64, 15, 11.5, t, fs=8.8, ec=DATA)
    if i:
        arrow((68 + i * 17.5 - 2.5, 69.75), (68 + i * 17.5, 69.75))
arrow((60, 65), (68, 85.5), rad=-0.2, label="read raw", loff=(2.5, -1))
arrow((75.5, 79), (75.5, 75.5))

tag(68, 59.5, "Maintenance path", MAINT)
box(68, 45, 50, 11.5, "Apache Spark (Spark Connect / EMR Serverless)\nintegrity check first; compaction, manifest rewrite,\n"
    "snapshot expiry, orphan removal; actions chosen\nfrom Polaris policies", fs=9.5, ec=MAINT, lw=1.4)

tag(68, 41, "Fast stream path (optional)", OPT)
box(68, 33, 50, 6, "Kafka Connect Iceberg sink / Flink into Bronze", fs=9, bold_first=False, dashed=True, ec=OPT)

# 4. Lake
box(126, 80, 26, 17, "Apache Polaris\nIceberg REST catalog\nRBAC, vended credentials\n(STS session per principal),\n"
    "maintenance policies", fs=9.5, ec="#3f8f3a", lw=1.4)
box(126, 34, 26, 42, "", ec="#3f8f3a", lw=1.4)
ax.text(139, 72.5, "Iceberg tables", ha="center", va="center", fontsize=9.5, fontweight="bold", color=INK)
ax.text(139, 69.3, "S3 Intelligent-Tiering / RustFS:", ha="center", va="center", fontsize=7.8, color=INK)
ax.text(139, 67.2, "lake-phi, lake-deid (versioned)", ha="center", va="center", fontsize=7.8, color=INK)
for i, t in enumerate(["Bronze (v2, by ingest day)", "Silver (v3, by key bucket)", "Gold"]):
    box(128.5, 58 - i * 9.5, 21, 7, t, fs=8.6, bold_first=False, fc="#f6fbf5", ec="#7fb77a")
ax.text(139, 36.5, "identifiers stored only as ciphertext", ha="center", fontsize=7.5, color="#475569", style="italic")
arrow((118, 69.75), (126, 66), label="commits", loff=(0, 1.0))
arrow((118, 50.75), (126, 50.75), color=MAINT, both=True)
arrow((118, 36), (126, 40), color=OPT, dashed=True)
arrow((139, 80), (139, 76), color="#3f8f3a", both=True)

# 5. Serving
box(160, 48, 20, 38, "Apache Doris\n1 FE + 1 BE\n\nGold copies refreshed\nby the pipeline\n\nIceberg catalog for\nad-hoc reads\n\n"
    "grants, row policies;\nno plaintext identifiers", fs=10, ec="#c46a12", lw=1.4)
box(160, 34, 20, 11, "Doris Routine Load\nfrom Kafka: seconds,\nserving only", fs=8.6, dashed=True, ec=OPT)
arrow((152, 58), (160, 63), label="refresh", loff=(-0.5, 0.6))
arrow((170, 45), (170, 48), color=OPT, dashed=True)

# 6. Applications
box(188, 85, 18, 11, "Other engines\nDuckDB, Spark,\nSnowflake (via Polaris)", fs=8.8, dashed=True, ec=OPT)
apps = ["Dashboards", "Operational\nmonitoring", "Ad-hoc analysis", "Reports"]
for i, t in enumerate(apps):
    y = 72 - i * 10
    box(188, y, 18, 7.5, t, fs=9, bold_first=False)
    arrow((180, y + 3.75), (188, y + 3.75))
arrow((152, 92), (188, 91.5), color=OPT, dashed=True, label="vended credentials", loff=(0, 0.6))

# Cross-cutting band
box(2, 2, 206, 26, "", fc="#f4f5f7", ec="none")
tag(4, 24.5, "Across all layers", "#334155")
cross = [
    "Prefect (self-hosted)\nflows, schedules, retries;\none writer per table;\nFargate Spot on AWS",
    "Identity and keys\nper-subject keys, wrapped by\nper-source keys, under a\nmaster key per partner (KMS);\ncrypto-shredding",
    "Partners\nfreeze (reversible),\nreport, and a delete\nonly after approval",
    "Manifest and locks\nDynamoDB (DynamoDB Local):\nbatches, lease locks,\npartner state",
    "Access control\nPolaris roles, bucket policies,\nper-source raw sessions,\nDoris grants, row policies",
    "Audit\nCloudTrail names principal\nand table; write-once journal:\ndecrypts, erasures, partners",
]
for i, t in enumerate(cross):
    box(4 + i * 34, 5, 32, 15, t, fs=9.3)

out = Path(__file__).parent
fig.savefig(out / "architecture.svg", bbox_inches="tight", pad_inches=0.15)
fig.savefig(out / "architecture.png", dpi=150, bbox_inches="tight", pad_inches=0.15, facecolor="white")
print("wrote", out / "architecture.png")
