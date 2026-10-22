"""Create-tenant lifecycle sequence diagram (matplotlib)."""

import matplotlib.pyplot as plt
from matplotlib import font_manager as fm
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

for f in fm.findSystemFonts():
    if "Inter" in f:
        fm.fontManager.addfont(f)
plt.rcParams["font.family"] = "Inter"

# (name, subtitle, fill, edge, text)
P = [
    ("Client", "HTTP", "#F1F5F9", "#64748B", "#0F172A"),
    ("REST API", "control plane", "#DBEAFE", "#2563EB", "#1E3A8A"),
    ("Postgres", "tenants · tasks · outbox", "#D1FAE5", "#059669", "#065F46"),
    ("Outbox Relay", "control plane", "#DBEAFE", "#2563EB", "#1E3A8A"),
    ("RabbitMQ", "tasks · task_updates", "#FFEDD5", "#EA580C", "#9A3412"),
    ("Worker", "simulator", "#EDE9FE", "#7C3AED", "#5B21B6"),
    ("Updates Consumer", "control plane", "#DBEAFE", "#2563EB", "#1E3A8A"),
]
X = {p[0]: 1.5 + i * 3.2 for i, p in enumerate(P)}
COL = {p[0]: p[3] for p in P}

# (kind, from, to, label, sublabel) ; kind: msg | ret | note | phase | self
S = [
    ("phase", "1 · API call — write once, atomically", "#2563EB"),
    ("msg", "Client", "REST API", "POST /v1/tenants  {slug, name}", None),
    (
        "msg",
        "REST API",
        "Postgres",
        "BEGIN · INSERT tenant (provisioning, v1)",
        "INSERT task (deploy, accepted) · INSERT outbox · COMMIT",
    ),
    ("ret", "REST API", "Client", "201 Created  {tenant, current_task}", None),
    ("phase", "2 · Outbox relay — publish strictly after commit", "#EA580C"),
    (
        "msg",
        "Outbox Relay",
        "Postgres",
        "SELECT … WHERE published_at IS NULL",
        "FOR UPDATE SKIP LOCKED",
    ),
    ("msg", "Outbox Relay", "RabbitMQ", "publish TaskEvent → tasks", "publisher confirm"),
    ("msg", "Outbox Relay", "Postgres", "UPDATE outbox SET published_at = now()", None),
    ("phase", "3 · Worker — simulate provisioning", "#7C3AED"),
    ("msg", "RabbitMQ", "Worker", "deliver TaskEvent", None),
    ("msg", "Worker", "RabbitMQ", "TaskUpdate {update_id, task_id, in_progress}", None),
    ("self", "Worker", None, "sleep(rand(min, max) ms)", None),
    ("msg", "Worker", "RabbitMQ", "TaskUpdate {update_id, task_id, done}", None),
    ("phase", "4 · Consumer — idempotent, order-tolerant apply", "#059669"),
    ("msg", "RabbitMQ", "Updates Consumer", "deliver TaskUpdate (in_progress)", None),
    (
        "msg",
        "Updates Consumer",
        "Postgres",
        "tx: INSERT processed_updates ON CONFLICT",
        "task → in_progress  (tenant row untouched)",
    ),
    ("ret", "Updates Consumer", "RabbitMQ", "ack", None),
    ("msg", "RabbitMQ", "Updates Consumer", "deliver TaskUpdate (done)", None),
    (
        "msg",
        "Updates Consumer",
        "Postgres",
        "tx: INSERT processed_updates · task → done",
        "tenant → active · version 1 → 2",
    ),
    ("ret", "Updates Consumer", "RabbitMQ", "ack", None),
    ("phase", "5 · Observe", "#334155"),
    ("msg", "Client", "REST API", "GET /v1/tenants/{id}", None),
    ("ret", "REST API", "Client", "200  {status: active, version: 2}", None),
]

ROW = 0.68
heights = []
for s in S:
    heights.append(0.75 if s[0] == "phase" else (1.0 if (len(s) > 4 and s[4]) else ROW))
total = sum(heights)
W = X[P[-1][0]] + 1.6
TOP = total + 2.3
fig, ax = plt.subplots(figsize=(W, TOP + 0.4), dpi=150)
ax.set_xlim(0, W)
ax.set_ylim(-0.3, TOP + 0.4)
ax.axis("off")
fig.patch.set_facecolor("white")

ax.text(
    W / 2,
    TOP + 0.05,
    "Create Tenant — Full Lifecycle",
    fontsize=21,
    fontweight="semibold",
    ha="center",
    color="#0F172A",
)
ax.text(
    W / 2,
    TOP - 0.42,
    "provisioning → active  ·  task accepted → in_progress → done",
    fontsize=12,
    ha="center",
    color="#64748B",
)

head_y = TOP - 1.2
for name, sub, fill, edge, tc in P:
    x = X[name]
    ax.add_patch(
        FancyBboxPatch(
            (x - 1.3, head_y - 0.38),
            2.6,
            0.82,
            boxstyle="round,pad=0,rounding_size=0.14",
            fc=fill,
            ec=edge,
            lw=1.8,
            zorder=3,
        )
    )
    ax.text(
        x,
        head_y + 0.12,
        name,
        fontsize=12,
        fontweight="bold",
        ha="center",
        va="center",
        color="#0F172A",
        zorder=4,
    )
    ax.text(x, head_y - 0.17, sub, fontsize=9, ha="center", va="center", color=tc, zorder=4)
    ax.plot([x, x], [head_y - 0.38, 0.0], color=edge, lw=1.3, ls=(0, (5, 4)), alpha=0.55, zorder=1)

y = head_y - 0.75
for s, h in zip(S, heights):
    y -= h
    kind = s[0]
    if kind == "phase":
        _, label, c = s
        ax.add_patch(
            FancyBboxPatch(
                (0.15, y - 0.17),
                W - 0.3,
                0.42,
                boxstyle="round,pad=0,rounding_size=0.08",
                fc=c,
                ec="none",
                alpha=0.10,
                zorder=0,
            )
        )
        ax.text(
            0.35,
            y + 0.04,
            label,
            fontsize=11,
            fontweight="semibold",
            color=c,
            va="center",
            zorder=2,
            bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none", alpha=0.85),
        )
        continue
    _, a, b, label, sub = s
    xa = X[a]
    if kind == "self":
        ax.add_patch(
            FancyArrowPatch(
                (xa, y + 0.2),
                (xa, y - 0.15),
                connectionstyle="arc3,rad=-1.6",
                arrowstyle="-|>,head_length=7,head_width=4",
                color=COL[a],
                lw=1.8,
                zorder=3,
            )
        )
        ax.text(xa + 0.55, y + 0.03, label, fontsize=10, color=COL[a], va="center", style="italic")
        continue
    xb = X[b]
    c = COL[a]
    ls = (0, (4, 3)) if kind == "ret" else "-"
    ax.add_patch(
        FancyArrowPatch(
            (xa, y),
            (xb, y),
            arrowstyle="-|>,head_length=9,head_width=5",
            color=c,
            lw=1.9,
            ls=ls,
            zorder=3,
            shrinkA=0,
            shrinkB=0,
        )
    )
    mid = (xa + xb) / 2
    ax.text(
        mid,
        y + 0.2,
        label,
        fontsize=10.2,
        ha="center",
        va="center",
        color="#0F172A",
        zorder=4,
        fontweight="medium",
        bbox=dict(boxstyle="round,pad=0.12", fc="white", ec="none", alpha=0.9),
    )
    if sub:
        ax.text(
            mid,
            y - 0.24,
            sub,
            fontsize=9,
            ha="center",
            va="center",
            color="#475569",
            zorder=4,
            bbox=dict(boxstyle="round,pad=0.1", fc="white", ec="none", alpha=0.9),
        )

plt.savefig(
    "images/sequence-create-tenant.png",
    dpi=150,
    bbox_inches="tight",
    pad_inches=0.25,
    facecolor="white",
)
print("ok")
