"""Hand-laid-out system architecture diagram (matplotlib) for full control of positions."""

import matplotlib.pyplot as plt
from matplotlib import font_manager as fm
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

for f in fm.findSystemFonts():
    if "Inter" in f:
        fm.fontManager.addfont(f)
plt.rcParams["font.family"] = "Inter"

W, H = 22, 12.4
fig, ax = plt.subplots(figsize=(W, H), dpi=150)
ax.set_xlim(0, W)
ax.set_ylim(0, H)
ax.axis("off")
fig.patch.set_facecolor("white")

SLATE = dict(fill="#F8FAFC", edge="#94A3B8", text="#0F172A", sub="#475569")
BLUE = dict(fill="#EFF6FF", edge="#3B82F6", inner="#DBEAFE", text="#1E3A8A", sub="#1E40AF")
GREEN = dict(fill="#ECFDF5", edge="#10B981", inner="#D1FAE5", text="#065F46", sub="#047857")
ORANGE = dict(fill="#FFF7ED", edge="#F97316", inner="#FFEDD5", text="#9A3412", sub="#C2410C")
PURPLE = dict(fill="#F5F3FF", edge="#8B5CF6", inner="#EDE9FE", text="#5B21B6", sub="#6D28D9")
RED = dict(inner="#FEE2E2", edge="#EF4444", text="#991B1B")


def box(x, y, w, h, fill, edge, lw=1.6, r=0.18, z=1, ls="-"):
    p = FancyBboxPatch(
        (x, y),
        w,
        h,
        boxstyle=f"round,pad=0,rounding_size={r}",
        fc=fill,
        ec=edge,
        lw=lw,
        zorder=z,
        ls=ls,
    )
    ax.add_patch(p)
    return p


def text(x, y, s, size=11, color="#0F172A", weight="normal", ha="center", va="center", z=5, **kw):
    ax.text(x, y, s, fontsize=size, color=color, fontweight=weight, ha=ha, va=va, zorder=z, **kw)


def container(x, y, w, h, pal, title):
    box(x, y, w, h, pal["fill"], pal["edge"], lw=2.2, r=0.3, z=1)
    text(x + 0.3, y + h - 0.38, title, size=12.5, color=pal["text"], weight="semibold", ha="left")


def component(x, y, w, h, pal, title, lines, fill=None):
    box(x, y, w, h, fill or pal["inner"], pal["edge"], lw=1.6, r=0.14, z=2)
    text(x + w / 2, y + h - 0.36, title, size=12.5, color="#0F172A", weight="bold")
    for i, ln in enumerate(lines):
        text(x + w / 2, y + h - 0.78 - i * 0.33, ln, size=9.6, color=pal["sub"])


def arrow(p1, p2, color, label=None, lpos=None, lw=2.2, ls="-", rad=0.0, num=None, lha="center"):
    a = FancyArrowPatch(
        p1,
        p2,
        arrowstyle="-|>,head_length=9,head_width=5.5",
        connectionstyle=f"arc3,rad={rad}",
        color=color,
        lw=lw,
        ls=ls,
        zorder=3,
        shrinkA=2,
        shrinkB=2,
    )
    ax.add_patch(a)
    if label:
        lx, ly = lpos or ((p1[0] + p2[0]) / 2, (p1[1] + p2[1]) / 2)
        if num:
            # numbered step badge
            off = -0.22 if lha == "left" else -len(label) * 0.043 - 0.22
            bx = lx + off if lha != "center" else lx - len(label) * 0.045 - 0.25
            ax.add_patch(plt.Circle((bx, ly), 0.17, color=color, zorder=6))
            text(bx, ly - 0.005, str(num), size=9, color="white", weight="bold", z=7)
            text(
                bx + 0.27,
                ly,
                label,
                size=10,
                color=color,
                weight="medium",
                ha="left",
                z=6,
                bbox=dict(boxstyle="round,pad=0.18", fc="white", ec="none", alpha=0.92),
            )
        else:
            text(
                lx,
                ly,
                label,
                size=10,
                color=color,
                weight="medium",
                z=6,
                bbox=dict(boxstyle="round,pad=0.18", fc="white", ec="none", alpha=0.92),
            )


# ---------- title ----------
text(
    W / 2,
    H - 0.45,
    "Tenant Provisioning Control Plane — System Architecture",
    size=22,
    weight="semibold",
    color="#0F172A",
)
text(
    W / 2,
    H - 0.95,
    "docker compose  ·  4 containers  ·  one command:  make up",
    size=12,
    color="#64748B",
)

# ---------- client ----------
box(0.4, 7.35, 2.6, 1.3, SLATE["fill"], SLATE["edge"], lw=1.6, r=0.16, z=2)
text(1.7, 8.25, "API Client", size=12.5, weight="bold")
text(1.7, 7.85, "curl · tests · load script", size=9.6, color=SLATE["sub"])

# ---------- control plane ----------
container(3.8, 3.6, 6.4, 7.3, BLUE, "CONTROL PLANE   ·   control-plane   ·   :8080")
component(
    4.65,
    8.1,
    5.05,
    1.75,
    BLUE,
    "REST API  (FastAPI)",
    ["validation · state-machine guards", "optimistic locking · structured errors"],
)
component(
    4.65,
    6.05,
    5.05,
    1.75,
    BLUE,
    "Outbox Relay  (background loop)",
    ["SELECT … FOR UPDATE SKIP LOCKED", "publish  →  mark published_at"],
)
component(
    4.65,
    4.0,
    5.05,
    1.75,
    BLUE,
    "Updates Consumer  (background loop)",
    ["idempotent · order-tolerant", "retry w/ backoff · poison → DLQ"],
)

# ---------- postgres ----------
container(3.8, 0.35, 6.4, 2.85, GREEN, "POSTGRES 16   ·   :5432")
tables = [
    ("tenants", "slug UNIQUE · version"),
    ("tasks", "≤ 1 open per tenant"),
    ("outbox", "events awaiting publish"),
    ("processed_updates", "idempotency keys"),
]
for i, (t, d) in enumerate(tables):
    col, row = i % 2, i // 2
    x, y = 4.25 + col * 2.95, 1.55 - row * 0.95
    box(x, y, 2.75, 0.8, "#A7F3D0" if i == 0 else GREEN["inner"], "#059669", lw=1.3, r=0.1, z=2)
    text(x + 0.15, y + 0.52, t, size=11, weight="bold", ha="left", color="#064E3B")
    text(x + 0.15, y + 0.22, d, size=8.8, color=GREEN["sub"], ha="left")

# ---------- rabbitmq ----------
container(12.4, 3.6, 4.8, 7.3, ORANGE, "RABBITMQ 3.13  ·  :5672  ·  UI :15672")


def queue(x, y, name, sub, pal_fill, pal_edge, color):
    box(x, y, 3.2, 1.25, pal_fill, pal_edge, lw=1.6, r=0.5, z=2)
    text(x + 1.6, y + 0.8, name, size=12.5, weight="bold", color="#0F172A")
    text(x + 1.6, y + 0.42, sub, size=9.6, color=color)


queue(
    13.2, 8.35, "tasks", "TaskEvent  (CP → worker)", ORANGE["inner"], ORANGE["edge"], ORANGE["sub"]
)
queue(
    13.2,
    6.3,
    "task_updates",
    "TaskUpdate  (worker → CP)",
    ORANGE["inner"],
    ORANGE["edge"],
    ORANGE["sub"],
)
queue(13.2, 4.25, "*.dlq", "poison / unparseable", RED["inner"], RED["edge"], RED["text"])

# ---------- worker ----------
container(18.6, 5.9, 3.0, 3.5, PURPLE, "WORKER SIMULATOR")
component(
    18.85,
    6.3,
    2.5,
    2.55,
    PURPLE,
    "worker  (CLI)",
    ["--fail-rate", "--min-delay-ms", "--max-delay-ms", "--duplicate-rate"],
)

# ---------- arrows ----------
C_HTTP, C_DB, C_MQ, C_W, C_CP, C_RED = (
    "#334155",
    "#059669",
    "#EA580C",
    "#7C3AED",
    "#2563EB",
    "#DC2626",
)

arrow((3.0, 8.3), (4.65, 9.45), C_HTTP)
text(2.75, 9.25, "①  HTTP", size=10.5, color=C_HTTP, weight="semibold", ha="left")

# API -> DB (outer left lane)
ax.plot([4.65, 4.05, 4.05], [8.45, 8.45, 3.3], color=C_DB, lw=2.4, zorder=3)
arrow((4.05, 3.35), (4.05, 3.2), C_DB, lw=2.4)
ax.text(
    3.6,
    6.2,
    "②  ONE transaction: tenant + task + outbox row",
    rotation=90,
    fontsize=10,
    color=C_DB,
    fontweight="semibold",
    ha="center",
    va="center",
    zorder=6,
    bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="none"),
)
# relay <-> DB (inner left lane, dashed)
ax.plot([4.65, 4.35, 4.35], [6.6, 6.6, 3.35], color=C_DB, lw=1.9, ls=(0, (4, 3)), zorder=3)
arrow((4.35, 3.4), (4.35, 3.2), C_DB, lw=1.9)
text(
    5.35,
    3.4,
    "③ poll / mark sent",
    size=9.5,
    color=C_DB,
    weight="medium",
    z=6,
    bbox=dict(boxstyle="round,pad=0.15", fc=BLUE["fill"], ec="none"),
)
# consumer -> DB
arrow((7.95, 4.0), (7.95, 3.2), C_DB, lw=2.4)
text(
    8.9,
    3.4,
    "⑧ apply update + key",
    size=9.5,
    color=C_DB,
    weight="medium",
    z=6,
    bbox=dict(boxstyle="round,pad=0.15", fc=BLUE["fill"], ec="none"),
)

# relay -> tasks queue
arrow((9.7, 7.0), (13.2, 8.95), C_MQ, lw=2.6)
text(
    11.4,
    8.35,
    "④  publish AFTER commit",
    size=10.5,
    color=C_MQ,
    weight="semibold",
    rotation=30,
    bbox=dict(boxstyle="round,pad=0.18", fc="white", ec="none"),
)

# tasks -> worker
arrow((16.4, 9.05), (18.85, 8.3), C_W, lw=2.4)
text(
    17.65,
    9.2,
    "⑤ consume",
    size=10.5,
    color=C_W,
    weight="semibold",
    bbox=dict(boxstyle="round,pad=0.18", fc="white", ec="none"),
)

# worker -> task_updates
arrow((18.85, 6.9), (16.4, 6.9), C_W, lw=2.4)
text(
    17.65,
    7.25,
    "⑥ in_progress",
    size=10,
    color=C_W,
    weight="semibold",
    bbox=dict(boxstyle="round,pad=0.12", fc="white", ec="none"),
)
text(
    17.65,
    6.55,
    "→ done | failed",
    size=10,
    color=C_W,
    weight="semibold",
    bbox=dict(boxstyle="round,pad=0.12", fc="white", ec="none"),
)

# task_updates -> consumer
arrow((13.2, 6.7), (9.7, 5.1), C_CP, lw=2.6)
text(
    11.4,
    6.25,
    "⑦  consume (manual ack)",
    size=10.5,
    color=C_CP,
    weight="semibold",
    rotation=25,
    bbox=dict(boxstyle="round,pad=0.18", fc="white", ec="none"),
)

# consumer -> dlq
arrow((9.7, 4.45), (13.2, 4.85), C_RED, ls=(0, (4, 3)), lw=1.8)
text(
    11.4,
    4.35,
    "reject poison → DLQ",
    size=10,
    color=C_RED,
    weight="medium",
    bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none"),
)

# ---------- legend / guarantees ----------
lx, ly = 12.4, 0.35
box(lx, ly, 9.2, 2.85, "#F8FAFC", "#CBD5E1", lw=1.4, r=0.25, z=1)
text(
    lx + 0.3,
    ly + 2.45,
    "Correctness guarantees",
    size=12.5,
    weight="semibold",
    ha="left",
    color="#0F172A",
)
items = [
    (C_DB, "Transactional outbox — event published only after the DB commit, never on rollback"),
    (C_CP, "Idempotent inbox — update_id stored in processed_updates in the same tx"),
    (C_W, "Order-tolerant — forward-only transitions; stale / terminal updates ignored"),
    (C_MQ, "At-least-once — manual ack after commit; retry with exponential backoff"),
    (C_RED, "Poison-safe — unparseable messages rejected to DLQ, consumer keeps running"),
]
for i, (c, s) in enumerate(items):
    yy = ly + 1.95 - i * 0.39
    ax.add_patch(plt.Circle((lx + 0.42, yy), 0.08, color=c, zorder=3))
    text(lx + 0.65, yy, s, size=9.8, color="#334155", ha="left")

plt.savefig(
    "images/architecture.png", dpi=150, bbox_inches="tight", pad_inches=0.25, facecolor="white"
)
print("ok")
