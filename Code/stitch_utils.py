"""
Shared helpers for encounter stitching and CONSORT reporting.

The sepsis-definitions pipeline keys every analysis on ``hospitalization_id``.
After running clifpy's ``stitch_encounters`` we want the *stitched encounter*
(``encounter_block``) to become the unit of analysis everywhere, while keeping
the column name ``hospitalization_id`` so downstream code (and the R analysis)
need no structural change.

The mechanism: we materialize a copy of every CLIF table the pipeline consumes
into ``<phi_directory>/intermediate/stitched_tables/`` with ``hospitalization_id``
overwritten by the stitched encounter id. Downstream notebooks point clifpy at
that derived directory (computed from ``phi_directory`` in ``clif_config.json``
via :func:`stitched_dir`), so there is a single source-of-truth config file.

Stitched encounter ids are written as strings prefixed with ``EB`` (e.g.
``"EB42"``) so they never collide with the original ``hospitalization_id``
namespace and are obviously not raw hospitalization ids.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd


# Stitched encounter id prefix (keeps the namespace distinct from raw hosp ids)
ENCOUNTER_ID_PREFIX = "EB"

# Long / event tables that carry hospitalization_id and must have their ids
# remapped to the stitched encounter id (all rows kept). ``hospitalization`` is
# handled separately (collapsed to one row per block) and ``patient`` is copied
# unchanged (it is keyed by patient_id, not hospitalization_id).
STITCH_EVENT_TABLES: List[str] = [
    "adt",
    "labs",
    "vitals",
    "patient_assessments",
    "respiratory_support",
    "medication_admin_continuous",
    "medication_admin_intermittent",
    "microbiology_culture",
    "hospital_diagnosis",
    "crrt_therapy",
]


def encounter_id(block) -> str:
    """Canonical string id for a stitched encounter_block."""
    return f"{ENCOUNTER_ID_PREFIX}{int(block)}"


def stitched_dir(phi_dir) -> Path:
    """Directory that holds the materialized stitched CLIF tables (PHI)."""
    return Path(phi_dir) / "intermediate" / "stitched_tables"


def clif_filename(table_name: str, filetype: str) -> str:
    """clifpy resolves files as ``clif_<table_name>.<filetype>`` (io.py:89)."""
    return f"clif_{table_name}.{filetype}"


def load_encounter_mapping(phi_dir) -> pd.DataFrame:
    """Load the saved hospitalization_id -> encounter_block mapping."""
    return pd.read_parquet(Path(phi_dir) / "encounter_mapping.parquet")


def remap_ids(
    df: pd.DataFrame,
    mapping: pd.DataFrame,
    keep_unmapped: bool = False,
) -> pd.DataFrame:
    """Overwrite ``hospitalization_id`` with the stitched encounter id.

    Left-merges ``mapping`` (cols: hospitalization_id, encounter_block) onto
    ``df``, replaces ``hospitalization_id`` with ``EB<block>`` and drops the
    helper column. Rows whose hospitalization_id is not in ``mapping`` are
    dropped unless ``keep_unmapped`` is True.
    """
    out = df.merge(
        mapping[["hospitalization_id", "encounter_block"]],
        on="hospitalization_id",
        how="left",
    )
    if not keep_unmapped:
        out = out[out["encounter_block"].notna()].copy()
    mapped = out["encounter_block"].notna()
    out.loc[mapped, "hospitalization_id"] = (
        out.loc[mapped, "encounter_block"].map(encounter_id)
    )
    return out.drop(columns=["encounter_block"])


def build_block_hospitalization(
    hosp_df: pd.DataFrame,
    mapping: pd.DataFrame,
) -> pd.DataFrame:
    """Collapse the hospitalization table to one row per stitched encounter.

    Temporal span is widened (admission = earliest, discharge = latest) while
    all other attributes are taken from the *index* (earliest-admission) member
    hospitalization, so "admitted via ED at hospital X" semantics are preserved.
    ``discharge_category`` uses the latest discharge (the block's final outcome).
    The resulting ``hospitalization_id`` is the stitched encounter id (``EB...``).
    """
    h = hosp_df.merge(
        mapping[["hospitalization_id", "encounter_block"]],
        on="hospitalization_id",
        how="inner",
    )

    # Index member: earliest admission within the block -> source of attributes.
    index_member = (
        h.sort_values(["encounter_block", "admission_dttm"])
        .groupby("encounter_block", as_index=False)
        .first()
    )

    # Block-level temporal span.
    span = h.groupby("encounter_block", as_index=False).agg(
        _admission_dttm=("admission_dttm", "min"),
        _discharge_dttm=("discharge_dttm", "max"),
    )

    # Final discharge_category = latest discharge in the block.
    last_discharge = (
        h.sort_values(["encounter_block", "discharge_dttm"])
        .groupby("encounter_block", as_index=False)
        .last()[["encounter_block", "discharge_category"]]
        .rename(columns={"discharge_category": "_discharge_category"})
    )

    block = index_member.merge(span, on="encounter_block", how="left")
    block = block.merge(last_discharge, on="encounter_block", how="left")
    block["admission_dttm"] = block["_admission_dttm"]
    block["discharge_dttm"] = block["_discharge_dttm"]
    block["discharge_category"] = block["_discharge_category"]
    block = block.drop(columns=["_admission_dttm", "_discharge_dttm", "_discharge_category"])

    # The stitched encounter id replaces the original hospitalization_id.
    block["hospitalization_id"] = block["encounter_block"].map(encounter_id)
    return block.drop(columns=["encounter_block"])


def materialize_stitched_tables(
    *,
    base_data_directory: str,
    filetype: str,
    timezone: str,
    phi_dir,
    mapping: pd.DataFrame,
    member_hosp_ids: Iterable[str],
    patient_ids: Iterable[str],
    block_hospitalization: pd.DataFrame,
    event_tables: Optional[List[str]] = None,
    verbose: bool = True,
) -> Path:
    """Write stitched copies of the CLIF tables into the stitched directory.

    For each event table: load (filtered to the cohort's original member
    hospitalization ids), remap ids to the stitched encounter id, and write as
    ``clif_<table>.<filetype>``. ``hospitalization`` is written from the
    pre-built ``block_hospitalization`` and ``patient`` is copied unchanged
    (filtered to cohort patients). Missing optional tables are skipped.

    Returns the stitched directory path. clifpy table classes are imported
    lazily so this module has no hard clifpy dependency at import time.
    """
    from clifpy.tables import (
        Adt,
        CrrtTherapy,
        HospitalDiagnosis,
        Labs,
        MedicationAdminContinuous,
        MedicationAdminIntermittent,
        MicrobiologyCulture,
        Patient,
        PatientAssessments,
        RespiratorySupport,
        Vitals,
    )

    table_classes = {
        "adt": Adt,
        "labs": Labs,
        "vitals": Vitals,
        "patient_assessments": PatientAssessments,
        "respiratory_support": RespiratorySupport,
        "medication_admin_continuous": MedicationAdminContinuous,
        "medication_admin_intermittent": MedicationAdminIntermittent,
        "microbiology_culture": MicrobiologyCulture,
        "hospital_diagnosis": HospitalDiagnosis,
        "crrt_therapy": CrrtTherapy,
    }

    event_tables = event_tables or STITCH_EVENT_TABLES
    out_dir = stitched_dir(phi_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    member_hosp_ids = [str(x) for x in member_hosp_ids]

    def _write(df: pd.DataFrame, table_name: str) -> None:
        path = out_dir / clif_filename(table_name, filetype)
        if filetype == "parquet":
            df.to_parquet(path, index=False)
        else:
            df.to_csv(path, index=False)
        if verbose:
            print(f"  wrote {table_name}: {len(df):,} rows -> {path.name}")

    # 1. hospitalization (already block-level, ids already EB...)
    _write(block_hospitalization, "hospitalization")

    # 2. patient — copy unchanged (keyed by patient_id)
    patient = Patient.from_file(
        data_directory=base_data_directory,
        filetype=filetype,
        timezone=timezone,
        filters={"patient_id": [str(p) for p in patient_ids]},
    ).df
    _write(patient, "patient")

    # 3. event tables — load filtered to member hosp ids, remap, write.
    # Optional tables (crrt_therapy in particular) may be absent or — at sites
    # that ship a non-CLIF wide-format export under the same filename — fail
    # the hospitalization_id filter. Skip such tables with a warning rather
    # than abort the whole pipeline.
    for table_name in event_tables:
        cls = table_classes[table_name]
        try:
            tbl = cls.from_file(
                data_directory=base_data_directory,
                filetype=filetype,
                timezone=timezone,
                filters={"hospitalization_id": member_hosp_ids},
            )
        except FileNotFoundError:
            if verbose:
                print(f"  skip {table_name}: source file not found")
            continue
        except Exception as e:
            if verbose:
                print(f"  skip {table_name}: load failed ({type(e).__name__}: {e})")
            continue
        remapped = remap_ids(tbl.df, mapping)
        _write(remapped, table_name)

    return out_dir


# ---------------------------------------------------------------------------
# CONSORT tracking
# ---------------------------------------------------------------------------

def suppress_small(n, threshold: int = 11, sentinel: int = -99):
    """Cell-suppression: counts below ``threshold`` are masked (shareable output)."""
    if n is None:
        return None
    return sentinel if 0 < n < threshold else int(n)


def save_consort(steps: List[dict], out_path, suppress: bool = True) -> Path:
    """Save the ordered CONSORT step counts to JSON.

    Each step is a dict like
    ``{"step": ..., "n_encounter_blocks": ..., "n_hospitalizations": ...,
       "n_patients": ..., "n_excluded": ...}``.
    Small cells are suppressed (<11 -> -99) before writing when ``suppress``.
    """
    out_path = Path(out_path)
    payload = []
    count_keys = ("n_encounter_blocks", "n_hospitalizations", "n_patients", "n_excluded")
    for s in steps:
        row = dict(s)
        if suppress:
            for k in count_keys:
                if k in row and row[k] is not None:
                    row[k] = suppress_small(row[k])
        payload.append(row)
    out_path.write_text(json.dumps(payload, indent=2))
    return out_path


def plot_consort(steps: List[dict], title: str = "Cohort CONSORT diagram"):
    """Render a vertical CONSORT flow figure from the step counts.

    One box per step (with its encounter-block N) connected by downward arrows;
    the number excluded at each transition is annotated to the side. Returns a
    matplotlib Figure. Designed to be regenerated from ``consort_counts.json``
    alone, so it depends only on the ``steps`` list.
    """
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.patches import FancyBboxPatch

    n = len(steps)
    fig_h = max(4.0, 1.4 * n)
    fig, ax = plt.subplots(figsize=(9, fig_h))
    ax.set_xlim(0, 10)
    ax.set_ylim(0, n)
    ax.axis("off")
    ax.set_title(title, fontsize=13, fontweight="bold")

    box_w, box_h = 5.2, 0.62
    cx = 3.2  # center x of the main flow column

    def _fmt(v):
        if v is None:
            return "—"
        if v == -99:
            return "<11 (suppressed)"
        return f"{int(v):,}"

    for i, s in enumerate(steps):
        y = n - 1 - i + 0.2  # top to bottom
        label = s.get("step", f"step {i}")
        nb = s.get("n_encounter_blocks")
        nh = s.get("n_hospitalizations")
        lines = [str(label).replace("_", " ")]
        lines.append(f"encounters: {_fmt(nb)}")
        if nh is not None:
            lines.append(f"hospitalizations: {_fmt(nh)}")
        text = "\n".join(lines)

        box = FancyBboxPatch(
            (cx - box_w / 2, y), box_w, box_h,
            boxstyle="round,pad=0.02,rounding_size=0.06",
            linewidth=1.2, edgecolor="#274472", facecolor="#dbe9f4",
        )
        ax.add_patch(box)
        ax.text(cx, y + box_h / 2, text, ha="center", va="center", fontsize=8.5)

        # Arrow + exclusion annotation to the next box
        if i < n - 1:
            y_next_top = (n - 1 - (i + 1) + 0.2) + box_h
            ax.annotate(
                "", xy=(cx, y_next_top), xytext=(cx, y),
                arrowprops=dict(arrowstyle="-|>", color="#274472", lw=1.4),
            )
            n_excl = steps[i + 1].get("n_excluded")
            if n_excl is not None:
                ax.text(
                    cx + box_w / 2 + 0.3, (y + y_next_top) / 2,
                    f"excluded: {_fmt(n_excl)}", ha="left", va="center",
                    fontsize=8, color="#7a2f2f",
                )

    fig.tight_layout()
    return fig
