"""
ASE.py - Adult Sepsis Event Detection Module

Implements the CDC Adult Sepsis Event (ASE) surveillance definition using:
- clifpy for CLIF data loading and table access
- duckdb for efficient SQL-based data processing

References:
    CDC Hospital Toolkit for Adult Sepsis Surveillance (March 2018)
    https://www.cdc.gov/sepsis/pdfs/sepsis-surveillance-toolkit-mar-2018_508.pdf

CDC ASE Definition (Page 5):
    "ASE: Adult Sepsis Event
    (Must include the 2 components of criteria A AND include one or more
    organ dysfunction listed among B criteria)

    A. Presumed Infection (presence of both 1 and 2):
       1. Blood culture obtained (irrespective of the result), AND
       2. At least 4 Qualifying Antimicrobial Days (QAD)

    B. Organ Dysfunction (at least 1 of following criteria met within ±2 days
       of blood culture):
       1. Initiation of a new vasopressor infusion
       2. Initiation of invasive mechanical ventilation
       3. Doubling of serum creatinine (excluding ESRD)
       4. Total bilirubin ≥2.0 mg/dL and increase by 100% from baseline
       5. Platelet count <100 AND ≥50% decline from baseline
       6. Optional: Serum lactate ≥2.0 mmol/L"

Validation Reference (Page 4):
    "This definition was validated by Rhee, et al. and shown to be present
    in 6% of hospital admissions in a study of nearly 400 hospitals."
"""

import gc
import json
from functools import reduce
import duckdb
import numpy as np
import pandas as pd
from typing import List, Optional

# clifpy imports
from clifpy.tables import (
    Adt,
    Hospitalization,
    Patient,
    Labs,
    MedicationAdminContinuous,
    MedicationAdminIntermittent,
    MicrobiologyCulture,
    RespiratorySupport,
    HospitalDiagnosis,
)

# =============================================================================
# Constants
# =============================================================================

# CDC Page 15 (Appendix B): Vasopressors Included in Adult Sepsis Event Definition
# "Eligible vasopressors must have been administered via continuous intravenous
#  infusion. Vasopressors administered in an operating room are excluded."
# Note: Vasopressor filtering uses med_group = 'vasoactives' in the SQL queries
# which includes: norepinephrine, dopamine, epinephrine, phenylephrine, vasopressin

# CDC Page 5: "excluding patients with ICD-10 code for end-stage renal disease (N18.6)"
ESRD_ICD10 = "N18.6"
ESRD_ICD10_NORMALIZED = ESRD_ICD10.lower().replace('.', '')  # 'n186' for robust matching

# CDC Page 6: "window period extending both 2 days before and 2 days after the blood culture"
WINDOW_DAYS = 2  # ±2 days from blood culture

# CDC Page 6-7: QAD window
# "the first QAD is the first day in window period extending both 2 days before
#  and 2 days after the patient receives a new antimicrobial"
QAD_WINDOW_START = -2  # days relative to blood culture
QAD_WINDOW_END = 6  # days relative to blood culture

# CDC Page 5: "At least 4 Qualifying Antimicrobial Days (QAD)"
MIN_QAD = 4

# CDC Page 9: Repeat Infection Timeframe
# "The repeat infection timeframe (RIT) is a timeframe after an ASE or BSE onset
#  date when no new events are counted... An RIT of 14 days is used [by NHSN]"
RIT_DAYS = 14

# Outlier thresholds for lab values
OUTLIERS = {
    "creatinine_max": 20,
    "bilirubin_max": 80,
    "platelet_max": 2000,
    "lactate_max": 30,
}


# =============================================================================
# Data Loading Functions
# =============================================================================


def _load_clif_tables(
    hospitalization_ids: List[str],
    config_path: str = "clif_config.json",
) -> duckdb.DuckDBPyConnection:
    """
    Load CLIF tables into DuckDB for efficient querying.

    .. deprecated::
        This function is deprecated. The main `calculate_ase()` function now
        uses lazy loading to load tables on-demand and drop them after use,
        which is more memory-efficient for large cohorts.

    Parameters
    ----------
    hospitalization_ids : List[str]
        List of hospitalization IDs to filter data
    config_path : str
        Path to config file (supports both clifpy format and custom format)

    Returns
    -------
    duckdb.DuckDBPyConnection
        DuckDB connection with registered tables
    """
    import warnings
    warnings.warn(
        "_load_clif_tables is deprecated. calculate_ase() now uses lazy loading "
        "for better memory efficiency.",
        DeprecationWarning,
        stacklevel=2
    )
    # Load config and map field names (support both formats)
    with open(config_path) as f:
        config = json.load(f)
    
    # Map field names: support both custom format and clifpy format
    data_directory = config.get('tables_path') or config.get('data_directory')
    filetype = config.get('file_type') or config.get('filetype')
    timezone = config.get('timezone')
    
    con = duckdb.connect(":memory:")

    # Load hospitalization table
    hosp_table = Hospitalization.from_file(
        data_directory=data_directory,
        filetype=filetype,
        timezone=timezone,
        filters={"hospitalization_id": hospitalization_ids},
    )
    con.register("hospitalization", hosp_table.df)

    # Set DuckDB timezone to match site timezone (prevents timezone shifts)
    con.execute(f"SET timezone = '{timezone}'")

    # Load patient table - get patient_ids from hospitalization
    patient_ids = hosp_table.df["patient_id"].unique().tolist()
    patient_table = Patient.from_file(
        data_directory=data_directory,
        filetype=filetype,
        timezone=timezone,
        filters={"patient_id": patient_ids},
    )
    con.register("patient", patient_table.df)

    # Load labs table
    labs_table = Labs.from_file(
        data_directory=data_directory,
        filetype=filetype,
        timezone=timezone,
        filters={
            "hospitalization_id": hospitalization_ids,
             "lab_category": ["creatinine", "bilirubin_total", "platelet_count", "lactate"]},
        columns=[
            "hospitalization_id",
            "lab_category",
            "lab_value",
            "lab_value_numeric",
            "lab_result_dttm",
            "lab_order_dttm",
        ],
    )
    con.register("labs", labs_table.df)

    # Load microbiology culture table (only blood cultures needed for ASE)
    micro_table = MicrobiologyCulture.from_file(
        data_directory=data_directory,
        filetype=filetype,
        timezone=timezone,
        filters={
            "hospitalization_id": hospitalization_ids,
            "fluid_category": ["blood_buffy"],
        },
    )
    con.register("microbiology", micro_table.df)

    # Load continuous medication table (only vasoactives needed for ASE)
    med_cont_table = MedicationAdminContinuous.from_file(
        data_directory=data_directory,
        filetype=filetype,
        timezone=timezone,
        filters={
            "hospitalization_id": hospitalization_ids,
            "med_group": ["vasoactives"],
        },
    )
    con.register("med_continuous", med_cont_table.df)

    # Load intermittent medication table (only qualifying antibiotics needed for QAD)
    med_int_table = MedicationAdminIntermittent.from_file(
        data_directory=data_directory,
        filetype=filetype,
        timezone=timezone,
        filters={
            "hospitalization_id": hospitalization_ids,
            "med_group": ["CMS_sepsis_qualifying_antibiotics"],
        },
    )
    con.register("med_intermittent", med_int_table.df)

    # Load respiratory support table (for IMV)
    resp_table = RespiratorySupport.from_file(
        data_directory=data_directory,
        filetype=filetype,
        timezone=timezone,
        filters={"hospitalization_id": hospitalization_ids},
    )
    con.register("respiratory", resp_table.df)

    # Load hospital diagnosis table (for ESRD)
    dx_table = HospitalDiagnosis.from_file(
        data_directory=data_directory,
        filetype=filetype,
        timezone=timezone,
        filters={"hospitalization_id": hospitalization_ids},
        columns=["hospitalization_id", "diagnosis_code", "diagnosis_code_format"],
    )
    con.register("diagnosis", dx_table.df)

    # Load ADT table (for OR/procedural location exclusion per CDC Appendix B)
    adt_table = Adt.from_file(
        data_directory=data_directory,
        filetype=filetype,
        timezone=timezone,
        filters={"hospitalization_id": hospitalization_ids},
        columns=["hospitalization_id", "in_dttm", "out_dttm", "location_category"],
    )
    con.register("adt", adt_table.df)

    return con


# =============================================================================
# Blood Culture Functions
# =============================================================================


def _get_blood_cultures(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Get ALL blood cultures per hospitalization - each evaluated independently.

    CDC Definition (Page 6):
        "Qualifying cultures include those drawn for bacterial (aerobic and/or
        anaerobic), acid-fast bacilli (AFB), and fungal cultures. Blood cultures
        for specific viruses (e.g., cytomegalovirus) are excluded. For ASE, blood
        cultures merely need to have been drawn, regardless of result."

        "Multiple window periods during a hospitalization are possible. If multiple
        blood cultures are obtained in a short period of time, window periods may
        overlap."

    Note: RIT (Repeat Infection Timeframe) is applied AFTER ASE determination
    as post-processing, not during blood culture grouping. Per CDC (Page 9):
    "The repeat infection timeframe (RIT) is a timeframe after an ASE or BSE
    onset date when no new events are counted."

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        DuckDB connection with loaded tables

    Returns
    -------
    pd.DataFrame
        DataFrame with columns:
        - hospitalization_id
        - bc_id (unique blood culture identifier per hospitalization)
        - blood_culture_dttm
        - admission_dttm
        - discharge_dttm
    """
    return con.execute("""
        SELECT
            m.hospitalization_id,
            ROW_NUMBER() OVER (
                PARTITION BY m.hospitalization_id
                ORDER BY m.order_dttm
            ) as bc_id,
            m.order_dttm as blood_culture_dttm,
            h.admission_dttm,
            h.discharge_dttm
        FROM microbiology m
        JOIN hospitalization h USING (hospitalization_id)
        WHERE m.fluid_category = 'blood_buffy'
        ORDER BY m.hospitalization_id, m.order_dttm
    """).df()


def _aggregate_to_episodes(blood_cultures: pd.DataFrame) -> pd.DataFrame:
    """
    .. deprecated::
        This function is deprecated. Each blood culture is now evaluated
        independently per CDC guidelines (Page 6): "Multiple window periods
        during a hospitalization are possible. If multiple blood cultures are
        obtained in a short period of time, window periods may overlap."

        RIT (Repeat Infection Timeframe) is now applied as post-processing
        AFTER ASE determination using _apply_rit_post_processing().
    """
    import warnings
    warnings.warn(
        "_aggregate_to_episodes is deprecated. Each blood culture is now "
        "evaluated independently. Use _apply_rit_post_processing() after "
        "ASE determination to apply RIT.",
        DeprecationWarning,
        stacklevel=2
    )
    # Legacy behavior - kept for backwards compatibility if needed
    return blood_cultures.groupby(
        ["hospitalization_id", "bc_id", "admission_dttm", "discharge_dttm"],
        as_index=False,
    ).agg({"blood_culture_dttm": "min"})


# =============================================================================
# QAD (Qualifying Antimicrobial Days) Functions
# =============================================================================


def _calculate_qad(
    con: duckdb.DuckDBPyConnection,
    blood_cultures: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calculate consecutive qualifying antimicrobial days (QAD) per blood culture.

    Each blood culture is evaluated independently per CDC (Page 6):
    "Multiple window periods during a hospitalization are possible."

    CDC Definition (Page 6-7):
        "For ASE events, the first QAD is the first day in window period extending
        both 2 days before and 2 days after the patient receives a new antimicrobial.
        A new antimicrobial is defined as an antimicrobial not previously administered
        in the prior 2 calendar days."

        "There must be at least one new parenteral (intravenous or intramuscular)
        antimicrobial administered within the window period for the QADs to satisfy
        the definition."

        "A gap of a single calendar day between administrations of the same antibiotic
        (oral or intravenous) count as QADs as long as the gap is not greater than 1 day."

    CDC QAD Censoring (Page 8):
        "If a patient's care transitions to comfort measures only, or the patient dies,
        is discharged to another hospital, or discharged to hospice before 4 QADs have
        elapsed, then the presumed infection criteria can be met with less than 4 QADs
        as long as they have consecutive QADs until day of, or 1 day prior to, death
        or discharge."

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        DuckDB connection with loaded tables
    blood_cultures : pd.DataFrame
        Blood culture data with bc_id (unique per hospitalization)

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: hospitalization_id, bc_id, total_qad, first_qad_dttm
    """
    # blood_cultures_temp is pre-registered in calculate_ase()

    return con.execute("""
        WITH bc_hosp AS (
            SELECT * FROM blood_cultures_temp
        ),
        abx_admin AS (
            -- Get qualifying antimicrobial administrations per blood culture
            SELECT
                m.hospitalization_id,
                bc.bc_id,
                m.admin_dttm,
                m.med_name,
                m.med_route_category,
                DATE(m.admin_dttm) as admin_date,
                bc.blood_culture_dttm,
                -- Calculate day relative to blood culture
                DATEDIFF('day', DATE(bc.blood_culture_dttm), DATE(m.admin_dttm)) as day_from_bc
            FROM med_intermittent m
            JOIN bc_hosp bc ON m.hospitalization_id = bc.hospitalization_id
            WHERE m.med_group = 'CMS_sepsis_qualifying_antibiotics'
              AND DATEDIFF('day', DATE(bc.blood_culture_dttm), DATE(m.admin_dttm)) BETWEEN -2 AND 6
        ),
        daily_abx AS (
            -- Aggregate to daily level - one row per hospitalization-bc_id-date
            SELECT
                hospitalization_id,
                bc_id,
                admin_date,
                day_from_bc,
                blood_culture_dttm,
                MAX(CASE WHEN med_route_category = 'iv' THEN 1 ELSE 0 END) as has_iv,
                MIN(admin_dttm) as first_admin_of_day
            FROM abx_admin
            GROUP BY hospitalization_id, bc_id, admin_date, day_from_bc, blood_culture_dttm
        ),
        -- Check if there's at least one IV antibiotic in the window per blood culture
        iv_check AS (
            SELECT
                hospitalization_id,
                bc_id,
                MAX(has_iv) as has_iv_in_window
            FROM daily_abx
            GROUP BY hospitalization_id, bc_id
        ),
        daily_with_gaps AS (
            -- Calculate gaps between antibiotic days (CDC Figure 4)
            -- Per CDC: "Antibiotic regimens that allow for 1 day between doses
            -- (e.g., every other day dosing) can still qualify"
            SELECT
                d.hospitalization_id,
                d.bc_id,
                d.admin_date,
                d.day_from_bc,
                d.first_admin_of_day,
                d.day_from_bc - LAG(d.day_from_bc, 1) OVER (
                    PARTITION BY d.hospitalization_id, d.bc_id
                    ORDER BY d.day_from_bc
                ) as gap_from_prev
            FROM daily_abx d
            JOIN iv_check ic ON d.hospitalization_id = ic.hospitalization_id
                            AND d.bc_id = ic.bc_id
            WHERE ic.has_iv_in_window = 1
        ),
        consecutive_runs AS (
            -- Assign run groups: new group starts if gap > 2 days
            -- Gap <= 2 means consecutive or 1-day gap (every-other-day dosing OK)
            SELECT
                hospitalization_id,
                bc_id,
                admin_date,
                day_from_bc,
                first_admin_of_day,
                SUM(CASE WHEN gap_from_prev IS NULL OR gap_from_prev > 2 THEN 1 ELSE 0 END) OVER (
                    PARTITION BY hospitalization_id, bc_id
                    ORDER BY day_from_bc
                    ROWS UNBOUNDED PRECEDING
                ) as run_group
            FROM daily_with_gaps
        ),
        run_lengths AS (
            SELECT
                hospitalization_id,
                bc_id,
                run_group,
                COUNT(*) as run_length,
                MIN(admin_date) as first_qad_date,
                MAX(admin_date) as last_qad_date,
                MIN(first_admin_of_day) as first_qad_dttm
            FROM consecutive_runs
            GROUP BY hospitalization_id, bc_id, run_group
        ),
        best_runs AS (
            -- Get the longest run for each hospitalization-bc_id
            SELECT
                hospitalization_id,
                bc_id,
                MAX(run_length) as total_qad,
                FIRST(first_qad_dttm ORDER BY run_length DESC, first_qad_dttm) as first_qad_dttm,
                FIRST(first_qad_date ORDER BY run_length DESC, first_qad_dttm) as qad_start_date,
                FIRST(last_qad_date ORDER BY run_length DESC, first_qad_dttm) as qad_end_date
            FROM run_lengths
            GROUP BY hospitalization_id, bc_id
        )
        SELECT
            hospitalization_id,
            bc_id,
            total_qad,
            first_qad_dttm,
            qad_start_date,
            qad_end_date
        FROM best_runs
    """).df()


# =============================================================================
# ESRD Detection
# =============================================================================


def _get_esrd_flags(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Identify patients with End-Stage Renal Disease (ICD-10: N18.6).

    CDC Definition (Page 5):
        "Doubling of serum creatinine... excluding patients with ICD-10 code
        for end-stage renal disease (N18.6)."

    These patients are excluded from AKI organ dysfunction criteria.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: hospitalization_id, esrd
    """
    # Normalize diagnosis codes by removing dots and converting to lowercase
    # to handle variations like 'N18.6', 'n18.6', 'N186', 'n186'
    return con.execute(f"""
        SELECT DISTINCT
            hospitalization_id,
            1 as esrd
        FROM diagnosis
        WHERE LOWER(REPLACE(diagnosis_code, '.', '')) = '{ESRD_ICD10_NORMALIZED}'
           OR LOWER(REPLACE(diagnosis_code, '.', '')) LIKE '{ESRD_ICD10_NORMALIZED}%'
    """).df()


# =============================================================================
# Organ Dysfunction - Vasopressors
# =============================================================================


def _get_vasopressor_dysfunction(
    con: duckdb.DuckDBPyConnection,
    blood_cultures: pd.DataFrame,
) -> pd.DataFrame:
    """
    Identify new vasopressor initiation within ±2 days of each blood culture.

    Each blood culture is evaluated independently per CDC (Page 6):
    "Multiple window periods during a hospitalization are possible."

    CDC Definition (Page 5):
        "Initiation of a new vasopressor infusion (norepinephrine, dopamine,
        epinephrine, phenylephrine, OR vasopressin). To count as a new vasopressor,
        that specific vasopressor cannot have been administered in the prior
        calendar day."

    CDC Appendix B (Page 15):
        "Eligible vasopressors must have been administered via continuous intravenous
        infusion. Vasopressors administered in an operating room are excluded as these
        are frequently needed to counteract hypotension induced by sedative medication
        administration. Since the location of administration may be challenging to
        identify in an EHR, single bolus injections of vasopressors (a frequent method
        of delivering perioperative vasopressors) are generally excluded."

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: hospitalization_id, bc_id, vasopressor_dttm, vasopressor_name
    """
    # blood_cultures_temp is pre-registered in calculate_ase()

    return con.execute("""
        WITH bc_hosp AS (
            SELECT * FROM blood_cultures_temp
        ),
        vaso_admin AS (
            SELECT
                m.hospitalization_id,
                bc.bc_id,
                m.admin_dttm,
                m.med_name,
                m.med_category,
                DATE(m.admin_dttm) as admin_date,
                bc.blood_culture_dttm,
                LAG(DATE(m.admin_dttm)) OVER (
                    PARTITION BY m.hospitalization_id, m.med_category
                    ORDER BY m.admin_dttm
                ) as prev_admin_date
            FROM med_continuous m
            JOIN bc_hosp bc ON m.hospitalization_id = bc.hospitalization_id
            -- Join ADT to get patient location at time of vasopressor admin
            LEFT JOIN adt a ON m.hospitalization_id = a.hospitalization_id
                           AND m.admin_dttm >= a.in_dttm
                           AND m.admin_dttm < a.out_dttm
            WHERE m.med_group = 'vasoactives'
              AND m.med_dose > 0
              -- CDC Appendix B: Exclude vasopressors given in OR/procedural areas
              AND (a.location_category IS NULL OR a.location_category != 'procedural')
        ),
        new_vaso_in_window AS (
            -- Only count new vasopressors (not given in prior day) within ±2 days of BC
            SELECT *
            FROM vaso_admin
            WHERE (prev_admin_date IS NULL OR DATEDIFF('day', prev_admin_date, admin_date) > 1)
              AND admin_dttm BETWEEN
                  blood_culture_dttm - INTERVAL '2 days'
                  AND blood_culture_dttm + INTERVAL '2 days'
        )
        SELECT
            hospitalization_id,
            bc_id,
            MIN(admin_dttm) as vasopressor_dttm,
            FIRST(med_category ORDER BY admin_dttm) as vasopressor_name
        FROM new_vaso_in_window
        GROUP BY hospitalization_id, bc_id
    """).df()


# =============================================================================
# Organ Dysfunction - IMV
# =============================================================================


def _get_imv_dysfunction(
    con: duckdb.DuckDBPyConnection,
    blood_cultures: pd.DataFrame,
) -> pd.DataFrame:
    """
    Identify new invasive mechanical ventilation within ±2 days of each blood culture.

    Each blood culture is evaluated independently per CDC (Page 6):
    "Multiple window periods during a hospitalization are possible."

    CDC Definition (Page 5):
        "Initiation of invasive mechanical ventilation (must be greater than 1
        calendar day between mechanical ventilation episodes)."

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: hospitalization_id, bc_id, imv_dttm
    """
    # blood_cultures_temp is pre-registered in calculate_ase()

    return con.execute("""
        WITH bc_hosp AS (
            SELECT * FROM blood_cultures_temp
        ),
        imv_episodes AS (
            SELECT
                r.hospitalization_id,
                bc.bc_id,
                r.recorded_dttm,
                DATE(r.recorded_dttm) as imv_date,
                bc.blood_culture_dttm,
                LAG(DATE(r.recorded_dttm)) OVER (
                    PARTITION BY r.hospitalization_id
                    ORDER BY r.recorded_dttm
                ) as prev_imv_date
            FROM respiratory r
            JOIN bc_hosp bc ON r.hospitalization_id = bc.hospitalization_id
            WHERE LOWER(r.device_category) = 'imv'
        ),
        new_imv_in_window AS (
            -- Only count new IMV (>1 day gap from previous) within ±2 days of BC
            SELECT *
            FROM imv_episodes
            WHERE (prev_imv_date IS NULL OR DATEDIFF('day', prev_imv_date, imv_date) > 1)
              AND recorded_dttm BETWEEN
                  blood_culture_dttm - INTERVAL '2 days'
                  AND blood_culture_dttm + INTERVAL '2 days'
        )
        SELECT
            hospitalization_id,
            bc_id,
            MIN(recorded_dttm) as imv_dttm
        FROM new_imv_in_window
        GROUP BY hospitalization_id, bc_id
    """).df()


# =============================================================================
# Organ Dysfunction - Lab Criteria
# =============================================================================


def _get_lab_dysfunction(
    con: duckdb.DuckDBPyConnection,
    blood_cultures: pd.DataFrame,
    esrd_flags: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calculate lab-based organ dysfunction criteria per blood culture.

    Each blood culture is evaluated independently per CDC (Page 6):
    "Multiple window periods during a hospitalization are possible."

    CDC Definitions (Page 5):
        AKI: "Doubling of serum creatinine OR decrease by ≥50% of estimated
             glomerular filtration rate (eGFR) relative to baseline, excluding
             patients with ICD-10 code for end-stage renal disease (N18.6)."

        Hyperbilirubinemia: "Total bilirubin ≥2.0 mg/dL and increase by 100%
                           from baseline."

        Thrombocytopenia: "Platelet count <100 cells/µL AND ≥50% decline from
                         baseline - baseline must be ≥100 cells/µL."

        Lactate: "Optional: Serum lactate ≥2.0 mmol/L, note that serum lactate
                 has become an increasingly common test to measure tissue perfusion.
                 When serum lactate is included in the surveillance definition,
                 the likely effect will be to slightly increase the number of
                 sepsis cases identified."

    CDC Baseline Definitions (Page 9):
        Community-Onset Events:
        - Creatinine baseline: lowest value during hospitalization
        - Bilirubin baseline: lowest value during hospitalization
        - Platelet baseline: highest value during hospitalization (must be ≥100)

        Hospital-Onset Events:
        - Creatinine baseline: lowest value within ±2 days of blood culture
        - Bilirubin baseline: lowest value within ±2 days of blood culture
        - Platelet baseline: highest value within ±2 days of blood culture (must be ≥100)

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: hospitalization_id, bc_id, aki_dttm,
        hyperbilirubinemia_dttm, thrombocytopenia_dttm, lactate_dttm
    """
    # blood_cultures_temp is pre-registered in calculate_ase()
    # Register esrd_temp for this function
    con.register("esrd_temp", esrd_flags)

    # Optimized SQL: Combined labs scan, pre-joined baselines to avoid repeated joins
    return con.execute(f"""
        WITH bc_hosp AS (
            SELECT * FROM blood_cultures_temp
        ),
        -- Get hospitalization IDs with blood cultures (filter labs early)
        bc_hosp_ids AS (
            SELECT DISTINCT hospitalization_id FROM bc_hosp
        ),
        -- Single labs scan: get both window labs and all labs for baseline calculation
        -- Filter to only hospitalizations with blood cultures
        labs_filtered AS (
            SELECT
                l.hospitalization_id,
                l.lab_category,
                l.lab_value_numeric as value,
                l.lab_result_dttm
            FROM labs l
            WHERE l.hospitalization_id IN (SELECT hospitalization_id FROM bc_hosp_ids)
              AND l.lab_category IN ('creatinine', 'bilirubin_total', 'platelet_count', 'lactate')
              AND l.lab_value_numeric IS NOT NULL
        ),
        -- Community-onset baselines (whole hospitalization) - computed once
        baseline_community AS (
            SELECT
                hospitalization_id,
                MIN(CASE WHEN lab_category = 'creatinine' AND value <= {OUTLIERS['creatinine_max']} THEN value END) as cr_baseline_co,
                MIN(CASE WHEN lab_category = 'bilirubin_total' AND value <= {OUTLIERS['bilirubin_max']} THEN value END) as bili_baseline_co,
                MAX(CASE WHEN lab_category = 'platelet_count' AND value <= {OUTLIERS['platelet_max']} AND value >= 100 THEN value END) as plt_baseline_co
            FROM labs_filtered
            WHERE lab_category IN ('creatinine', 'bilirubin_total', 'platelet_count')
            GROUP BY hospitalization_id
        ),
        -- Labs within ±2 days of blood culture per bc_id (with onset type computed inline)
        labs_window AS (
            SELECT
                l.hospitalization_id,
                bc.bc_id,
                l.lab_category,
                l.value,
                l.lab_result_dttm,
                bc.blood_culture_dttm,
                bc.admission_dttm,
                -- Onset type computed inline (avoids separate CTE and join)
                CASE WHEN DATEDIFF('day', DATE(bc.admission_dttm), DATE(bc.blood_culture_dttm)) + 1 <= 2
                     THEN 'community' ELSE 'hospital' END as onset
            FROM labs_filtered l
            JOIN bc_hosp bc ON l.hospitalization_id = bc.hospitalization_id
            WHERE l.lab_result_dttm BETWEEN
                  bc.blood_culture_dttm - INTERVAL '2 days'
                  AND bc.blood_culture_dttm + INTERVAL '2 days'
        ),
        -- Hospital-onset baselines (within ±2 days of blood culture per bc_id)
        baseline_hospital AS (
            SELECT
                hospitalization_id,
                bc_id,
                MIN(CASE WHEN lab_category = 'creatinine' AND value <= {OUTLIERS['creatinine_max']} THEN value END) as cr_baseline_ho,
                MIN(CASE WHEN lab_category = 'bilirubin_total' AND value <= {OUTLIERS['bilirubin_max']} THEN value END) as bili_baseline_ho,
                MAX(CASE WHEN lab_category = 'platelet_count' AND value <= {OUTLIERS['platelet_max']} AND value >= 100 THEN value END) as plt_baseline_ho
            FROM labs_window
            WHERE lab_category IN ('creatinine', 'bilirubin_total', 'platelet_count')
            GROUP BY hospitalization_id, bc_id
        ),
        -- Pre-join labs_window with baselines and ESRD (single join operation)
        labs_with_baselines AS (
            SELECT
                lw.*,
                bc.cr_baseline_co,
                bc.bili_baseline_co,
                bc.plt_baseline_co,
                bh.cr_baseline_ho,
                bh.bili_baseline_ho,
                bh.plt_baseline_ho,
                e.esrd
            FROM labs_window lw
            LEFT JOIN baseline_community bc ON lw.hospitalization_id = bc.hospitalization_id
            LEFT JOIN baseline_hospital bh ON lw.hospitalization_id = bh.hospitalization_id
                                          AND lw.bc_id = bh.bc_id
            LEFT JOIN esrd_temp e ON lw.hospitalization_id = e.hospitalization_id
        ),
        -- AKI detection per bc_id (no additional joins needed)
        aki AS (
            SELECT
                hospitalization_id,
                bc_id,
                MIN(lab_result_dttm) as aki_dttm
            FROM labs_with_baselines
            WHERE lab_category = 'creatinine'
              AND value <= {OUTLIERS['creatinine_max']}
              AND esrd IS NULL  -- exclude ESRD patients
              AND (
                  (onset = 'community' AND cr_baseline_co IS NOT NULL AND value >= 2.0 * cr_baseline_co) OR
                  (onset = 'hospital' AND cr_baseline_ho IS NOT NULL AND value >= 2.0 * cr_baseline_ho)
              )
            GROUP BY hospitalization_id, bc_id
        ),
        -- Hyperbilirubinemia detection per bc_id
        hyperbili AS (
            SELECT
                hospitalization_id,
                bc_id,
                MIN(lab_result_dttm) as hyperbilirubinemia_dttm
            FROM labs_with_baselines
            WHERE lab_category = 'bilirubin_total'
              AND value >= 2.0
              AND value <= {OUTLIERS['bilirubin_max']}
              AND (
                  (onset = 'community' AND bili_baseline_co IS NOT NULL AND value >= 2.0 * bili_baseline_co) OR
                  (onset = 'hospital' AND bili_baseline_ho IS NOT NULL AND value >= 2.0 * bili_baseline_ho)
              )
            GROUP BY hospitalization_id, bc_id
        ),
        -- Thrombocytopenia detection per bc_id
        thrombocytopenia AS (
            SELECT
                hospitalization_id,
                bc_id,
                MIN(lab_result_dttm) as thrombocytopenia_dttm
            FROM labs_with_baselines
            WHERE lab_category = 'platelet_count'
              AND value < 100
              AND value <= {OUTLIERS['platelet_max']}
              AND (
                  (onset = 'community' AND plt_baseline_co IS NOT NULL AND plt_baseline_co >= 100 AND value <= 0.5 * plt_baseline_co) OR
                  (onset = 'hospital' AND plt_baseline_ho IS NOT NULL AND plt_baseline_ho >= 100 AND value <= 0.5 * plt_baseline_ho)
              )
            GROUP BY hospitalization_id, bc_id
        ),
        -- Elevated lactate detection per bc_id (no baseline required)
        lactate AS (
            SELECT
                hospitalization_id,
                bc_id,
                MIN(lab_result_dttm) as lactate_dttm
            FROM labs_window
            WHERE lab_category = 'lactate'
              AND value >= 2.0
              AND value <= {OUTLIERS['lactate_max']}
            GROUP BY hospitalization_id, bc_id
        )
        -- Combine all lab dysfunction per bc_id
        SELECT
            bc.hospitalization_id,
            bc.bc_id,
            aki.aki_dttm,
            hyperbili.hyperbilirubinemia_dttm,
            thrombocytopenia.thrombocytopenia_dttm,
            lactate.lactate_dttm
        FROM bc_hosp bc
        LEFT JOIN aki ON bc.hospitalization_id = aki.hospitalization_id
                     AND bc.bc_id = aki.bc_id
        LEFT JOIN hyperbili ON bc.hospitalization_id = hyperbili.hospitalization_id
                           AND bc.bc_id = hyperbili.bc_id
        LEFT JOIN thrombocytopenia ON bc.hospitalization_id = thrombocytopenia.hospitalization_id
                                  AND bc.bc_id = thrombocytopenia.bc_id
        LEFT JOIN lactate ON bc.hospitalization_id = lactate.hospitalization_id
                         AND bc.bc_id = lactate.bc_id
    """).df()


# =============================================================================
# Presumed Infection Determination
# =============================================================================


def _determine_presumed_infection(
    con: duckdb.DuckDBPyConnection,
    blood_cultures: pd.DataFrame,
    qad_results: pd.DataFrame,
) -> pd.DataFrame:
    """
    Determine presumed infection status per blood culture.

    CDC Definition - Criteria A (Page 5):
        "Presumed Infection (presence of both 1 and 2):
        1. Blood culture obtained (irrespective of the result), AND
        2. At least 4 Qualifying Antimicrobial Days (QAD)"

    CDC QAD Censoring Exception (Page 8):
        "If a patient's care transitions to comfort measures only, or the patient
        dies, is discharged to another hospital, or discharged to hospice before
        4 QADs have elapsed, then the presumed infection criteria can be met with
        less than 4 QADs as long as they have consecutive QADs until day of, or
        1 day prior to, death or discharge."

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: hospitalization_id, bc_id, blood_culture_dttm,
        total_qad, first_qad_dttm, qad_start_date, qad_end_date, presumed_infection
    """
    # blood_cultures_temp is pre-registered in calculate_ase()
    # Register qad_temp for this function
    con.register("qad_temp", qad_results)

    return con.execute("""
        WITH bc_hosp AS (
            SELECT * FROM blood_cultures_temp
        ),
        qad AS (
            SELECT * FROM qad_temp
        ),
        censoring AS (
            -- Get death/discharge info for censoring logic
            SELECT
                h.hospitalization_id,
                h.discharge_dttm,
                h.discharge_category,
                p.death_dttm,
                CASE
                    WHEN p.death_dttm IS NOT NULL THEN p.death_dttm
                    ELSE h.discharge_dttm
                END as censor_dttm,
                CASE
                    WHEN h.discharge_category IN ('expired', 'Expired', 'acute_care_hospital', 'Acute Care Hospital', 'hospice', 'Hospice')
                    THEN 1
                    ELSE 0
                END as qualifies_for_censoring
            FROM hospitalization h
            LEFT JOIN patient p USING (patient_id)
        )
        SELECT
            bc.hospitalization_id,
            bc.bc_id,
            bc.blood_culture_dttm,
            bc.admission_dttm,
            bc.discharge_dttm,
            COALESCE(qad.total_qad, 0) as total_qad,
            qad.first_qad_dttm,
            qad.qad_start_date,
            qad.qad_end_date,
            CASE
                -- Standard: >=4 QAD
                WHEN qad.total_qad >= 4 THEN 1
                -- Censored: >=1 QAD and patient died/transferred before completing 4 days
                WHEN qad.total_qad >= 1
                     AND c.qualifies_for_censoring = 1
                     AND c.censor_dttm IS NOT NULL
                     AND c.censor_dttm <= qad.first_qad_dttm + INTERVAL '1 day' * (4 - 1)
                THEN 1
                ELSE 0
            END as presumed_infection
        FROM bc_hosp bc
        LEFT JOIN qad ON bc.hospitalization_id = qad.hospitalization_id
                     AND bc.bc_id = qad.bc_id
        LEFT JOIN censoring c ON bc.hospitalization_id = c.hospitalization_id
    """).df()


# =============================================================================
# Final ASE Determination
# =============================================================================


def _calculate_final_ase(
    presumed_infection: pd.DataFrame,
    vasopressor_df: pd.DataFrame,
    imv_df: pd.DataFrame,
    lab_dysfunction: pd.DataFrame,
    esrd_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Combine all criteria to determine final ASE status per episode.

    CDC ASE Definition (Page 5):
        "ASE: Adult Sepsis Event
        (Must include the 2 components of criteria A AND include one or more
        organ dysfunction listed among B criteria)"

    CDC Onset Type Classification (Page 8):
        "Hospital-Onset Events require onset date to be on hospital day 3 or later,
        counting the date of admission as hospital day 1."

        "Community-Onset Events require onset date to be on hospital day 2 or earlier,
        when the date of admission counts as hospital day 1."

    CDC Onset Date Definition (Page 8):
        "For ASE, onset date is defined as the earliest day in the window period
        extending both 2 days before and 2 days after the blood culture when EITHER
        the blood culture, first QAD, OR organ dysfunction criteria is met."

    Returns
    -------
    pd.DataFrame
        Final ASE results with all specified columns including bc_id
    """
    # Merge all dataframes by hospitalization_id and bc_id
    # Using index-based merge for better performance on large datasets
    merge_keys = ["hospitalization_id", "bc_id"]
    
    # Set index on base dataframe for faster joins
    result = presumed_infection.set_index(merge_keys)
    
    # Prepare dataframes for merge (set index, handle empty dataframes)
    dfs_to_merge = [
        vasopressor_df.set_index(merge_keys) if len(vasopressor_df) > 0 else pd.DataFrame(index=pd.MultiIndex.from_tuples([], names=merge_keys)),
        imv_df.set_index(merge_keys) if len(imv_df) > 0 else pd.DataFrame(index=pd.MultiIndex.from_tuples([], names=merge_keys)),
        lab_dysfunction.set_index(merge_keys) if len(lab_dysfunction) > 0 else pd.DataFrame(index=pd.MultiIndex.from_tuples([], names=merge_keys)),
    ]
    
    # Use reduce for sequential left joins (cleaner and maintains order)
    result = reduce(
        lambda left, right: left.join(right, how="left"),
        dfs_to_merge,
        result
    )
    
    # Reset index to get columns back
    result = result.reset_index()
    
    # Merge ESRD separately (different key)
    result = result.merge(esrd_df, on="hospitalization_id", how="left")

    # Fill ESRD nulls with 0 (use int8 for memory efficiency)
    result["esrd"] = result["esrd"].fillna(0).astype(np.int8)

    # Define organ dysfunction columns
    organ_cols_w_lactate = [
        "vasopressor_dttm",
        "imv_dttm",
        "aki_dttm",
        "hyperbilirubinemia_dttm",
        "thrombocytopenia_dttm",
        "lactate_dttm",
    ]
    organ_cols_wo_lactate = [
        "vasopressor_dttm",
        "imv_dttm",
        "aki_dttm",
        "hyperbilirubinemia_dttm",
        "thrombocytopenia_dttm",
    ]

    # Has any organ dysfunction
    result["has_organ_dysfunction_w_lactate"] = (
        result[organ_cols_w_lactate].notna().any(axis=1)
    )
    result["has_organ_dysfunction_wo_lactate"] = (
        result[organ_cols_wo_lactate].notna().any(axis=1)
    )

    # Determine ASE (sepsis) status - with lactate version (primary)
    result["sepsis"] = (
        (result["presumed_infection"] == 1)
        & (result["has_organ_dysfunction_w_lactate"])
    ).astype(int)

    # Determine sepsis without lactate version
    result["sepsis_wo_lactate"] = (
        (result["presumed_infection"] == 1)
        & (result["has_organ_dysfunction_wo_lactate"])
    ).astype(int)

    # Determine reason for no sepsis (transparency column)
    result["no_sepsis_reason"] = None
    # Priority 1: No IV antibiotic in window
    result.loc[
        (result["sepsis"] == 0) & (result["total_qad"].isna() | (result["total_qad"] == 0)),
        "no_sepsis_reason"
    ] = "no_qualifying_antibiotics"
    # Priority 2: Insufficient QAD (< 4 days)
    result.loc[
        (result["sepsis"] == 0) & (result["presumed_infection"] == 0) &
        (result["total_qad"].notna()) & (result["total_qad"] > 0) & (result["total_qad"] < 4),
        "no_sepsis_reason"
    ] = "insufficient_qad"
    # Priority 3: No organ dysfunction despite presumed infection
    result.loc[
        (result["sepsis"] == 0) & (result["presumed_infection"] == 1) &
        (~result["has_organ_dysfunction_w_lactate"]),
        "no_sepsis_reason"
    ] = "no_organ_dysfunction"

    # =========================================================================
    # VECTORIZED: Calculate onset times and first criteria (replaces slow apply())
    # =========================================================================
    
    # Define columns for onset calculation
    all_onset_cols_w_lactate = [
        "blood_culture_dttm", "first_qad_dttm", "vasopressor_dttm",
        "imv_dttm", "aki_dttm", "hyperbilirubinemia_dttm",
        "thrombocytopenia_dttm", "lactate_dttm"
    ]
    all_onset_cols_wo_lactate = [
        "blood_culture_dttm", "first_qad_dttm", "vasopressor_dttm",
        "imv_dttm", "aki_dttm", "hyperbilirubinemia_dttm",
        "thrombocytopenia_dttm"
    ]
    
    # Map column names to criteria names for output
    col_to_criteria = {
        "blood_culture_dttm": "blood_culture",
        "first_qad_dttm": "first_qad",
        "vasopressor_dttm": "vasopressor",
        "imv_dttm": "imv",
        "aki_dttm": "aki",
        "hyperbilirubinemia_dttm": "hyperbilirubinemia",
        "thrombocytopenia_dttm": "thrombocytopenia",
        "lactate_dttm": "lactate",
    }
    
    # Calculate ASE onset WITH lactate (vectorized min across columns)
    onset_df_w_lactate = result[all_onset_cols_w_lactate].copy()
    result["ase_onset_w_lactate_dttm"] = onset_df_w_lactate.min(axis=1)
    
    # Get first criteria name (vectorized idxmin)
    # idxmin returns the column name of the minimum value per row
    first_criteria_col_w_lactate = onset_df_w_lactate.idxmin(axis=1)
    # Map column names to criteria names, handling NaN (all values were NaT)
    result["ase_first_criteria_w_lactate"] = first_criteria_col_w_lactate.map(col_to_criteria)
    # Set to None where onset is NaT (no valid criteria)
    result.loc[result["ase_onset_w_lactate_dttm"].isna(), "ase_first_criteria_w_lactate"] = None
    
    # Calculate ASE onset WITHOUT lactate (vectorized min across columns)
    onset_df_wo_lactate = result[all_onset_cols_wo_lactate].copy()
    result["ase_onset_wo_lactate_dttm"] = onset_df_wo_lactate.min(axis=1)
    
    # Get first criteria name without lactate
    first_criteria_col_wo_lactate = onset_df_wo_lactate.idxmin(axis=1)
    result["ase_first_criteria_wo_lactate"] = first_criteria_col_wo_lactate.map(col_to_criteria)
    result.loc[result["ase_onset_wo_lactate_dttm"].isna(), "ase_first_criteria_wo_lactate"] = None
    
    # =========================================================================
    # VECTORIZED: Presumed infection onset (earliest of blood culture and first QAD)
    # =========================================================================
    pi_onset_cols = ["blood_culture_dttm", "first_qad_dttm"]
    pi_onset_df = result[pi_onset_cols].copy()
    presumed_infection_onset = pi_onset_df.min(axis=1)
    # Only set for rows with presumed_infection == 1
    result["presumed_infection_onset_dttm"] = np.where(
        result["presumed_infection"] == 1,
        presumed_infection_onset,
        pd.NaT
    )
    # Ensure proper datetime type
    result["presumed_infection_onset_dttm"] = pd.to_datetime(
        result["presumed_infection_onset_dttm"], errors="coerce"
    )
    
    # =========================================================================
    # VECTORIZED: Determine onset type (community vs hospital) based on onset day
    # =========================================================================
    # Convert to datetime for safe arithmetic
    onset_dates = pd.to_datetime(result["ase_onset_w_lactate_dttm"], errors="coerce")
    admission_dates = pd.to_datetime(result["admission_dttm"], errors="coerce")
    
    # Calculate hospital day (day 1 = admission day)
    # Using .dt.normalize() to get date-only comparison (midnight)
    hospital_day = (onset_dates.dt.normalize() - admission_dates.dt.normalize()).dt.days + 1
    
    # Vectorized conditional: community if hospital_day <= 2, else hospital
    result["type"] = np.where(
        onset_dates.isna() | admission_dates.isna(),
        None,
        np.where(hospital_day <= 2, "community", "hospital")
    )

    # Select and order final columns
    final_columns = [
        "hospitalization_id",
        "bc_id",
        "presumed_infection",
        "sepsis",
        "sepsis_wo_lactate",
        "type",
        "no_sepsis_reason",
        "blood_culture_dttm",
        "total_qad",
        "qad_start_date",
        "qad_end_date",
        "presumed_infection_onset_dttm",
        "ase_onset_w_lactate_dttm",
        "ase_onset_wo_lactate_dttm",
        "ase_first_criteria_w_lactate",
        "ase_first_criteria_wo_lactate",
        "vasopressor_dttm",
        "vasopressor_name",
        "imv_dttm",
        "aki_dttm",
        "hyperbilirubinemia_dttm",
        "thrombocytopenia_dttm",
        "lactate_dttm",
        "esrd",
    ]

    # =========================================================================
    # Memory Optimization: Convert to efficient dtypes
    # =========================================================================
    result_final = result[final_columns].copy()
    
    # Convert string columns to categorical for memory efficiency
    categorical_cols = ["type", "no_sepsis_reason", "vasopressor_name",
                        "ase_first_criteria_w_lactate", "ase_first_criteria_wo_lactate"]
    for col in categorical_cols:
        if col in result_final.columns:
            result_final[col] = result_final[col].astype("category")
    
    # Downcast integer columns
    int_cols = ["bc_id", "presumed_infection", "sepsis", "esrd"]
    for col in int_cols:
        if col in result_final.columns:
            result_final[col] = pd.to_numeric(result_final[col], downcast="integer")
    
    # Downcast float columns (total_qad can have NaN)
    if "total_qad" in result_final.columns:
        result_final["total_qad"] = pd.to_numeric(result_final["total_qad"], downcast="float")

    return result_final


# =============================================================================
# RIT Post-Processing
# =============================================================================


def _apply_rit_post_processing(
    results: pd.DataFrame,
    rit_days: int = 14
) -> pd.DataFrame:
    """
    Apply Repeat Infection Timeframe (RIT) as post-processing after ASE determination.

    CDC Definition (Page 9):
        "The repeat infection timeframe (RIT) is a timeframe after an ASE or BSE
        onset date when no new events are counted, in order to minimize the chance
        a single, prolonged episode of ASE or BSE is counted twice."

        "RIT therefore only applies to determination of hospital-onset events."

    This function:
    1. Separates ASE cases (sepsis=1) from non-ASE blood cultures
    2. Applies RIT to ASE cases only
    3. Assigns episode_id to ASE cases, NA to non-ASE cases
    4. Returns ALL blood cultures (both ASE and non-ASE)

    Parameters
    ----------
    results : pd.DataFrame
        Full results from _calculate_final_ase() containing both ASE and non-ASE rows
    rit_days : int, default=14
        Number of days for repeat infection timeframe

    Returns
    -------
    pd.DataFrame
        DataFrame containing ALL blood cultures with:
        - episode_id: Sequential ID (1, 2, 3...) for ASE cases, NA for non-ASE
        - bc_id: Original blood culture ID preserved
        - All other columns preserved
    """
    # Separate ASE cases from non-ASE blood cultures
    ase_cases = results[results["sepsis"] == 1].copy()
    non_ase_cases = results[results["sepsis"] != 1].copy()

    # Handle non-ASE cases: add episode_id = NA
    non_ase_cases["episode_id"] = pd.NA

    if len(ase_cases) == 0:
        # No ASE events - just return non-ASE cases with episode_id column
        # Reorder columns to put episode_id after bc_id
        cols = list(non_ase_cases.columns)
        if "episode_id" in cols and "bc_id" in cols:
            cols.remove("episode_id")
            bc_id_idx = cols.index("bc_id")
            cols.insert(bc_id_idx + 1, "episode_id")
            non_ase_cases = non_ase_cases[cols]
        return non_ase_cases

    # Sort ASE cases by hospitalization and onset date
    ase_cases = ase_cases.sort_values(
        ["hospitalization_id", "ase_onset_w_lactate_dttm"]
    ).reset_index(drop=True)

    # Apply RIT within each hospitalization
    def apply_rit_to_group(group: pd.DataFrame) -> pd.DataFrame:
        """Remove ASE events within 14 days of previous ASE onset."""
        if len(group) <= 1:
            return group

        kept_indices = [group.index[0]]  # Always keep first ASE
        last_onset = group.iloc[0]["ase_onset_w_lactate_dttm"]

        for idx in group.index[1:]:
            current_onset = group.loc[idx, "ase_onset_w_lactate_dttm"]

            # Handle NaT values
            if pd.isna(current_onset) or pd.isna(last_onset):
                continue

            # Calculate days since last ASE onset
            days_since_last = (current_onset - last_onset).days

            if days_since_last > rit_days:
                kept_indices.append(idx)
                last_onset = current_onset

        return group.loc[kept_indices]

    # Apply RIT per hospitalization
    ase_filtered = ase_cases.groupby(
        "hospitalization_id", group_keys=False
    ).apply(apply_rit_to_group)

    # Assign sequential episode_id within each hospitalization
    ase_filtered = ase_filtered.reset_index(drop=True)
    ase_filtered["episode_id"] = ase_filtered.groupby(
        "hospitalization_id"
    ).cumcount() + 1

    # Convert episode_id to appropriate dtype
    ase_filtered["episode_id"] = ase_filtered["episode_id"].astype("Int64")

    # Combine ASE and non-ASE cases
    combined = pd.concat([ase_filtered, non_ase_cases], ignore_index=True)

    # Reorder columns to put episode_id after bc_id
    cols = list(combined.columns)
    if "episode_id" in cols and "bc_id" in cols:
        cols.remove("episode_id")
        bc_id_idx = cols.index("bc_id")
        cols.insert(bc_id_idx + 1, "episode_id")
        combined = combined[cols]

    return combined


# =============================================================================
# Validation
# =============================================================================


def _validate_results(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply quality checks per CDC guidelines.

    Validates:
    - If sepsis == 1, then presumed_infection must == 1

    Returns
    -------
    pd.DataFrame
        Validated DataFrame (unchanged if valid)

    Raises
    ------
    ValueError
        If validation fails
    """
    # If sepsis == 1, then presumed_infection must == 1
    invalid = df[(df["sepsis"] == 1) & (df["presumed_infection"] == 0)]
    if len(invalid) > 0:
        raise ValueError(
            f"Found {len(invalid)} invalid rows: sepsis=1 with presumed_infection=0"
        )

    return df


# =============================================================================
# Main Public Function
# =============================================================================


def calculate_ase(
    hospitalization_ids: List[str],
    config_path: str = "clif_config.json",
    rit_days: int = 14,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Calculate Adult Sepsis Event (ASE) for given hospitalizations.

    Implements the CDC Adult Sepsis Event surveillance definition:
    - Criteria A: Presumed Infection (blood culture + ≥4 QAD)
    - Criteria B: Organ Dysfunction within ±2 days of blood culture

    Each blood culture is evaluated independently with its own ±2 day window.
    RIT (Repeat Infection Timeframe) is applied as post-processing AFTER
    determining which blood cultures meet ASE criteria.

    CDC Definition (Page 6):
        "Multiple window periods during a hospitalization are possible.
        If multiple blood cultures are obtained in a short period of time,
        window periods may overlap."

    CDC Definition (Page 9 - RIT):
        "The repeat infection timeframe (RIT) is a timeframe after an ASE
        onset date when no new events are counted... An RIT of 14 days is used."

    Parameters
    ----------
    hospitalization_ids : List[str]
        List of hospitalization IDs to evaluate
    config_path : str, default "clif_config.json"
        Path to clifpy config file
    rit_days : int, default 14
        Repeat Infection Timeframe in days. Applied as post-processing after
        ASE determination - ASE events within this many days of a previous
        ASE onset are excluded. Per CDC, 14 days is the recommended value.
    verbose : bool, default True
        Print progress messages

    Returns
    -------
    pd.DataFrame
        ALL blood cultures (both ASE and non-ASE) with columns:
        - hospitalization_id: Unique encounter ID
        - bc_id: Blood culture ID within hospitalization (original)
        - episode_id: Sequential ASE episode number after RIT (1, 2, 3...), NA for non-ASE
        - presumed_infection: 1 = met criteria, 0 = not met
        - sepsis: 1 = ASE case (with lactate), 0 = not ASE
        - sepsis_wo_lactate: 1 = ASE case (without lactate), 0 = not ASE
        - type: "community" or "hospital" (based on onset day)
        - presumed_infection_onset_dttm: Earliest of blood culture/first QAD
        - ase_onset_w_lactate_dttm: ASE onset including lactate
        - ase_onset_wo_lactate_dttm: ASE onset excluding lactate
        - ase_first_criteria_w_lactate: First criteria met (with lactate)
        - ase_first_criteria_wo_lactate: First criteria met (without lactate)
        - vasopressor_dttm: First qualifying vasopressor time
        - imv_dttm: First qualifying IMV time
        - aki_dttm: First AKI time
        - hyperbilirubinemia_dttm: First hyperbilirubinemia time
        - thrombocytopenia_dttm: First thrombocytopenia time
        - lactate_dttm: First elevated lactate time
        - esrd: 1 = has ESRD, 0 = no ESRD

    Example
    -------
    >>> from code.ASE import calculate_ase
    >>> hosp_ids = ['H001', 'H002', 'H003']
    >>> results = calculate_ase(hosp_ids, config_path='clif_config.json')
    >>> results.to_parquet('output/ase_results.parquet')

    Notes
    -----
    - Each blood culture is evaluated independently with its own ±2 day window
    - RIT is applied AFTER determining which blood cultures meet ASE criteria
    - Output contains ALL blood cultures (both ASE and non-ASE)
    - bc_id preserves the original blood culture ID; episode_id is assigned after RIT
    - For ASE cases: episode_id = 1, 2, 3... (sequential after RIT)
    - For non-ASE cases: episode_id = NA
    - To get first episode per hospitalization (ASE) or first blood culture (non-ASE):
        ase_first = results[results['episode_id'] == 1]
        non_ase_first = results[results['episode_id'].isna()].drop_duplicates('hospitalization_id')
    """
    if verbose:
        print("=== Adult Sepsis Event (ASE) Calculation ===")
        print(f"Processing {len(hospitalization_ids):,} hospitalizations...")
        print(f"Repeat Infection Timeframe (RIT): {rit_days} days")

    # Load config once
    with open(config_path) as f:
        config = json.load(f)
    data_directory = config.get('tables_path') or config.get('data_directory')
    filetype = config.get('file_type') or config.get('filetype')
    timezone = config.get('timezone')

    # Create DuckDB connection
    con = duckdb.connect(":memory:")
    con.execute(f"SET timezone = '{timezone}'")

    # =========================================================================
    # Load hospitalization + patient (keep throughout - small tables, used multiple times)
    # =========================================================================
    if verbose:
        print("Loading hospitalization and patient tables...")
    hosp_df = Hospitalization.from_file(
        data_directory=data_directory,
        filetype=filetype,
        timezone=timezone,
        filters={"hospitalization_id": hospitalization_ids},
    ).df
    con.register("hospitalization", hosp_df)

    patient_ids = hosp_df["patient_id"].unique().tolist()
    patient_df = Patient.from_file(
        data_directory=data_directory,
        filetype=filetype,
        timezone=timezone,
        filters={"patient_id": patient_ids},
    ).df
    con.register("patient", patient_df)

    # =========================================================================
    # Step 1: Get blood cultures (load microbiology → use → drop)
    # =========================================================================
    if verbose:
        print("Identifying blood cultures...")
    micro_df = MicrobiologyCulture.from_file(
        data_directory=data_directory,
        filetype=filetype,
        timezone=timezone,
        filters={
            "hospitalization_id": hospitalization_ids,
            "fluid_category": ["blood_buffy"],
        },
    ).df
    con.register("microbiology", micro_df)

    blood_cultures = _get_blood_cultures(con)

    # Drop microbiology table - no longer needed
    con.execute("DROP VIEW IF EXISTS microbiology")
    del micro_df
    gc.collect()

    n_hosp_with_bc = blood_cultures["hospitalization_id"].nunique()
    n_blood_cultures = len(blood_cultures)
    if verbose:
        print(f"  Found blood cultures for {n_hosp_with_bc:,} hospitalizations")
        print(f"  Total blood cultures to evaluate: {n_blood_cultures:,}")

    if len(blood_cultures) == 0:
        if verbose:
            print("No blood cultures found. Returning empty results.")
        con.close()
        return pd.DataFrame(columns=[
            "hospitalization_id", "bc_id", "episode_id", "presumed_infection",
            "sepsis", "type", "no_sepsis_reason", "blood_culture_dttm",
            "total_qad", "qad_start_date", "qad_end_date",
            "presumed_infection_onset_dttm", "ase_onset_w_lactate_dttm",
            "ase_onset_wo_lactate_dttm", "ase_first_criteria_w_lactate",
            "ase_first_criteria_wo_lactate", "vasopressor_dttm", "vasopressor_name",
            "imv_dttm", "aki_dttm", "hyperbilirubinemia_dttm",
            "thrombocytopenia_dttm", "lactate_dttm", "esrd"
        ])

    # Register blood_cultures_temp ONCE for reuse by all helper functions
    # Each blood culture is evaluated independently (no episode aggregation)
    con.register("blood_cultures_temp", blood_cultures)

    # =========================================================================
    # Step 2: Calculate QAD (load med_intermittent → use → drop)
    # =========================================================================
    if verbose:
        print("Calculating Qualifying Antimicrobial Days (QAD)...")
    med_int_df = MedicationAdminIntermittent.from_file(
        data_directory=data_directory,
        filetype=filetype,
        timezone=timezone,
        filters={
            "hospitalization_id": hospitalization_ids,
            "med_group": ["CMS_sepsis_qualifying_antibiotics"],
        },
    ).df
    con.register("med_intermittent", med_int_df)

    qad_results = _calculate_qad(con, blood_cultures)

    # Drop med_intermittent table - no longer needed
    con.execute("DROP VIEW IF EXISTS med_intermittent")
    del med_int_df
    gc.collect()

    if verbose:
        qad_with_value = qad_results[qad_results["total_qad"] >= 4]
        print(f"  {len(qad_with_value):,} blood cultures with ≥4 QAD")

    # =========================================================================
    # Step 3: Get ESRD flags (load diagnosis → use → drop)
    # =========================================================================
    if verbose:
        print("Identifying ESRD patients...")
    dx_df = HospitalDiagnosis.from_file(
        data_directory=data_directory,
        filetype=filetype,
        timezone=timezone,
        filters={"hospitalization_id": hospitalization_ids},
        columns=["hospitalization_id", "diagnosis_code", "diagnosis_code_format"],
    ).df
    con.register("diagnosis", dx_df)

    esrd_flags = _get_esrd_flags(con)

    # Drop diagnosis table - no longer needed
    con.execute("DROP VIEW IF EXISTS diagnosis")
    del dx_df
    gc.collect()

    if verbose:
        print(f"  Found {len(esrd_flags):,} hospitalizations with ESRD")

    # =========================================================================
    # Step 4: Determine presumed infection (uses hospitalization, patient - already loaded)
    # =========================================================================
    if verbose:
        print("Determining presumed infection status...")
    presumed_infection = _determine_presumed_infection(con, blood_cultures, qad_results)
    pi_count = presumed_infection["presumed_infection"].sum()
    if verbose:
        print(f"  {pi_count:,} blood cultures with presumed infection")

    # =========================================================================
    # Step 5: Get organ dysfunction
    # =========================================================================
    if verbose:
        print("Evaluating organ dysfunction criteria...")

    # 5a: Vasopressors (load med_continuous + adt → use → drop)
    med_cont_df = MedicationAdminContinuous.from_file(
        data_directory=data_directory,
        filetype=filetype,
        timezone=timezone,
        filters={
            "hospitalization_id": hospitalization_ids,
            "med_group": ["vasoactives"],
        },
    ).df
    con.register("med_continuous", med_cont_df)

    adt_df = Adt.from_file(
        data_directory=data_directory,
        filetype=filetype,
        timezone=timezone,
        filters={"hospitalization_id": hospitalization_ids},
        columns=["hospitalization_id", "in_dttm", "out_dttm", "location_category"],
    ).df
    con.register("adt", adt_df)

    vasopressor_df = _get_vasopressor_dysfunction(con, blood_cultures)

    # Drop med_continuous and adt tables - no longer needed
    con.execute("DROP VIEW IF EXISTS med_continuous")
    con.execute("DROP VIEW IF EXISTS adt")
    del med_cont_df, adt_df
    gc.collect()

    if verbose:
        print(f"  Vasopressor: {len(vasopressor_df):,} blood cultures")

    # 5b: IMV (load respiratory → use → drop)
    resp_df = RespiratorySupport.from_file(
        data_directory=data_directory,
        filetype=filetype,
        timezone=timezone,
        filters={"hospitalization_id": hospitalization_ids},
    ).df
    con.register("respiratory", resp_df)

    imv_df = _get_imv_dysfunction(con, blood_cultures)

    # Drop respiratory table - no longer needed
    con.execute("DROP VIEW IF EXISTS respiratory")
    del resp_df
    gc.collect()

    if verbose:
        print(f"  IMV: {len(imv_df):,} blood cultures")

    # 5c: Lab-based dysfunction (load labs → use → drop)
    labs_df = Labs.from_file(
        data_directory=data_directory,
        filetype=filetype,
        timezone=timezone,
        filters={
            "hospitalization_id": hospitalization_ids,
            "lab_category": ["creatinine", "bilirubin_total", "platelet_count", "lactate"],
        },
        columns=[
            "hospitalization_id",
            "lab_category",
            "lab_value",
            "lab_value_numeric",
            "lab_result_dttm",
            "lab_order_dttm",
        ],
    ).df
    con.register("labs", labs_df)

    lab_dysfunction = _get_lab_dysfunction(con, blood_cultures, esrd_flags)

    # Drop labs table - no longer needed
    con.execute("DROP VIEW IF EXISTS labs")
    del labs_df
    gc.collect()

    aki_count = lab_dysfunction["aki_dttm"].notna().sum()
    bili_count = lab_dysfunction["hyperbilirubinemia_dttm"].notna().sum()
    plt_count = lab_dysfunction["thrombocytopenia_dttm"].notna().sum()
    lac_count = lab_dysfunction["lactate_dttm"].notna().sum()
    if verbose:
        print(f"  AKI: {aki_count:,} blood cultures")
        print(f"  Hyperbilirubinemia: {bili_count:,} blood cultures")
        print(f"  Thrombocytopenia: {plt_count:,} blood cultures")
        print(f"  Elevated Lactate: {lac_count:,} blood cultures")

    # =========================================================================
    # Step 6: Calculate final ASE
    # =========================================================================
    if verbose:
        print("Calculating final ASE status...")
    result = _calculate_final_ase(
        presumed_infection,
        vasopressor_df,
        imv_df,
        lab_dysfunction,
        esrd_flags,
    )

    # Step 7: Validate results (before RIT filtering)
    if verbose:
        print("Validating results...")
    result = _validate_results(result)

    # Capture pre-RIT counts for summary
    pre_rit_ase_count = result["sepsis"].sum()

    # =========================================================================
    # Step 8: Apply RIT post-processing (filter to ASE events, apply RIT, assign episode_id)
    # =========================================================================
    if verbose:
        print(f"Applying RIT post-processing (RIT={rit_days} days)...")
    result = _apply_rit_post_processing(result, rit_days=rit_days)

    # Summary
    if verbose:
        total_bc = len(result)
        ase_events = result[result["sepsis"] == 1]
        ase_count = len(ase_events)
        pi_count = (result["presumed_infection"] == 1).sum()
        ase_wo_lactate_count = (result["sepsis_wo_lactate"] == 1).sum()
        community_count = (ase_events["type"] == "community").sum()
        hospital_count = (ase_events["type"] == "hospital").sum()
        n_hosp_with_sepsis = ase_events["hospitalization_id"].nunique()
        rit_removed = pre_rit_ase_count - ase_count
        print("\n=== ASE Calculation Complete ===")
        print(f"Total blood cultures evaluated: {total_bc:,}")
        print(f"  Presumed infection: {pi_count:,}")
        print(f"  ASE with lactate (sepsis): {ase_count:,}")
        print(f"  ASE without lactate: {ase_wo_lactate_count:,}")
        if rit_removed > 0:
            print(f"  (Removed {rit_removed:,} duplicate ASE events within {rit_days}d RIT)")
        print(f"Unique hospitalizations with ASE: {n_hosp_with_sepsis:,}")
        print(f"  Community-onset: {community_count:,}")
        print(f"  Hospital-onset: {hospital_count:,}")

    # Cleanup remaining tables and close connection
    del hosp_df, patient_df
    gc.collect()
    con.close()

    return result


# =============================================================================
# Convenience Functions
# =============================================================================


def calculate_ase_from_cohort(
    cohort_path: str,
    config_path: str = "clif_config.json",
    rit_days: int = 14,
    output_path: Optional[str] = None,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Calculate ASE from a cohort parquet file.

    Parameters
    ----------
    cohort_path : str
        Path to cohort parquet file (must have hospitalization_id column)
    config_path : str
        Path to clifpy config file
    rit_days : int, default 14
        Repeat Infection Timeframe in days
    output_path : str, optional
        If provided, save results to this path
    verbose : bool
        Print progress messages

    Returns
    -------
    pd.DataFrame
        ASE results
    """
    # Load cohort
    cohort = pd.read_parquet(cohort_path)
    hosp_ids = cohort["hospitalization_id"].astype(str).unique().tolist()

    # Calculate ASE
    results = calculate_ase(hosp_ids, config_path=config_path, rit_days=rit_days, verbose=verbose)

    # Save if output path provided
    if output_path:
        results.to_parquet(output_path, index=False)
        if verbose:
            print(f"Results saved to: {output_path}")

    return results


# =============================================================================
# Main Entry Point
# =============================================================================


if __name__ == "__main__":
    # Example usage
    import argparse

    parser = argparse.ArgumentParser(description="Calculate Adult Sepsis Event (ASE)")
    parser.add_argument(
        "--cohort",
        type=str,
        default="PHI_DATA/cohort_icu_first_stay.parquet",
        help="Path to cohort parquet file",
    )
    parser.add_argument(
        "--config",
        type=str,
        default="clif_config.json",
        help="Path to clifpy config file",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="PHI_DATA/ase_results.parquet",
        help="Path to save results",
    )

    args = parser.parse_args()

    results = calculate_ase_from_cohort(
        cohort_path=args.cohort,
        config_path=args.config,
        output_path=args.output,
        verbose=True,
    )

    print("\nSample results:")
    print(results.head())
