import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    mo.md(r"""
    # Sepsis Definitions Cohort Identification

    This notebook builds the cohort for the sepsis definitions comparison study (ASE with/without lactate vs BSE).

    ## Inclusion Criteria
    - Adult hospitalized patients aged 18 years or older
    - Admitted 2018-2024
    - Admitted via the ED
    - Admitted to academic or community hospitals (excluding LTACH)
    - Must have ED location during hospitalization
    """)
    return


@app.cell
def _():
    import json
    import pandas as pd
    from pathlib import Path
    from clifpy.tables import Patient, Hospitalization, Adt
    return Adt, Hospitalization, Path, Patient, json, pd


@app.cell
def _(Path, json):
    # Load configuration (run notebook from project root)
    config_path = Path("clif_config.json")
    with open(config_path, "r") as f:
        config = json.load(f)

    DATA_DIR = config["data_directory"]
    FILETYPE = config["filetype"]
    TIMEZONE = config["timezone"]
    OUTPUT_DIR = Path(config["output_directory"])
    SITE_NAME = config["site_name"]

    print(f"Site: {SITE_NAME}")
    print(f"Data directory: {DATA_DIR}")
    print(f"Filetype: {FILETYPE}")
    print(f"Timezone: {TIMEZONE}")
    return DATA_DIR, FILETYPE, OUTPUT_DIR, SITE_NAME, TIMEZONE


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 1: Load Core Tables
    """)
    return


@app.cell
def _(Adt, DATA_DIR, FILETYPE, Hospitalization, Patient, TIMEZONE):
    # Load patient, hospitalization, and adt tables
    patient = Patient.from_file(
        data_directory=DATA_DIR,
        filetype=FILETYPE,
        timezone=TIMEZONE
    )

    hospitalization = Hospitalization.from_file(
        data_directory=DATA_DIR,
        filetype=FILETYPE,
        timezone=TIMEZONE
    )

    adt = Adt.from_file(
        data_directory=DATA_DIR,
        filetype=FILETYPE,
        timezone=TIMEZONE
    )

    print(f"Patient: {len(patient.df):,} rows")
    print(f"Hospitalization: {len(hospitalization.df):,} rows")
    print(f"ADT: {len(adt.df):,} rows")
    return adt, hospitalization, patient


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 2: Apply Inclusion Criteria
    """)
    return


@app.cell
def _(adt, hospitalization, pd):
    # Get dataframes
    hosp_df = hospitalization.df.copy()
    adt_df = adt.df.copy()

    # Merge hospitalization and ADT to get hospital_type
    merged_df = pd.merge(
        hosp_df,
        adt_df[["hospitalization_id", "hospital_id", "hospital_type", "in_dttm", "location_category"]].drop_duplicates(),
        on="hospitalization_id",
        how="inner"
    )

    print(f"Total hospitalizations after merge: {merged_df['hospitalization_id'].nunique():,}")
    return (merged_df,)


@app.cell
def _(merged_df):
    # Filter 1: Adults (age >= 18)
    cohort_df = merged_df[
        (merged_df["age_at_admission"] >= 18) &
        (merged_df["age_at_admission"].notna())
    ].copy()

    print(f"After age filter (>=18): {cohort_df['hospitalization_id'].nunique():,} hospitalizations")
    return (cohort_df,)


@app.cell
def _(cohort_df):
    # Filter 2: Date range 2018-2024
    cohort_df_filtered = cohort_df[
        (cohort_df["admission_dttm"].dt.year >= 2018) &
        (cohort_df["admission_dttm"].dt.year <= 2024)
    ].copy()

    print(f"After date filter (2018-2024): {cohort_df_filtered['hospitalization_id'].nunique():,} hospitalizations")
    return (cohort_df_filtered,)


@app.cell
def _(cohort_df_filtered):
    # Filter 3: Admitted via ED
    cohort_df_ed = cohort_df_filtered[
        cohort_df_filtered["admission_type_category"] == "ed"
    ].copy()

    print(f"After ED admission filter: {cohort_df_ed['hospitalization_id'].nunique():,} hospitalizations")
    return (cohort_df_ed,)


@app.cell
def _(cohort_df_ed):
    # Filter 4: Academic or community hospitals (exclude LTACH)
    cohort_df_hospital = cohort_df_ed[
        cohort_df_ed["hospital_type"].isin(["academic", "community"])
    ].copy()

    print(f"After hospital type filter (academic/community): {cohort_df_hospital['hospitalization_id'].nunique():,} hospitalizations")
    return (cohort_df_hospital,)


@app.cell
def _(adt, cohort_df_hospital):
    # Filter 5: Must have ED location during hospitalization
    hosp_with_ed = set(adt.df[adt.df["location_category"] == "ed"]["hospitalization_id"].unique())

    cohort_df_final = cohort_df_hospital[
        cohort_df_hospital["hospitalization_id"].isin(hosp_with_ed)
    ].copy()

    print(f"After ED location filter: {cohort_df_final['hospitalization_id'].nunique():,} hospitalizations")
    return (cohort_df_final,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 3: Build Final Cohort with Demographics
    """)
    return


@app.cell
def _(cohort_df_final, patient, pd):
    # Build final cohort with one row per hospitalization
    final_cohort = cohort_df_final.drop_duplicates(subset=["hospitalization_id"]).copy()

    # Add patient demographics
    patient_df = patient.df[["patient_id", "death_dttm", "race_category", "sex_category", "ethnicity_category"]]
    final_cohort = pd.merge(final_cohort, patient_df, on="patient_id", how="left")

    print("Final Cohort Summary:")
    print(f"  Hospitalizations: {final_cohort['hospitalization_id'].nunique():,}")
    print(f"  Unique patients: {final_cohort['patient_id'].nunique():,}")
    return (final_cohort,)


@app.cell
def _(final_cohort):
    # Display cohort demographics
    print("Demographics:")
    print(f"  Age: mean={final_cohort['age_at_admission'].mean():.1f}, median={final_cohort['age_at_admission'].median():.1f}")
    print("Sex distribution:")
    print(final_cohort["sex_category"].value_counts())
    print("Hospital type distribution:")
    print(final_cohort["hospital_type"].value_counts())
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 4: Save Cohort to Parquet
    """)
    return


@app.cell
def _(OUTPUT_DIR, SITE_NAME, final_cohort):
    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Save cohort
    cohort_output_path = OUTPUT_DIR / f"{SITE_NAME}_cohort_df.parquet"
    final_cohort.to_parquet(cohort_output_path, index=False)

    print(f"Cohort saved to: {cohort_output_path}")
    print(f"Shape: {final_cohort.shape}")
    return


@app.cell
def _(final_cohort):
    # Display final cohort
    final_cohort.head(10)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 5: Calculate Adult Sepsis Event (ASE)
    """)
    return


@app.cell
def _():
    from clifpy.utils.ase import compute_ase
    return (compute_ase,)


@app.cell
def _(compute_ase, final_cohort, pd):
    # Get hospitalization IDs from cohort
    hosp_ids = final_cohort["hospitalization_id"].astype(str).unique().tolist()
    print(f"Running ASE calculation on {len(hosp_ids):,} hospitalizations...")

    # Calculate ASE using clifpy (returns ALL blood cultures, both ASE and non-ASE)
    # include_lactate=True is REQUIRED to compare lactate vs non-lactate definitions
    # - sepsis: ASE with lactate as organ dysfunction criterion
    # - sepsis_wo_lactate: ASE without lactate criterion
    ase_results_all = compute_ase(
        hospitalization_ids=hosp_ids,
        config_path="clif_config.json",
        apply_rit=True,
        rit_only_hospital_onset=True,
        include_lactate=True,  # Must be True to compare lactate vs non-lactate
        verbose=True
    )

    # Keep first ASE episode OR first blood culture (for non-ASE patients)
    # For ASE patients: episode_id == 1 (first ASE after RIT)
    # For non-ASE patients: first blood culture by date (episode_id is NA)
    ase_first = ase_results_all[ase_results_all["episode_id"] == 1].copy()
    non_ase_first = (
        ase_results_all[ase_results_all["episode_id"].isna()]
        .sort_values("blood_culture_dttm")
        .drop_duplicates(subset=["hospitalization_id"], keep="first")
    )
    ase_results = pd.concat([ase_first, non_ase_first], ignore_index=True)

    print(f"Filtered to first episode/BC: {len(ase_results):,} hospitalizations")
    print(f"  - ASE cases: {ase_first['hospitalization_id'].nunique():,}")
    print(f"  - Non-ASE with blood culture: {non_ase_first['hospitalization_id'].nunique():,}")
    return (ase_results,)


@app.cell
def _(ase_results):
    # Display ASE summary
    print("ASE Results Summary:")
    print(f"  Total blood cultures evaluated: {len(ase_results):,}")
    print(f"  Presumed infection: {ase_results['presumed_infection'].sum():,}")
    print(f"  ASE with lactate (sepsis): {ase_results['sepsis'].sum():,}")
    print(f"  ASE without lactate: {ase_results['sepsis_wo_lactate'].sum():,}")
    if "type" in ase_results.columns:
        ase_only = ase_results[ase_results['sepsis'] == 1]
        print(f"  Community-onset ASE: {(ase_only['type'] == 'community').sum():,}")
        print(f"  Hospital-onset ASE: {(ase_only['type'] == 'hospital').sum():,}")
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 6: Save ASE Results to Parquet
    """)
    return


@app.cell
def _(OUTPUT_DIR, SITE_NAME, ase_results):
    # Save ASE results
    ase_output_path = OUTPUT_DIR / f"{SITE_NAME}_ase_results.parquet"
    ase_results.to_parquet(ase_output_path, index=False)

    print(f"ASE results saved to: {ase_output_path}")
    print(f"Shape: {ase_results.shape}")
    return


@app.cell
def _(ase_results):
    # Display ASE results
    ase_results
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
