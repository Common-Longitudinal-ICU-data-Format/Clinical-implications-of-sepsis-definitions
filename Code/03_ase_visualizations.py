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
    # ASE Organ Dysfunction Visualizations

    This notebook creates visualizations showing the sequence of organ dysfunctions
    in Adult Sepsis Events (ASE).

    ## Outputs
    1. **Sankey plot for ASE with lactate** - 6-level flow showing organ failure sequence
    2. **Sankey plot for ASE without lactate** - 5-level flow (excluding lactate criterion)
    3. **QAD distribution plot** - Histogram comparing QAD days between ASE groups
    4. **Yearly cases plot** - Line chart of ASE cases by year
    5. **Yearly organ dysfunctions plot** - Multi-line chart of organ failures by year
    6. **Yearly onset type plot** - Line chart of Hospital vs Community onset by year
    7. **Yearly ED hospitalizations summary** - Descriptive statistics of yearly ED visits by hospital
    8. **Top 20 QAD antimicrobials** - Frequency tables of most common antimicrobials in QAD runs
    """)
    return


@app.cell
def _():
    import json
    import pandas as pd
    from pathlib import Path
    import plotly.graph_objects as go
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from clifpy.tables import Labs
    return Labs, Path, go, json, matplotlib, pd, plt


@app.cell
def _(Path, json):
    # Load configuration
    config_path = Path("clif_config.json")
    config = json.loads(config_path.read_text())

    CLIF_DATA_DIR = Path(config["data_directory"])
    FILETYPE = config["filetype"]
    TIMEZONE = config["timezone"]
    OUTPUT_DIR = Path(config["output_directory"])
    PHI_DIR = Path(config["phi_directory"])
    SITE_NAME = config["site_name"]

    print(f"Site: {SITE_NAME}")
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"PHI directory: {PHI_DIR}")
    print(f"CLIF data directory: {CLIF_DATA_DIR}")
    return CLIF_DATA_DIR, FILETYPE, OUTPUT_DIR, PHI_DIR, SITE_NAME, TIMEZONE


@app.cell
def _(PHI_DIR, SITE_NAME, pd):
    # Load ASE results from cohort notebook (PHI data)
    ase_results_path = PHI_DIR / "ase_results.parquet"
    ase_df = pd.read_parquet(ase_results_path)

    print(f"Loaded ASE results: {len(ase_df):,} records")
    print(f"ASE with lactate (sepsis=1): {ase_df['sepsis'].sum():,}")
    print(f"ASE without lactate (sepsis_wo_lactate=1): {ase_df['sepsis_wo_lactate'].sum():,}")
    return (ase_df,)


@app.cell
def _(PHI_DIR, SITE_NAME, pd):
    cohort_df = pd.read_parquet(
        PHI_DIR / "cohort_df.parquet",
        columns=["hospitalization_id", "hospital_id", "hospital_type", "admission_dttm"]
    ).drop_duplicates(subset=["hospitalization_id"])
    print(f"Loaded cohort: {len(cohort_df):,} hospitalizations")
    return (cohort_df,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Monthly ED Hospitalizations by Hospital

    Since the entire cohort is ED-admitted, this section summarizes the average
    monthly adult hospitalizations from the ED, stratified by hospital and hospital type.
    Descriptive statistics (mean, SD, median, IQR) of monthly ED visit counts are reported.
    """)
    return


@app.cell
def _(cohort_df, pd, mo):
    import numpy as np

    # Derive month from admission datetime
    _cohort = cohort_df.copy()
    _cohort["month"] = pd.to_datetime(_cohort["admission_dttm"]).dt.to_period("M").astype(str)

    # Count hospitalizations per (month, hospital_id, hospital_type)
    monthly_hosp_counts = (
        _cohort.groupby(["month", "hospital_id", "hospital_type"])
        .size()
        .reset_index(name="n_hospitalizations")
    )

    # Summary stats per (hospital_id, hospital_type)
    def _summary(group):
        return pd.Series({
            "mean": group["n_hospitalizations"].mean(),
            "sd": group["n_hospitalizations"].std(),
            "median": group["n_hospitalizations"].median(),
            "q1": group["n_hospitalizations"].quantile(0.25),
            "q3": group["n_hospitalizations"].quantile(0.75),
            "n_months": len(group),
        })

    hospital_summary = (
        monthly_hosp_counts
        .groupby(["hospital_id", "hospital_type"])
        .apply(_summary)
        .reset_index()
    )

    # Overall summary (all hospitals combined per month)
    monthly_totals = (
        _cohort.groupby("month")
        .size()
        .reset_index(name="n_hospitalizations")
    )
    overall_row = pd.DataFrame([{
        "hospital_id": "Overall",
        "hospital_type": "All",
        "mean": monthly_totals["n_hospitalizations"].mean(),
        "sd": monthly_totals["n_hospitalizations"].std(),
        "median": monthly_totals["n_hospitalizations"].median(),
        "q1": monthly_totals["n_hospitalizations"].quantile(0.25),
        "q3": monthly_totals["n_hospitalizations"].quantile(0.75),
        "n_months": len(monthly_totals),
    }])

    monthly_ed_summary = pd.concat([hospital_summary, overall_row], ignore_index=True)

    # Round for display
    for _col in ["mean", "sd", "median", "q1", "q3"]:
        monthly_ed_summary[_col] = monthly_ed_summary[_col].round(1)

    print("Monthly ED Hospitalizations — Summary Statistics")
    print("=" * 70)
    print(monthly_ed_summary.to_string(index=False))
    print(f"\nMonths covered: {sorted(_cohort['month'].unique())}")

    mo.md(f"""
    ### Summary Table
    {mo.as_html(monthly_ed_summary)}
    """)
    return monthly_hosp_counts, monthly_ed_summary


@app.cell
def _(pd):
    # Define organ dysfunction columns and labels
    ORGAN_COLS_WITH_LACTATE = {
        'vasopressor_dttm': 'Vasopressor',
        'imv_dttm': 'IMV',
        'aki_dttm': 'AKI',
        'hyperbilirubinemia_dttm': 'Hyperbilirubinemia',
        'thrombocytopenia_dttm': 'Thrombocytopenia',
        'lactate_dttm': 'Lactate'
    }

    ORGAN_COLS_WITHOUT_LACTATE = {
        'vasopressor_dttm': 'Vasopressor',
        'imv_dttm': 'IMV',
        'aki_dttm': 'AKI',
        'hyperbilirubinemia_dttm': 'Hyperbilirubinemia',
        'thrombocytopenia_dttm': 'Thrombocytopenia'
    }

    def prepare_organ_sequence(df, organ_cols):
        """
        For each hospitalization, order organ dysfunctions by datetime.
        Returns a list of ordered organ labels (earliest first), with None for missing.

        Parameters:
        -----------
        df : DataFrame
            ASE results with organ dysfunction datetime columns
        organ_cols : dict
            Mapping of column names to labels

        Returns:
        --------
        DataFrame with hospitalization_id and ordered organ sequence columns
        """
        n_levels = len(organ_cols)
        sequences = []

        for _, row in df.iterrows():
            # Collect (datetime, label) pairs for non-null values
            organ_times = []
            for col, label in organ_cols.items():
                dttm = row.get(col)
                if pd.notna(dttm):
                    organ_times.append((pd.to_datetime(dttm), label))

            # Sort by datetime (earliest first)
            organ_times.sort(key=lambda x: x[0])

            # Extract ordered labels, pad with "None" for missing slots
            ordered_labels = [t[1] for t in organ_times]
            while len(ordered_labels) < n_levels:
                ordered_labels.append("None")

            sequences.append({
                'hospitalization_id': row['hospitalization_id'],
                **{f'level_{i+1}': ordered_labels[i] for i in range(n_levels)}
            })

        return pd.DataFrame(sequences)
    return (
        ORGAN_COLS_WITHOUT_LACTATE,
        ORGAN_COLS_WITH_LACTATE,
        prepare_organ_sequence,
    )


@app.cell
def _(go):
    def create_sankey_data(sequence_df, n_levels, title="Organ Dysfunction Sequence"):
        """
        Create Sankey diagram data from organ sequence DataFrame.

        Parameters:
        -----------
        sequence_df : DataFrame
            Output from prepare_organ_sequence with level_1, level_2, etc. columns
        n_levels : int
            Number of levels in the Sankey (e.g., 6 for with lactate, 5 for without)
        title : str
            Title for the Sankey diagram

        Returns:
        --------
        Plotly Figure object
        """
        # Get all unique labels across all levels
        all_labels = set()
        for i in range(1, n_levels + 1):
            all_labels.update(sequence_df[f'level_{i}'].unique())
        all_labels = sorted(list(all_labels))

        # Create node labels: each level has its own set of nodes
        # Format: "Level N: Label"
        node_labels = []
        node_to_idx = {}

        for level in range(1, n_levels + 1):
            for label in all_labels:
                node_name = f"L{level}: {label}"
                node_to_idx[node_name] = len(node_labels)
                node_labels.append(node_name)

        # Count transitions between consecutive levels
        sources = []
        targets = []
        values = []

        for level in range(1, n_levels):
            # Group by source->target transition
            transition_counts = sequence_df.groupby(
                [f'level_{level}', f'level_{level+1}']
            ).size().reset_index(name='count')

            for _, row in transition_counts.iterrows():
                src_label = f"L{level}: {row[f'level_{level}']}"
                tgt_label = f"L{level+1}: {row[f'level_{level+1}']}"

                sources.append(node_to_idx[src_label])
                targets.append(node_to_idx[tgt_label])
                values.append(row['count'])

        # Define colors for each organ type
        color_map = {
            'Vasopressor': 'rgba(255, 99, 71, 0.8)',   # Tomato red
            'IMV': 'rgba(30, 144, 255, 0.8)',          # Dodger blue
            'AKI': 'rgba(255, 165, 0, 0.8)',           # Orange
            'Hyperbilirubinemia': 'rgba(255, 215, 0, 0.8)',  # Gold
            'Thrombocytopenia': 'rgba(147, 112, 219, 0.8)', # Medium purple
            'Lactate': 'rgba(60, 179, 113, 0.8)',      # Medium sea green
            'None': 'rgba(192, 192, 192, 0.4)'         # Light gray
        }

        # Assign colors to nodes
        node_colors = []
        for label in node_labels:
            # Extract the organ name from "L1: Vasopressor" format
            organ = label.split(': ', 1)[1] if ': ' in label else 'None'
            node_colors.append(color_map.get(organ, 'rgba(128, 128, 128, 0.5)'))

        # Assign colors to links based on source node
        link_colors = []
        for src in sources:
            src_label = node_labels[src]
            organ = src_label.split(': ', 1)[1] if ': ' in src_label else 'None'
            # Make link colors slightly more transparent
            base_color = color_map.get(organ, 'rgba(128, 128, 128, 0.3)')
            link_colors.append(base_color.replace('0.8)', '0.4)').replace('0.4)', '0.3)'))

        # Create Sankey figure
        fig = go.Figure(data=[go.Sankey(
            node=dict(
                pad=15,
                thickness=20,
                line=dict(color="black", width=0.5),
                label=node_labels,
                color=node_colors
            ),
            link=dict(
                source=sources,
                target=targets,
                value=values,
                color=link_colors
            )
        )])

        fig.update_layout(
            title_text=title,
            font_size=10,
            height=600,
            width=1200
        )

        return fig
    return (create_sankey_data,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Sankey Plot: ASE WITH Lactate

    Shows the sequence of organ dysfunction for patients meeting ASE criteria
    when lactate is included as an organ dysfunction criterion.
    """)
    return


@app.cell
def _(
    ORGAN_COLS_WITH_LACTATE,
    ase_df,
    create_sankey_data,
    pd,
    prepare_organ_sequence,
):
    # Filter to ASE with lactate patients
    ase_w_lactate_df = ase_df[ase_df['sepsis'] == 1].copy()
    print(f"ASE with lactate patients: {len(ase_w_lactate_df):,}")

    # Prepare organ sequence
    sequence_w_lactate = prepare_organ_sequence(ase_w_lactate_df, ORGAN_COLS_WITH_LACTATE)

    # Create Sankey plot
    sankey_w_lactate = create_sankey_data(
        sequence_w_lactate,
        n_levels=6,
        title="Organ Dysfunction Sequence - ASE WITH Lactate"
    )

    # Create aggregated transition counts for export (no PHI)
    transitions_w_lactate = []
    for _level in range(1, 6):  # 5 transitions for 6 levels
        _counts = sequence_w_lactate.groupby(
            [f'level_{_level}', f'level_{_level+1}']
        ).size().reset_index(name='count')
        _counts.columns = ['source_organ', 'target_organ', 'count']
        _counts['source_level'] = _level
        _counts['target_level'] = _level + 1
        transitions_w_lactate.append(_counts)
    sankey_w_lactate_data = pd.concat(transitions_w_lactate, ignore_index=True)
    sankey_w_lactate_data = sankey_w_lactate_data[['source_level', 'source_organ', 'target_level', 'target_organ', 'count']]

    sankey_w_lactate
    return ase_w_lactate_df, sankey_w_lactate, sankey_w_lactate_data


@app.cell
def _(mo):
    mo.md(r"""
    ## Sankey Plot: ASE WITHOUT Lactate

    Shows the sequence of organ dysfunction for patients meeting ASE criteria
    when lactate is NOT included as an organ dysfunction criterion.
    """)
    return


@app.cell
def _(
    ORGAN_COLS_WITHOUT_LACTATE,
    ase_df,
    create_sankey_data,
    pd,
    prepare_organ_sequence,
):
    # Filter to ASE without lactate patients
    ase_wo_lactate_df = ase_df[ase_df['sepsis_wo_lactate'] == 1].copy()
    print(f"ASE without lactate patients: {len(ase_wo_lactate_df):,}")

    # Prepare organ sequence
    sequence_wo_lactate = prepare_organ_sequence(ase_wo_lactate_df, ORGAN_COLS_WITHOUT_LACTATE)

    # Create Sankey plot
    sankey_wo_lactate = create_sankey_data(
        sequence_wo_lactate,
        n_levels=5,
        title="Organ Dysfunction Sequence - ASE WITHOUT Lactate"
    )

    # Create aggregated transition counts for export (no PHI)
    transitions_wo_lactate = []
    for _level in range(1, 5):  # 4 transitions for 5 levels
        _counts = sequence_wo_lactate.groupby(
            [f'level_{_level}', f'level_{_level+1}']
        ).size().reset_index(name='count')
        _counts.columns = ['source_organ', 'target_organ', 'count']
        _counts['source_level'] = _level
        _counts['target_level'] = _level + 1
        transitions_wo_lactate.append(_counts)
    sankey_wo_lactate_data = pd.concat(transitions_wo_lactate, ignore_index=True)
    sankey_wo_lactate_data = sankey_wo_lactate_data[['source_level', 'source_organ', 'target_level', 'target_organ', 'count']]

    sankey_wo_lactate
    return ase_wo_lactate_df, sankey_wo_lactate, sankey_wo_lactate_data


@app.cell
def _(mo):
    mo.md(r"""
    ## QAD Days Distribution

    Shows the distribution of Qualifying Antimicrobial Days (QAD) comparing
    ASE with lactate vs ASE without lactate groups.
    """)
    return


@app.cell
def _(go):
    def create_qad_distribution(ase_df_with_lactate, ase_df_without_lactate, title="QAD Days Distribution"):
        """
        Create side-by-side bar chart comparing QAD distribution between ASE groups.
        QAD ranges from 0 to 8 days.
        """
        qad_with = ase_df_with_lactate['total_qad'].dropna().astype(int)
        qad_without = ase_df_without_lactate['total_qad'].dropna().astype(int)

        # Individual days 0-8
        labels = list(range(0, 9))

        with_counts = qad_with.value_counts().reindex(labels, fill_value=0).sort_index()
        without_counts = qad_without.value_counts().reindex(labels, fill_value=0).sort_index()

        fig = go.Figure()

        # ASE with lactate
        fig.add_trace(go.Bar(
            x=[str(l) for l in labels],
            y=[with_counts.get(l, 0) for l in labels],
            name='ASE with Lactate',
            marker_color='rgba(60, 179, 113, 0.8)'  # Medium sea green
        ))

        # ASE without lactate
        fig.add_trace(go.Bar(
            x=[str(l) for l in labels],
            y=[without_counts.get(l, 0) for l in labels],
            name='ASE without Lactate',
            marker_color='rgba(30, 144, 255, 0.8)'  # Dodger blue
        ))

        fig.update_layout(
            title_text=title,
            xaxis_title="QAD Days",
            yaxis_title="Count",
            barmode='group',
            height=500,
            width=900,
            legend=dict(x=0.7, y=0.95)
        )

        return fig
    return (create_qad_distribution,)


@app.cell
def _(ase_df, create_qad_distribution, pd):
    # Filter to each group
    ase_w_lactate_qad = ase_df[ase_df['sepsis'] == 1].copy()
    ase_wo_lactate_qad = ase_df[ase_df['sepsis_wo_lactate'] == 1].copy()

    # Create distribution plot
    qad_distribution = create_qad_distribution(
        ase_w_lactate_qad,
        ase_wo_lactate_qad,
        title="QAD Days Distribution: ASE with vs without Lactate"
    )

    # Create QAD data DataFrame for export (QAD ranges 0-8)
    _qad_labels = list(range(0, 9))
    _qad_with = ase_w_lactate_qad['total_qad'].dropna().astype(int)
    _qad_without = ase_wo_lactate_qad['total_qad'].dropna().astype(int)
    _with_counts = _qad_with.value_counts().reindex(_qad_labels, fill_value=0).sort_index()
    _without_counts = _qad_without.value_counts().reindex(_qad_labels, fill_value=0).sort_index()
    qad_data_df = pd.DataFrame({
        'qad_days': _qad_labels,
        'ase_with_lactate_count': [_with_counts.get(l, 0) for l in _qad_labels],
        'ase_without_lactate_count': [_without_counts.get(l, 0) for l in _qad_labels]
    })

    # Print summary statistics
    print("QAD Summary Statistics:")
    print(f"\nASE WITH Lactate (n={len(ase_w_lactate_qad):,}):")
    print(f"  Mean: {ase_w_lactate_qad['total_qad'].mean():.1f}")
    print(f"  Median: {ase_w_lactate_qad['total_qad'].median():.1f}")
    print(f"  Range: {ase_w_lactate_qad['total_qad'].min():.0f} - {ase_w_lactate_qad['total_qad'].max():.0f}")

    print(f"\nASE WITHOUT Lactate (n={len(ase_wo_lactate_qad):,}):")
    print(f"  Mean: {ase_wo_lactate_qad['total_qad'].mean():.1f}")
    print(f"  Median: {ase_wo_lactate_qad['total_qad'].median():.1f}")
    print(f"  Range: {ase_wo_lactate_qad['total_qad'].min():.0f} - {ase_wo_lactate_qad['total_qad'].max():.0f}")

    qad_distribution
    return qad_data_df, qad_distribution


@app.cell
def _(mo):
    mo.md(r"""
    ## Top 20 QAD Antimicrobials

    Frequency table of the most common antimicrobials contributing to
    Qualifying Antimicrobial Days (QADs) in ASE episodes, for both
    with-lactate and without-lactate definitions.
    """)
    return


@app.cell
def _(ase_df, mo, pd):
    def _top20_with_other(series):
        """From an exploded med Series, return top-20 + Other DataFrame."""
        counts = series.value_counts()
        top20 = counts.head(20)
        other_count = counts.iloc[20:].sum() if len(counts) > 20 else 0
        result = top20.reset_index()
        result.columns = ["antimicrobial", "count"]
        if other_count > 0:
            result = pd.concat(
                [result, pd.DataFrame([{"antimicrobial": "Other", "count": other_count}])],
                ignore_index=True,
            )
        total = result["count"].sum()
        result["pct"] = (result["count"] / total * 100).round(1)
        return result

    # --- ASE WITH lactate ---
    ase_w_meds = ase_df.loc[ase_df["sepsis"] == 1, "run_meds"].dropna()
    exploded_w = ase_w_meds.str.split(", ").explode()
    top20_meds_w_lactate = _top20_with_other(exploded_w)

    # --- ASE WITHOUT lactate ---
    ase_wo_meds = ase_df.loc[ase_df["sepsis_wo_lactate"] == 1, "run_meds"].dropna()
    exploded_wo = ase_wo_meds.str.split(", ").explode()
    top20_meds_wo_lactate = _top20_with_other(exploded_wo)

    print("Top 20 QAD Antimicrobials — WITH Lactate")
    print("=" * 50)
    print(top20_meds_w_lactate.to_string(index=False))

    print(f"\nTop 20 QAD Antimicrobials — WITHOUT Lactate")
    print("=" * 50)
    print(top20_meds_wo_lactate.to_string(index=False))

    mo.md(f"""
    ### ASE WITH Lactate
    {mo.as_html(top20_meds_w_lactate)}

    ### ASE WITHOUT Lactate
    {mo.as_html(top20_meds_wo_lactate)}
    """)
    return top20_meds_w_lactate, top20_meds_wo_lactate


@app.cell
def _(mo):
    mo.md(r"""
    ## Monthly ASE Cases

    Shows the monthly trend of ASE cases comparing with lactate vs without lactate criteria.
    """)
    return


@app.cell
def _(ase_df, go, pd):
    # Extract year-month from blood culture date
    ase_monthly = ase_df.copy()
    ase_monthly['blood_culture_dttm'] = pd.to_datetime(ase_monthly['blood_culture_dttm'])
    ase_monthly['year'] = ase_monthly['blood_culture_dttm'].dt.year

    # Filter to 2018-2024 only
    ase_monthly = ase_monthly[(ase_monthly['year'] >= 2018) & (ase_monthly['year'] <= 2024)]
    ase_monthly['year_month'] = ase_monthly['blood_culture_dttm'].dt.to_period('M')

    # Count cases per year-month
    yearly_cases = ase_monthly.groupby('year_month').agg({
        'sepsis': 'sum',
        'sepsis_wo_lactate': 'sum'
    }).reset_index()
    yearly_cases['year_month'] = yearly_cases['year_month'].astype(str)

    # Create line chart
    yearly_cases_fig = go.Figure()

    yearly_cases_fig.add_trace(go.Scatter(
        x=yearly_cases['year_month'],
        y=yearly_cases['sepsis'],
        mode='lines+markers',
        name='ASE with Lactate',
        line=dict(color='rgba(60, 179, 113, 1)', width=2),
        marker=dict(size=6)
    ))

    yearly_cases_fig.add_trace(go.Scatter(
        x=yearly_cases['year_month'],
        y=yearly_cases['sepsis_wo_lactate'],
        mode='lines+markers',
        name='ASE without Lactate',
        line=dict(color='rgba(30, 144, 255, 1)', width=2),
        marker=dict(size=6)
    ))

    yearly_cases_fig.update_layout(
        title_text="Monthly ASE Cases: With vs Without Lactate",
        xaxis_title="Year-Month",
        yaxis_title="Number of Cases",
        height=500,
        width=1100,
        legend=dict(x=0.7, y=0.95),
        xaxis=dict(tickangle=-45)
    )

    # Add percentage columns (row-wise: each row sums to 100%)
    _row_total = yearly_cases['sepsis'] + yearly_cases['sepsis_wo_lactate']
    for _col in ['sepsis', 'sepsis_wo_lactate']:
        yearly_cases[f'{_col}_pct'] = (yearly_cases[_col] / _row_total.replace(0, float('nan')) * 100).round(1).fillna(0.0)

    # Add totals row
    totals = {'year_month': 'Total'}
    for _col in ['sepsis', 'sepsis_wo_lactate']:
        totals[_col] = yearly_cases[_col].sum()
    _total_sum = totals['sepsis'] + totals['sepsis_wo_lactate']
    for _col in ['sepsis', 'sepsis_wo_lactate']:
        totals[f'{_col}_pct'] = round(totals[_col] / _total_sum * 100, 1) if _total_sum > 0 else 0.0
    yearly_cases = pd.concat([yearly_cases, pd.DataFrame([totals])], ignore_index=True)

    # Print summary
    print("Monthly ASE Cases Summary:")
    print(yearly_cases.to_string(index=False))

    yearly_cases_fig
    return yearly_cases, yearly_cases_fig


@app.cell
def _(mo):
    mo.md(r"""
    ## Monthly Organ Dysfunctions

    Shows the monthly trend of each organ dysfunction type among ASE patients,
    split into with-lactate and without-lactate definitions.
    """)
    return


@app.cell
def _(ORGAN_COLS_WITHOUT_LACTATE, ORGAN_COLS_WITH_LACTATE, ase_df, go, pd):
    organ_colors = {
        'Vasopressor': 'rgba(255, 99, 71, 1)',
        'IMV': 'rgba(30, 144, 255, 1)',
        'AKI': 'rgba(255, 165, 0, 1)',
        'Hyperbilirubinemia': 'rgba(255, 215, 0, 1)',
        'Thrombocytopenia': 'rgba(147, 112, 219, 1)',
        'Lactate': 'rgba(60, 179, 113, 1)'
    }

    def _build_organ_fig(df, organ_cols, title):
        df = df.copy()
        df['blood_culture_dttm'] = pd.to_datetime(df['blood_culture_dttm'])
        df['year'] = df['blood_culture_dttm'].dt.year
        df = df[(df['year'] >= 2018) & (df['year'] <= 2024)]
        df['year_month'] = df['blood_culture_dttm'].dt.to_period('M')

        organ_data = []
        for ocol, olabel in organ_cols.items():
            monthly_counts = df.groupby('year_month')[ocol].apply(
                lambda x: x.notna().sum()
            ).reset_index()
            monthly_counts.columns = ['year_month', 'count']
            monthly_counts['organ'] = olabel
            organ_data.append(monthly_counts)

        organ_df = pd.concat(organ_data, ignore_index=True)

        fig = go.Figure()
        for organ_col, organ_label in organ_cols.items():
            odata = organ_df[organ_df['organ'] == organ_label]
            fig.add_trace(go.Scatter(
                x=odata['year_month'].astype(str),
                y=odata['count'],
                mode='lines+markers',
                name=organ_label,
                line=dict(color=organ_colors.get(organ_label, 'gray'), width=2),
                marker=dict(size=6)
            ))
        fig.update_layout(
            title_text=title,
            xaxis_title="Year-Month",
            yaxis_title="Number of Cases",
            height=500, width=1100,
            legend=dict(x=0.85, y=0.95),
            xaxis=dict(tickangle=-45)
        )

        # Pivot for export
        pivot = organ_df.pivot(index='year_month', columns='organ', values='count').fillna(0).reset_index()
        pivot['year_month'] = pivot['year_month'].astype(str)

        # Percentage columns (row-wise: each row sums to 100%)
        organ_labels = list(organ_cols.values())
        _present_cols = [c for c in organ_labels if c in pivot.columns]
        _row_total = pivot[_present_cols].sum(axis=1)
        for col in organ_labels:
            if col in pivot.columns:
                pivot[f'{col}_pct'] = (pivot[col] / _row_total.replace(0, float('nan')) * 100).round(1).fillna(0.0)

        # Totals row
        totals = {'year_month': 'Total'}
        for col in organ_labels:
            if col in pivot.columns:
                totals[col] = pivot[col].sum()
        _total_sum = sum(totals.get(col, 0) for col in _present_cols)
        for col in organ_labels:
            if col in pivot.columns:
                totals[f'{col}_pct'] = round(totals[col] / _total_sum * 100, 1) if _total_sum > 0 else 0.0
        pivot = pd.concat([pivot, pd.DataFrame([totals])], ignore_index=True)

        return fig, pivot

    # WITH lactate
    organ_monthly_w_fig, organ_monthly_w_pivot = _build_organ_fig(
        ase_df[ase_df['sepsis'] == 1],
        ORGAN_COLS_WITH_LACTATE,
        "Monthly Organ Dysfunctions — WITH Lactate"
    )

    # WITHOUT lactate
    organ_monthly_wo_fig, organ_monthly_wo_pivot = _build_organ_fig(
        ase_df[ase_df['sepsis_wo_lactate'] == 1],
        ORGAN_COLS_WITHOUT_LACTATE,
        "Monthly Organ Dysfunctions — WITHOUT Lactate"
    )

    print("WITH Lactate organs:")
    print(organ_monthly_w_pivot.to_string(index=False))
    print("\nWITHOUT Lactate organs:")
    print(organ_monthly_wo_pivot.to_string(index=False))

    organ_monthly_w_fig
    return (
        organ_monthly_w_fig,
        organ_monthly_w_pivot,
        organ_monthly_wo_fig,
        organ_monthly_wo_pivot,
    )


@app.cell
def _(mo):
    mo.md(r"""
    ## Yearly ASE by Onset Type

    Shows the yearly trend of ASE cases by onset type (Hospital vs Community),
    split into two plots: one for ASE **with** lactate and one **without** lactate.
    """)
    return


@app.cell
def _(ase_df, go, pd):
    def _build_onset_fig(df, title, dttm_col):
        """Build a community vs hospital onset line chart from filtered ASE df."""
        df = df.copy()
        df[dttm_col] = pd.to_datetime(df[dttm_col])
        df['year_month'] = df[dttm_col].dt.to_period('M')
        monthly = df.groupby(['year_month', 'type']).size().reset_index(name='count')
        pivot = monthly.pivot(index='year_month', columns='type', values='count').fillna(0)
        pivot.index = pivot.index.astype(str)

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=pivot.index, y=pivot.get('community', [0]*len(pivot)),
            mode='lines+markers', name='Community Onset',
            line=dict(color='rgba(30, 144, 255, 1)', width=2), marker=dict(size=8)
        ))
        fig.add_trace(go.Scatter(
            x=pivot.index, y=pivot.get('hospital', [0]*len(pivot)),
            mode='lines+markers', name='Hospital Onset',
            line=dict(color='rgba(255, 99, 71, 1)', width=2), marker=dict(size=8)
        ))
        fig.update_layout(
            title_text=title, xaxis_title="Year-Month", yaxis_title="Number of Cases",
            height=500, width=1100, legend=dict(x=0.7, y=0.95), xaxis=dict(tickangle=-45)
        )
        result = pivot.reset_index()

        # Add percentage columns (row-wise: each row sums to 100%)
        _onset_cols = [c for c in ['community', 'hospital'] if c in result.columns]
        _row_total = result[_onset_cols].sum(axis=1)
        for col in _onset_cols:
            result[f'{col}_pct'] = (result[col] / _row_total.replace(0, float('nan')) * 100).round(1).fillna(0.0)

        # Add totals row
        totals = {'year_month': 'Total'}
        for col in ['community', 'hospital']:
            if col in result.columns:
                totals[col] = result[col].sum()
        _total_sum = sum(totals.get(col, 0) for col in _onset_cols)
        for col in _onset_cols:
            totals[f'{col}_pct'] = round(totals[col] / _total_sum * 100, 1) if _total_sum > 0 else 0.0
        result = pd.concat([result, pd.DataFrame([totals])], ignore_index=True)

        return fig, result

    # ASE WITH lactate
    yearly_onset_w_fig, yearly_onset_w_data = _build_onset_fig(
        ase_df[ase_df['sepsis'] == 1],
        "Monthly ASE Cases by Onset Type — WITH Lactate",
        dttm_col="ase_onset_w_lactate_dttm"
    )
    # ASE WITHOUT lactate
    yearly_onset_wo_fig, yearly_onset_wo_data = _build_onset_fig(
        ase_df[ase_df['sepsis_wo_lactate'] == 1],
        "Monthly ASE Cases by Onset Type — WITHOUT Lactate",
        dttm_col="ase_onset_wo_lactate_dttm"
    )

    print("WITH Lactate onset:")
    print(yearly_onset_w_data.to_string(index=False))
    print("\nWITHOUT Lactate onset:")
    print(yearly_onset_wo_data.to_string(index=False))

    yearly_onset_w_fig
    return (
        yearly_onset_w_data,
        yearly_onset_w_fig,
        yearly_onset_wo_data,
        yearly_onset_wo_fig,
    )


@app.cell
def _(mo):
    mo.md(r"""
    ## Monthly Lactate Lab Counts by Hospital

    Lactate lab counts from the CLIF labs table, stratified by hospital ID and hospital type.
    """)
    return


@app.cell
def _(CLIF_DATA_DIR, FILETYPE, Labs, TIMEZONE, cohort_df, pd):
    # Load lactate labs for cohort hospitalizations
    lactate_labs = Labs.from_file(
        data_directory=str(CLIF_DATA_DIR),
        filetype=FILETYPE,
        timezone=TIMEZONE,
        filters={
            'hospitalization_id': cohort_df['hospitalization_id'].tolist(),
            'lab_category': ['lactate']
        },
        columns=['hospitalization_id', 'lab_result_dttm', 'lab_category']
    )
    lactate_df = lactate_labs.df.copy()

    # Join with cohort for hospital info
    lactate_df = lactate_df.merge(cohort_df, on='hospitalization_id', how='inner')

    # Derive year_month and filter to 2018-2024
    lactate_df['lab_result_dttm'] = pd.to_datetime(lactate_df['lab_result_dttm'])
    lactate_df['year'] = lactate_df['lab_result_dttm'].dt.year
    lactate_df = lactate_df[(lactate_df['year'] >= 2018) & (lactate_df['year'] <= 2024)]
    lactate_df['year_month'] = lactate_df['lab_result_dttm'].dt.to_period('M').astype(str)

    # Count lactate labs per year_month, hospital_id, hospital_type
    lactate_counts = (
        lactate_df
        .groupby(['year_month', 'hospital_id', 'hospital_type'])
        .size()
        .reset_index(name='lactate_count')
        .sort_values('year_month')
    )

    # Add totals row per hospital
    _totals = lactate_counts.groupby(['hospital_id', 'hospital_type'])['lactate_count'].sum().reset_index()
    _totals['year_month'] = 'Total'
    lactate_counts = pd.concat([lactate_counts, _totals], ignore_index=True)

    print("Lactate Lab Counts by Year-Month and Hospital:")
    print(lactate_counts.to_string(index=False))

    return lactate_counts, lactate_df


@app.cell
def _(mo):
    mo.md(r"""
    ## Lactate Orders During QADs by Health System

    Monthly trends in the number of lactate lab orders that fall within QAD
    (Qualifying Antimicrobial Day) windows, stratified by hospital/health system.
    This narrows the lactate counts to only those ordered during active antibiotic
    treatment windows for ASE episodes.
    """)
    return


@app.cell
def _(ase_df, cohort_df, go, lactate_df, pd):
    def _build_qad_lactate_trend(ase_subset, lactate_df_full, cohort, title):
        """Build monthly lactate-during-QAD trend by hospital for an ASE subset."""
        # Keep episodes with a QAD window
        qad_eps = ase_subset[ase_subset['qad_start_date'].notna()][
            ['hospitalization_id', 'qad_start_date', 'qad_end_date']
        ].copy()
        qad_eps['qad_start_date'] = pd.to_datetime(qad_eps['qad_start_date']).dt.date
        qad_eps['qad_end_date'] = pd.to_datetime(qad_eps['qad_end_date']).dt.date

        # Merge with cohort for hospital_id
        qad_eps = qad_eps.merge(
            cohort[['hospitalization_id', 'hospital_id']], on='hospitalization_id', how='inner'
        )

        # Inner join with lactate labs on hospitalization_id
        merged = qad_eps.merge(lactate_df_full[['hospitalization_id', 'lab_result_dttm']], on='hospitalization_id', how='inner')

        # Filter lactates to within the QAD window
        merged['lab_date'] = pd.to_datetime(merged['lab_result_dttm']).dt.date
        merged = merged[(merged['lab_date'] >= merged['qad_start_date']) & (merged['lab_date'] <= merged['qad_end_date'])]

        # Derive year_month, filter 2018-2024
        merged['lab_result_dttm'] = pd.to_datetime(merged['lab_result_dttm'])
        merged['year'] = merged['lab_result_dttm'].dt.year
        merged = merged[(merged['year'] >= 2018) & (merged['year'] <= 2024)]
        merged['year_month'] = merged['lab_result_dttm'].dt.to_period('M').astype(str)

        # Group by year_month + hospital_id
        counts = (
            merged.groupby(['year_month', 'hospital_id'])
            .size()
            .reset_index(name='lactate_during_qad_count')
            .sort_values('year_month')
        )

        # Build Plotly line chart — one trace per hospital
        fig = go.Figure()
        for hosp in sorted(counts['hospital_id'].unique()):
            hdata = counts[counts['hospital_id'] == hosp]
            fig.add_trace(go.Scatter(
                x=hdata['year_month'],
                y=hdata['lactate_during_qad_count'],
                mode='lines+markers',
                name=str(hosp),
                marker=dict(size=5),
            ))
        fig.update_layout(
            title_text=title,
            xaxis_title="Year-Month",
            yaxis_title="Lactate Orders During QADs",
            height=500, width=1100,
            legend=dict(x=0.85, y=0.95),
            xaxis=dict(tickangle=-45),
        )

        return fig, counts

    # ASE WITH lactate (sepsis == 1)
    qad_lactate_w_fig, qad_lactate_w_data = _build_qad_lactate_trend(
        ase_df[ase_df['sepsis'] == 1],
        lactate_df,
        cohort_df,
        "Monthly Lactate Orders During QADs — ASE WITH Lactate",
    )

    # ASE WITHOUT lactate (sepsis_wo_lactate == 1)
    qad_lactate_wo_fig, qad_lactate_wo_data = _build_qad_lactate_trend(
        ase_df[ase_df['sepsis_wo_lactate'] == 1],
        lactate_df,
        cohort_df,
        "Monthly Lactate Orders During QADs — ASE WITHOUT Lactate",
    )

    print("QAD Lactate Trend — WITH Lactate:")
    print(qad_lactate_w_data.to_string(index=False))
    print(f"\nQAD Lactate Trend — WITHOUT Lactate:")
    print(qad_lactate_wo_data.to_string(index=False))

    qad_lactate_w_fig
    return qad_lactate_w_data, qad_lactate_w_fig, qad_lactate_wo_data, qad_lactate_wo_fig


@app.cell
def _(mo):
    mo.md(r"""
    ## Save Outputs
    """)
    return


@app.cell
def _(
    OUTPUT_DIR,
    SITE_NAME,
    lactate_counts,
    organ_monthly_w_fig,
    organ_monthly_w_pivot,
    organ_monthly_wo_fig,
    organ_monthly_wo_pivot,
    plt,
    qad_data_df,
    qad_distribution,
    qad_lactate_w_data,
    qad_lactate_w_fig,
    qad_lactate_wo_data,
    qad_lactate_wo_fig,
    sankey_w_lactate,
    sankey_w_lactate_data,
    sankey_wo_lactate,
    sankey_wo_lactate_data,
    top20_meds_w_lactate,
    top20_meds_wo_lactate,
    yearly_cases,
    yearly_cases_fig,
    yearly_onset_w_data,
    yearly_onset_w_fig,
    yearly_onset_wo_data,
    yearly_onset_wo_fig,
    monthly_hosp_counts,
    monthly_ed_summary,
):
    # Create subdirectories for plots and data
    PLOTS_DIR = OUTPUT_DIR / "plots"
    DATA_DIR = OUTPUT_DIR / "data"
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Save Sankey plots as interactive HTML
    sankey_w_lactate_path = PLOTS_DIR / "sankey_ase_w_lactate.html"
    sankey_w_lactate.write_html(str(sankey_w_lactate_path))
    print(f"Saved: {sankey_w_lactate_path}")

    sankey_wo_lactate_path = PLOTS_DIR / "sankey_ase_wo_lactate.html"
    sankey_wo_lactate.write_html(str(sankey_wo_lactate_path))
    print(f"Saved: {sankey_wo_lactate_path}")

    # Save QAD distribution plot
    qad_html_path = PLOTS_DIR / "qad_distribution.html"
    qad_distribution.write_html(str(qad_html_path))
    print(f"Saved: {qad_html_path}")

    qad_png_path = PLOTS_DIR / "qad_distribution.png"
    fig, ax = plt.subplots(figsize=(9, 5))
    x_labels = qad_data_df["qad_days"].astype(str)
    x = range(len(x_labels))
    w = 0.35
    ax.bar([i - w / 2 for i in x], qad_data_df["ase_with_lactate_count"], w,
           label="ASE with Lactate", color=(60/255, 179/255, 113/255, 0.8))
    ax.bar([i + w / 2 for i in x], qad_data_df["ase_without_lactate_count"], w,
           label="ASE without Lactate", color=(30/255, 144/255, 255/255, 0.8))
    ax.set_xticks(list(x))
    ax.set_xticklabels(x_labels)
    ax.set_xlabel("QAD Days")
    ax.set_ylabel("Count")
    ax.set_title("QAD Days Distribution: ASE with vs without Lactate")
    ax.legend(loc="upper right")
    fig.savefig(str(qad_png_path), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {qad_png_path}")

    # Save yearly cases plot
    yearly_cases_html = PLOTS_DIR / "yearly_cases.html"
    yearly_cases_fig.write_html(str(yearly_cases_html))
    print(f"Saved: {yearly_cases_html}")

    yearly_cases_png = PLOTS_DIR / "yearly_cases.png"
    yc = yearly_cases[yearly_cases["year_month"] != "Total"]
    fig, ax = plt.subplots(figsize=(11, 5))
    ax.plot(yc["year_month"], yc["sepsis"], marker="o", markersize=4,
            label="ASE with Lactate", color=(60/255, 179/255, 113/255))
    ax.plot(yc["year_month"], yc["sepsis_wo_lactate"], marker="o", markersize=4,
            label="ASE without Lactate", color=(30/255, 144/255, 255/255))
    ax.set_xlabel("Year-Month")
    ax.set_ylabel("Number of Cases")
    ax.set_title("Monthly ASE Cases: With vs Without Lactate")
    ax.legend(loc="upper right")
    ax.tick_params(axis="x", rotation=45)
    nth = max(1, len(yc) // 12)
    ax.set_xticks(yc["year_month"].values[::nth])
    fig.savefig(str(yearly_cases_png), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {yearly_cases_png}")

    # Save monthly organs WITH lactate
    organ_w_html = PLOTS_DIR / "monthly_organs_w_lactate.html"
    organ_monthly_w_fig.write_html(str(organ_w_html))
    print(f"Saved: {organ_w_html}")

    organ_w_png = PLOTS_DIR / "monthly_organs_w_lactate.png"
    _organ_colors = {
        "Vasopressor": (255/255, 99/255, 71/255),
        "IMV": (30/255, 144/255, 255/255),
        "AKI": (255/255, 165/255, 0/255),
        "Hyperbilirubinemia": (255/255, 215/255, 0/255),
        "Thrombocytopenia": (147/255, 112/255, 219/255),
        "Lactate": (60/255, 179/255, 113/255),
    }
    ow = organ_monthly_w_pivot[organ_monthly_w_pivot["year_month"] != "Total"]
    fig, ax = plt.subplots(figsize=(11, 5))
    for _col in [c for c in ow.columns if c != "year_month" and "_pct" not in c]:
        ax.plot(ow["year_month"], ow[_col], marker="o", markersize=4,
                label=_col, color=_organ_colors.get(_col, "gray"))
    ax.set_xlabel("Year-Month")
    ax.set_ylabel("Number of Cases")
    ax.set_title("Monthly Organ Dysfunctions — WITH Lactate")
    ax.legend(loc="upper right", fontsize="small")
    ax.tick_params(axis="x", rotation=45)
    nth = max(1, len(ow) // 12)
    ax.set_xticks(ow["year_month"].values[::nth])
    fig.savefig(str(organ_w_png), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {organ_w_png}")

    # Save monthly organs WITHOUT lactate
    organ_wo_html = PLOTS_DIR / "monthly_organs_wo_lactate.html"
    organ_monthly_wo_fig.write_html(str(organ_wo_html))
    print(f"Saved: {organ_wo_html}")

    organ_wo_png = PLOTS_DIR / "monthly_organs_wo_lactate.png"
    owo = organ_monthly_wo_pivot[organ_monthly_wo_pivot["year_month"] != "Total"]
    fig, ax = plt.subplots(figsize=(11, 5))
    for _col in [c for c in owo.columns if c != "year_month" and "_pct" not in c]:
        ax.plot(owo["year_month"], owo[_col], marker="o", markersize=4,
                label=_col, color=_organ_colors.get(_col, "gray"))
    ax.set_xlabel("Year-Month")
    ax.set_ylabel("Number of Cases")
    ax.set_title("Monthly Organ Dysfunctions — WITHOUT Lactate")
    ax.legend(loc="upper right", fontsize="small")
    ax.tick_params(axis="x", rotation=45)
    nth = max(1, len(owo) // 12)
    ax.set_xticks(owo["year_month"].values[::nth])
    fig.savefig(str(organ_wo_png), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {organ_wo_png}")

    # Save monthly onset WITH lactate
    yearly_onset_w_html = PLOTS_DIR / "monthly_onset_w_lactate.html"
    yearly_onset_w_fig.write_html(str(yearly_onset_w_html))
    print(f"Saved: {yearly_onset_w_html}")

    yearly_onset_w_png = PLOTS_DIR / "monthly_onset_w_lactate.png"
    yw = yearly_onset_w_data[yearly_onset_w_data["year_month"] != "Total"]
    fig, ax = plt.subplots(figsize=(11, 5))
    if "community" in yw.columns:
        ax.plot(yw["year_month"], yw["community"], marker="o", markersize=6,
                label="Community Onset", color=(30/255, 144/255, 255/255))
    if "hospital" in yw.columns:
        ax.plot(yw["year_month"], yw["hospital"], marker="o", markersize=6,
                label="Hospital Onset", color=(255/255, 99/255, 71/255))
    ax.set_xlabel("Year-Month")
    ax.set_ylabel("Number of Cases")
    ax.set_title("Monthly ASE Cases by Onset Type — WITH Lactate")
    ax.legend(loc="upper right")
    ax.tick_params(axis="x", rotation=45)
    nth = max(1, len(yw) // 12)
    ax.set_xticks(yw["year_month"].values[::nth])
    fig.savefig(str(yearly_onset_w_png), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {yearly_onset_w_png}")

    # Save monthly onset WITHOUT lactate
    yearly_onset_wo_html = PLOTS_DIR / "monthly_onset_wo_lactate.html"
    yearly_onset_wo_fig.write_html(str(yearly_onset_wo_html))
    print(f"Saved: {yearly_onset_wo_html}")

    yearly_onset_wo_png = PLOTS_DIR / "monthly_onset_wo_lactate.png"
    ywo = yearly_onset_wo_data[yearly_onset_wo_data["year_month"] != "Total"]
    fig, ax = plt.subplots(figsize=(11, 5))
    if "community" in ywo.columns:
        ax.plot(ywo["year_month"], ywo["community"], marker="o", markersize=6,
                label="Community Onset", color=(30/255, 144/255, 255/255))
    if "hospital" in ywo.columns:
        ax.plot(ywo["year_month"], ywo["hospital"], marker="o", markersize=6,
                label="Hospital Onset", color=(255/255, 99/255, 71/255))
    ax.set_xlabel("Year-Month")
    ax.set_ylabel("Number of Cases")
    ax.set_title("Monthly ASE Cases by Onset Type — WITHOUT Lactate")
    ax.legend(loc="upper right")
    ax.tick_params(axis="x", rotation=45)
    nth = max(1, len(ywo) // 12)
    ax.set_xticks(ywo["year_month"].values[::nth])
    fig.savefig(str(yearly_onset_wo_png), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {yearly_onset_wo_png}")

    print("\n--- Plots saved ---")

    # Save CSV data files (aggregated transition counts, no PHI)
    sankey_w_lactate_csv = DATA_DIR / "sankey_ase_w_lactate_data.csv"
    sankey_w_lactate_data.to_csv(str(sankey_w_lactate_csv), index=False)
    print(f"Saved: {sankey_w_lactate_csv}")

    sankey_wo_lactate_csv = DATA_DIR / "sankey_ase_wo_lactate_data.csv"
    sankey_wo_lactate_data.to_csv(str(sankey_wo_lactate_csv), index=False)
    print(f"Saved: {sankey_wo_lactate_csv}")

    qad_csv_path = DATA_DIR / "qad_distribution_data.csv"
    qad_data_df.to_csv(str(qad_csv_path), index=False)
    print(f"Saved: {qad_csv_path}")

    yearly_cases_csv = DATA_DIR / "yearly_cases_data.csv"
    yearly_cases.to_csv(str(yearly_cases_csv), index=False)
    print(f"Saved: {yearly_cases_csv}")

    organ_w_csv = DATA_DIR / "monthly_organs_w_lactate_data.csv"
    organ_monthly_w_pivot.to_csv(str(organ_w_csv), index=False)
    print(f"Saved: {organ_w_csv}")

    organ_wo_csv = DATA_DIR / "monthly_organs_wo_lactate_data.csv"
    organ_monthly_wo_pivot.to_csv(str(organ_wo_csv), index=False)
    print(f"Saved: {organ_wo_csv}")

    yearly_onset_w_csv = DATA_DIR / "monthly_onset_w_lactate_data.csv"
    yearly_onset_w_data.to_csv(str(yearly_onset_w_csv), index=False)
    print(f"Saved: {yearly_onset_w_csv}")

    yearly_onset_wo_csv = DATA_DIR / "monthly_onset_wo_lactate_data.csv"
    yearly_onset_wo_data.to_csv(str(yearly_onset_wo_csv), index=False)
    print(f"Saved: {yearly_onset_wo_csv}")

    lactate_csv = DATA_DIR / "lactate_counts_by_hospital.csv"
    lactate_counts.to_csv(str(lactate_csv), index=False)
    print(f"Saved: {lactate_csv}")

    # Save QAD lactate trend — WITH lactate
    qad_lac_w_html = PLOTS_DIR / "qad_lactate_trend_w_lactate.html"
    qad_lactate_w_fig.write_html(str(qad_lac_w_html))
    print(f"Saved: {qad_lac_w_html}")

    qad_lac_w_png = PLOTS_DIR / "qad_lactate_trend_w_lactate.png"
    _qlw = qad_lactate_w_data.copy()
    fig, ax = plt.subplots(figsize=(11, 5))
    for hosp in sorted(_qlw['hospital_id'].unique()):
        hd = _qlw[_qlw['hospital_id'] == hosp]
        ax.plot(hd['year_month'], hd['lactate_during_qad_count'], marker='o', markersize=4, label=str(hosp))
    ax.set_xlabel("Year-Month")
    ax.set_ylabel("Lactate Orders During QADs")
    ax.set_title("Monthly Lactate Orders During QADs — ASE WITH Lactate")
    ax.legend(loc="upper right", fontsize="small")
    ax.tick_params(axis="x", rotation=45)
    nth = max(1, len(_qlw['year_month'].unique()) // 12)
    ax.set_xticks(_qlw['year_month'].unique()[::nth])
    fig.savefig(str(qad_lac_w_png), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {qad_lac_w_png}")

    qad_lac_w_csv = DATA_DIR / "qad_lactate_trend_w_lactate_data.csv"
    qad_lactate_w_data.to_csv(str(qad_lac_w_csv), index=False)
    print(f"Saved: {qad_lac_w_csv}")

    # Save QAD lactate trend — WITHOUT lactate
    qad_lac_wo_html = PLOTS_DIR / "qad_lactate_trend_wo_lactate.html"
    qad_lactate_wo_fig.write_html(str(qad_lac_wo_html))
    print(f"Saved: {qad_lac_wo_html}")

    qad_lac_wo_png = PLOTS_DIR / "qad_lactate_trend_wo_lactate.png"
    _qlwo = qad_lactate_wo_data.copy()
    fig, ax = plt.subplots(figsize=(11, 5))
    for hosp in sorted(_qlwo['hospital_id'].unique()):
        hd = _qlwo[_qlwo['hospital_id'] == hosp]
        ax.plot(hd['year_month'], hd['lactate_during_qad_count'], marker='o', markersize=4, label=str(hosp))
    ax.set_xlabel("Year-Month")
    ax.set_ylabel("Lactate Orders During QADs")
    ax.set_title("Monthly Lactate Orders During QADs — ASE WITHOUT Lactate")
    ax.legend(loc="upper right", fontsize="small")
    ax.tick_params(axis="x", rotation=45)
    nth = max(1, len(_qlwo['year_month'].unique()) // 12)
    ax.set_xticks(_qlwo['year_month'].unique()[::nth])
    fig.savefig(str(qad_lac_wo_png), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {qad_lac_wo_png}")

    qad_lac_wo_csv = DATA_DIR / "qad_lactate_trend_wo_lactate_data.csv"
    qad_lactate_wo_data.to_csv(str(qad_lac_wo_csv), index=False)
    print(f"Saved: {qad_lac_wo_csv}")

    monthly_ed_hosp_csv = DATA_DIR / "monthly_ed_hospitalizations.csv"
    monthly_hosp_counts.to_csv(str(monthly_ed_hosp_csv), index=False)
    print(f"Saved: {monthly_ed_hosp_csv}")

    monthly_ed_summary_csv = DATA_DIR / "monthly_ed_summary_stats.csv"
    monthly_ed_summary.to_csv(str(monthly_ed_summary_csv), index=False)
    print(f"Saved: {monthly_ed_summary_csv}")

    top20_meds_w_csv = DATA_DIR / "top20_qad_meds_w_lactate.csv"
    top20_meds_w_lactate.to_csv(str(top20_meds_w_csv), index=False)
    print(f"Saved: {top20_meds_w_csv}")

    top20_meds_wo_csv = DATA_DIR / "top20_qad_meds_wo_lactate.csv"
    top20_meds_wo_lactate.to_csv(str(top20_meds_wo_csv), index=False)
    print(f"Saved: {top20_meds_wo_csv}")

    print("\n--- Data CSVs saved ---")
    print("\nAll outputs saved successfully!")
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Summary Statistics
    """)
    return


@app.cell
def _(
    ORGAN_COLS_WITHOUT_LACTATE,
    ORGAN_COLS_WITH_LACTATE,
    ase_w_lactate_df,
    ase_wo_lactate_df,
):
    # Summary of organ dysfunction counts
    print("=" * 60)
    print("Organ Dysfunction Summary")
    print("=" * 60)

    print("\nASE WITH Lactate:")
    print(f"  Total patients: {len(ase_w_lactate_df):,}")
    for _col, label in ORGAN_COLS_WITH_LACTATE.items():
        count = ase_w_lactate_df[_col].notna().sum()
        pct = 100 * count / len(ase_w_lactate_df) if len(ase_w_lactate_df) > 0 else 0
        print(f"  {label}: {count:,} ({pct:.1f}%)")

    print("\nASE WITHOUT Lactate:")
    print(f"  Total patients: {len(ase_wo_lactate_df):,}")
    for _col, label in ORGAN_COLS_WITHOUT_LACTATE.items():
        count = ase_wo_lactate_df[_col].notna().sum()
        pct = 100 * count / len(ase_wo_lactate_df) if len(ase_wo_lactate_df) > 0 else 0
        print(f"  {label}: {count:,} ({pct:.1f}%)")

    # Count of organ dysfunctions per patient
    print("\nNumber of organ dysfunctions per patient (WITH lactate):")
    n_organs_w = ase_w_lactate_df[[c for c in ORGAN_COLS_WITH_LACTATE.keys()]].notna().sum(axis=1)
    print(n_organs_w.value_counts().sort_index().to_string())

    print("\nNumber of organ dysfunctions per patient (WITHOUT lactate):")
    n_organs_wo = ase_wo_lactate_df[[c for c in ORGAN_COLS_WITHOUT_LACTATE.keys()]].notna().sum(axis=1)
    print(n_organs_wo.value_counts().sort_index().to_string())
    return


if __name__ == "__main__":
    app.run()
