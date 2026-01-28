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
    """)
    return


@app.cell
def _():
    import json
    import pandas as pd
    from pathlib import Path
    import plotly.graph_objects as go
    return Path, go, json, pd


@app.cell
def _(Path, json):
    # Load configuration
    config_path = Path("clif_config.json")
    config = json.loads(config_path.read_text())

    OUTPUT_DIR = Path(config["output_directory"])
    SITE_NAME = config["site_name"]

    print(f"Site: {SITE_NAME}")
    print(f"Output directory: {OUTPUT_DIR}")
    return OUTPUT_DIR, SITE_NAME


@app.cell
def _(OUTPUT_DIR, SITE_NAME, pd):
    # Load ASE results from cohort_df.py output
    ase_results_path = OUTPUT_DIR / f"{SITE_NAME}_ase_results.parquet"
    ase_df = pd.read_parquet(ase_results_path)

    print(f"Loaded ASE results: {len(ase_df):,} records")
    print(f"ASE with lactate (sepsis=1): {ase_df['sepsis'].sum():,}")
    print(f"ASE without lactate (sepsis_wo_lactate=1): {ase_df['sepsis_wo_lactate'].sum():,}")
    return (ase_df,)


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
    for level in range(1, 6):  # 5 transitions for 6 levels
        counts = sequence_w_lactate.groupby(
            [f'level_{level}', f'level_{level+1}']
        ).size().reset_index(name='count')
        counts.columns = ['source_organ', 'target_organ', 'count']
        counts['source_level'] = level
        counts['target_level'] = level + 1
        transitions_w_lactate.append(counts)
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
    for level in range(1, 5):  # 4 transitions for 5 levels
        counts = sequence_wo_lactate.groupby(
            [f'level_{level}', f'level_{level+1}']
        ).size().reset_index(name='count')
        counts.columns = ['source_organ', 'target_organ', 'count']
        counts['source_level'] = level
        counts['target_level'] = level + 1
        transitions_wo_lactate.append(counts)
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
def _(go, pd):
    def create_qad_distribution(ase_df_with_lactate, ase_df_without_lactate, title="QAD Days Distribution"):
        """
        Create side-by-side bar chart comparing QAD distribution between ASE groups.
        """
        # Bin the QAD values
        qad_with = ase_df_with_lactate['total_qad'].dropna()
        qad_without = ase_df_without_lactate['total_qad'].dropna()

        # Create bins (0-4, 5-7, 8-10, 11-14, 15+)
        bins = [0, 4, 7, 10, 14, float('inf')]
        labels = ['0-4', '5-7', '8-10', '11-14', '15+']

        with_counts = pd.cut(qad_with, bins=bins, labels=labels, right=True).value_counts().sort_index()
        without_counts = pd.cut(qad_without, bins=bins, labels=labels, right=True).value_counts().sort_index()

        fig = go.Figure()

        # ASE with lactate
        fig.add_trace(go.Bar(
            x=labels,
            y=[with_counts.get(l, 0) for l in labels],
            name='ASE with Lactate',
            marker_color='rgba(60, 179, 113, 0.8)'  # Medium sea green
        ))

        # ASE without lactate
        fig.add_trace(go.Bar(
            x=labels,
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

    # Create QAD data DataFrame for export
    bins = [0, 4, 7, 10, 14, float('inf')]
    labels = ['0-4', '5-7', '8-10', '11-14', '15+']
    qad_with = ase_w_lactate_qad['total_qad'].dropna()
    qad_without = ase_wo_lactate_qad['total_qad'].dropna()
    with_counts = pd.cut(qad_with, bins=bins, labels=labels, right=True).value_counts().sort_index()
    without_counts = pd.cut(qad_without, bins=bins, labels=labels, right=True).value_counts().sort_index()
    qad_data_df = pd.DataFrame({
        'qad_bin': labels,
        'ase_with_lactate_count': [with_counts.get(l, 0) for l in labels],
        'ase_without_lactate_count': [without_counts.get(l, 0) for l in labels]
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
    return qad_distribution, qad_data_df


@app.cell
def _(mo):
    mo.md(r"""
    ## Yearly ASE Cases

    Shows the yearly trend of ASE cases comparing with lactate vs without lactate criteria.
    """)
    return


@app.cell
def _(ase_df, go, pd):
    # Extract year from blood culture date
    ase_yearly = ase_df.copy()
    ase_yearly['year'] = pd.to_datetime(ase_yearly['blood_culture_dttm']).dt.year

    # Filter to 2018-2024 only
    ase_yearly = ase_yearly[(ase_yearly['year'] >= 2018) & (ase_yearly['year'] <= 2024)]

    # Count cases per year
    yearly_cases = ase_yearly.groupby('year').agg({
        'sepsis': 'sum',
        'sepsis_wo_lactate': 'sum'
    }).reset_index()

    # Create line chart
    yearly_cases_fig = go.Figure()

    yearly_cases_fig.add_trace(go.Scatter(
        x=yearly_cases['year'],
        y=yearly_cases['sepsis'],
        mode='lines+markers',
        name='ASE with Lactate',
        line=dict(color='rgba(60, 179, 113, 1)', width=2),
        marker=dict(size=8)
    ))

    yearly_cases_fig.add_trace(go.Scatter(
        x=yearly_cases['year'],
        y=yearly_cases['sepsis_wo_lactate'],
        mode='lines+markers',
        name='ASE without Lactate',
        line=dict(color='rgba(30, 144, 255, 1)', width=2),
        marker=dict(size=8)
    ))

    yearly_cases_fig.update_layout(
        title_text="Yearly ASE Cases: With vs Without Lactate",
        xaxis_title="Year",
        yaxis_title="Number of Cases",
        height=500,
        width=900,
        legend=dict(x=0.7, y=0.95),
        xaxis=dict(dtick=1)
    )

    # Print summary
    print("Yearly ASE Cases Summary:")
    print(yearly_cases.to_string(index=False))

    yearly_cases_fig
    return yearly_cases_fig, yearly_cases


@app.cell
def _(mo):
    mo.md(r"""
    ## Yearly Organ Dysfunctions

    Shows the yearly trend of each organ dysfunction type among ASE patients.
    """)
    return


@app.cell
def _(ORGAN_COLS_WITH_LACTATE, ase_df, go, pd):
    # Extract year from blood culture date
    ase_organs = ase_df.copy()
    ase_organs['year'] = pd.to_datetime(ase_organs['blood_culture_dttm']).dt.year

    # Filter to 2018-2024 only
    ase_organs = ase_organs[(ase_organs['year'] >= 2018) & (ase_organs['year'] <= 2024)]

    # Color map for organs (consistent with Sankey)
    organ_colors = {
        'Vasopressor': 'rgba(255, 99, 71, 1)',      # Tomato red
        'IMV': 'rgba(30, 144, 255, 1)',              # Dodger blue
        'AKI': 'rgba(255, 165, 0, 1)',               # Orange
        'Hyperbilirubinemia': 'rgba(255, 215, 0, 1)', # Gold
        'Thrombocytopenia': 'rgba(147, 112, 219, 1)', # Medium purple
        'Lactate': 'rgba(60, 179, 113, 1)'           # Medium sea green
    }

    # Count each organ dysfunction per year
    organ_yearly_data = []
    for ocol, olabel in ORGAN_COLS_WITH_LACTATE.items():
        yearly_counts = ase_organs.groupby('year')[ocol].apply(
            lambda x: x.notna().sum()
        ).reset_index()
        yearly_counts.columns = ['year', 'count']
        yearly_counts['organ'] = olabel
        organ_yearly_data.append(yearly_counts)

    organ_yearly_df = pd.concat(organ_yearly_data, ignore_index=True)

    # Create multi-line chart
    yearly_organs_fig = go.Figure()

    for organ_col, organ_label in ORGAN_COLS_WITH_LACTATE.items():
        organ_data = organ_yearly_df[organ_yearly_df['organ'] == organ_label]
        yearly_organs_fig.add_trace(go.Scatter(
            x=organ_data['year'],
            y=organ_data['count'],
            mode='lines+markers',
            name=organ_label,
            line=dict(color=organ_colors.get(organ_label, 'gray'), width=2),
            marker=dict(size=6)
        ))

    yearly_organs_fig.update_layout(
        title_text="Yearly Organ Dysfunctions in ASE Patients",
        xaxis_title="Year",
        yaxis_title="Number of Cases",
        height=500,
        width=900,
        legend=dict(x=0.85, y=0.95),
        xaxis=dict(dtick=1)
    )

    # Create pivot table for export
    organ_yearly_pivot = organ_yearly_df.pivot(index='year', columns='organ', values='count').reset_index()
    print("Yearly Organ Dysfunctions Summary:")
    print(organ_yearly_pivot.to_string(index=False))

    yearly_organs_fig
    return yearly_organs_fig, organ_yearly_pivot


@app.cell
def _(mo):
    mo.md(r"""
    ## Yearly ASE by Onset Type

    Shows the yearly trend of ASE cases by onset type (Hospital vs Community).
    """)
    return


@app.cell
def _(ase_df, go, pd):
    # Filter to ASE cases only and extract year
    ase_onset = ase_df[ase_df['sepsis'] == 1].copy()
    ase_onset['year'] = pd.to_datetime(ase_onset['blood_culture_dttm']).dt.year

    # Filter to 2018-2024 only
    ase_onset = ase_onset[(ase_onset['year'] >= 2018) & (ase_onset['year'] <= 2024)]

    # Count by onset type per year
    yearly_onset = ase_onset.groupby(['year', 'type']).size().reset_index(name='count')
    yearly_onset_pivot = yearly_onset.pivot(index='year', columns='type', values='count').fillna(0)

    # Create line chart
    yearly_onset_fig = go.Figure()

    yearly_onset_fig.add_trace(go.Scatter(
        x=yearly_onset_pivot.index,
        y=yearly_onset_pivot.get('community', [0] * len(yearly_onset_pivot)),
        mode='lines+markers',
        name='Community Onset',
        line=dict(color='rgba(30, 144, 255, 1)', width=2),
        marker=dict(size=8)
    ))

    yearly_onset_fig.add_trace(go.Scatter(
        x=yearly_onset_pivot.index,
        y=yearly_onset_pivot.get('hospital', [0] * len(yearly_onset_pivot)),
        mode='lines+markers',
        name='Hospital Onset',
        line=dict(color='rgba(255, 99, 71, 1)', width=2),
        marker=dict(size=8)
    ))

    yearly_onset_fig.update_layout(
        title_text="Yearly ASE Cases: Community vs Hospital Onset",
        xaxis_title="Year",
        yaxis_title="Number of Cases",
        height=500,
        width=900,
        legend=dict(x=0.7, y=0.95),
        xaxis=dict(dtick=1)
    )

    # Print summary
    print("Yearly ASE by Onset Type Summary:")
    print(yearly_onset_pivot.to_string())

    # Reset index for export
    yearly_onset_data = yearly_onset_pivot.reset_index()

    yearly_onset_fig
    return yearly_onset_fig, yearly_onset_data


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
    organ_yearly_pivot,
    qad_data_df,
    qad_distribution,
    sankey_w_lactate,
    sankey_w_lactate_data,
    sankey_wo_lactate,
    sankey_wo_lactate_data,
    yearly_cases,
    yearly_cases_fig,
    yearly_onset_data,
    yearly_onset_fig,
    yearly_organs_fig,
):
    # Create subdirectories for plots and data
    PLOTS_DIR = OUTPUT_DIR / "plots"
    DATA_DIR = OUTPUT_DIR / "data"
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Save Sankey plots as interactive HTML
    sankey_w_lactate_path = PLOTS_DIR / f"{SITE_NAME}_sankey_ase_w_lactate.html"
    sankey_w_lactate.write_html(str(sankey_w_lactate_path))
    print(f"Saved: {sankey_w_lactate_path}")

    sankey_wo_lactate_path = PLOTS_DIR / f"{SITE_NAME}_sankey_ase_wo_lactate.html"
    sankey_wo_lactate.write_html(str(sankey_wo_lactate_path))
    print(f"Saved: {sankey_wo_lactate_path}")

    # Save Sankey plots as PNG (requires kaleido package)
    sankey_w_lactate_png = PLOTS_DIR / f"{SITE_NAME}_sankey_ase_w_lactate.png"
    sankey_w_lactate.write_image(str(sankey_w_lactate_png))
    print(f"Saved: {sankey_w_lactate_png}")

    sankey_wo_lactate_png = PLOTS_DIR / f"{SITE_NAME}_sankey_ase_wo_lactate.png"
    sankey_wo_lactate.write_image(str(sankey_wo_lactate_png))
    print(f"Saved: {sankey_wo_lactate_png}")

    # Save QAD distribution plot
    qad_html_path = PLOTS_DIR / f"{SITE_NAME}_qad_distribution.html"
    qad_distribution.write_html(str(qad_html_path))
    print(f"Saved: {qad_html_path}")

    qad_png_path = PLOTS_DIR / f"{SITE_NAME}_qad_distribution.png"
    qad_distribution.write_image(str(qad_png_path))
    print(f"Saved: {qad_png_path}")

    # Save yearly cases plot
    yearly_cases_html = PLOTS_DIR / f"{SITE_NAME}_yearly_cases.html"
    yearly_cases_fig.write_html(str(yearly_cases_html))
    print(f"Saved: {yearly_cases_html}")

    yearly_cases_png = PLOTS_DIR / f"{SITE_NAME}_yearly_cases.png"
    yearly_cases_fig.write_image(str(yearly_cases_png))
    print(f"Saved: {yearly_cases_png}")

    # Save yearly organs plot
    yearly_organs_html = PLOTS_DIR / f"{SITE_NAME}_yearly_organs.html"
    yearly_organs_fig.write_html(str(yearly_organs_html))
    print(f"Saved: {yearly_organs_html}")

    yearly_organs_png = PLOTS_DIR / f"{SITE_NAME}_yearly_organs.png"
    yearly_organs_fig.write_image(str(yearly_organs_png))
    print(f"Saved: {yearly_organs_png}")

    # Save yearly onset type plot
    yearly_onset_html = PLOTS_DIR / f"{SITE_NAME}_yearly_onset.html"
    yearly_onset_fig.write_html(str(yearly_onset_html))
    print(f"Saved: {yearly_onset_html}")

    yearly_onset_png = PLOTS_DIR / f"{SITE_NAME}_yearly_onset.png"
    yearly_onset_fig.write_image(str(yearly_onset_png))
    print(f"Saved: {yearly_onset_png}")

    print("\n--- Plots saved ---")

    # Save CSV data files (aggregated transition counts, no PHI)
    sankey_w_lactate_csv = DATA_DIR / f"{SITE_NAME}_sankey_ase_w_lactate_data.csv"
    sankey_w_lactate_data.to_csv(str(sankey_w_lactate_csv), index=False)
    print(f"Saved: {sankey_w_lactate_csv}")

    sankey_wo_lactate_csv = DATA_DIR / f"{SITE_NAME}_sankey_ase_wo_lactate_data.csv"
    sankey_wo_lactate_data.to_csv(str(sankey_wo_lactate_csv), index=False)
    print(f"Saved: {sankey_wo_lactate_csv}")

    qad_csv_path = DATA_DIR / f"{SITE_NAME}_qad_distribution_data.csv"
    qad_data_df.to_csv(str(qad_csv_path), index=False)
    print(f"Saved: {qad_csv_path}")

    yearly_cases_csv = DATA_DIR / f"{SITE_NAME}_yearly_cases_data.csv"
    yearly_cases.to_csv(str(yearly_cases_csv), index=False)
    print(f"Saved: {yearly_cases_csv}")

    yearly_organs_csv = DATA_DIR / f"{SITE_NAME}_yearly_organs_data.csv"
    organ_yearly_pivot.to_csv(str(yearly_organs_csv), index=False)
    print(f"Saved: {yearly_organs_csv}")

    yearly_onset_csv = DATA_DIR / f"{SITE_NAME}_yearly_onset_data.csv"
    yearly_onset_data.to_csv(str(yearly_onset_csv), index=False)
    print(f"Saved: {yearly_onset_csv}")

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
    for col, label in ORGAN_COLS_WITH_LACTATE.items():
        count = ase_w_lactate_df[col].notna().sum()
        pct = 100 * count / len(ase_w_lactate_df) if len(ase_w_lactate_df) > 0 else 0
        print(f"  {label}: {count:,} ({pct:.1f}%)")

    print("\nASE WITHOUT Lactate:")
    print(f"  Total patients: {len(ase_wo_lactate_df):,}")
    for col, label in ORGAN_COLS_WITHOUT_LACTATE.items():
        count = ase_wo_lactate_df[col].notna().sum()
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
