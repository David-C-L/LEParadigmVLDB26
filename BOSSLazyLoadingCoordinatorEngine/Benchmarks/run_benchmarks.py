import argparse
import csv
from collections import defaultdict
from pathlib import Path
import json
import matplotlib.pyplot as plt
import subprocess
from math import ceil, log

import numpy as np
import pandas as pd
import shutil
from matplotlib.ticker import MaxNLocator, FuncFormatter
import random
import matplotlib.colors as mcolors
import matplotlib.lines as mlines

# Improved aesthetics with seaborn
try:
    import seaborn as sns

    sns.set()  # Apply the default seaborn theme, scaling, and color palette
except ImportError:
    print("Seaborn not installed, using default matplotlib styles")


def lighten_color(color, amount=0.5):
    """
    Lightens the given color by mixing it with white.
    :param color: A matplotlib color string (e.g. 'blue') or RGB tuple.
    :param amount: The amount to lighten the color by (default is 0.5).
    :return: A lightened color in the same format.
    """
    try:
        c = mcolors.to_rgba(color)
    except ValueError:
        c = mcolors.to_rgba(color, alpha=None)
    white = np.array([1, 1, 1, 1])
    new_color = (1 - amount) * np.array(c) + amount * white
    return new_color


def run_executable(executable_path, flags, shell=False):
    """
    Runs an executable with specified flags.

    Parameters:
    - executable_path: Path to the executable file.
    - flags: List of flags (each flag is a string) to pass to the executable.
    """
    # Combine the executable path with the flags into a command
    command = [executable_path] + flags if not shell else f"{executable_path} " + " ".join(flags)

    try:
        # Run the command and capture the output
        result = subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                shell=shell)
        # Print the standard output and error (if any)
        print("Standard Output:", result.stdout)
        if result.stderr:
            print("Standard Error:", result.stderr)
    except subprocess.CalledProcessError as e:
        # Handle errors in the subprocess
        print(f"An error occurred while running the executable: {e}")


def create_tpch_queries_graph(json_path, csv_size_path, csv_time_path, plot_pdf):
    # Load JSON data
    with open(json_path, 'r') as file:
        data = json.load(file)

    # Extract benchmarks
    benchmarks = data['benchmarks']

    query_map = {
        0: 1,
        1: 3,
        2: 6,
        3: 9,
        4: 18
    }

    # Prepare data structure
    results = []
    for bench in benchmarks:
        parts = bench['name'].split('/')
        configuration = 'Load-Evaluate' if parts[1] == '0' else 'Data Vaults'
        query = int(parts[2])
        scale_factor = 10 ** (int(parts[3]) - 3) if int(parts[3]) != 5 else 20  # Scale factor as power of ten
        real_time = bench['real_time'] / 1000  # Convert to seconds

        results.append({
            'Configuration': configuration,
            'Query': query,
            'Scale Factor': scale_factor,
            'Real Time (s)': real_time  # Change label to seconds
        })

    # Convert to DataFrame
    df = pd.DataFrame(results)

    # Ensure consistent ordering for configuration
    config_order = ['Data Vaults', 'Load-Evaluate']  # DV first, then Load-Evaluate, ensures Load-Evaluate is on the right
    df['Configuration'] = pd.Categorical(df['Configuration'], categories=config_order, ordered=True)
    df.sort_values(['Query', 'Configuration'], inplace=True)

    # Load CSV data for data transfer
    csv_df = pd.read_csv(csv_size_path)

    # Mapping for System to Configuration
    system_map = {0: 'Load-Evaluate', 1: 'Data Vaults'}
    csv_df['Configuration'] = csv_df['System'].map(system_map)

    # Calculate total data transferred per row and convert to MB
    csv_df['TotalTransferred'] = (csv_df['Loaded'] + csv_df['Requested'] + csv_df['Headers']) / 1024 / 1024

    # Adjust iteration count for zero-based index
    csv_df['Iteration'] = csv_df['Iteration'] + 1

    # Compute the average transferred data for each (Scale, System, Query) group
    grouped = csv_df.groupby(['Scale', 'System', 'Query'])
    avg_data = grouped.apply(lambda x: x['TotalTransferred'].sum() / x['Iteration'].max()).reset_index(name='AvgTransferred')

    # Load CSV data for execution time
    csv_time_df = pd.read_csv(csv_time_path)

    # Calculate total transfer time as the sum of 'Pretransfer' and 'Download' columns and convert to seconds
    csv_time_df['TotalTransferTime'] = (csv_time_df['Pretransfer'] + csv_time_df['Download']) / 1000 / 1000

    # Adjust iteration count for zero-based index
    csv_time_df['Iteration'] = csv_time_df['Iteration'] + 1

    # Compute the average transfer time for each (Scale, System, Query) group
    time_grouped = csv_time_df.groupby(['Scale', 'System', 'Query'])
    avg_time = time_grouped.apply(lambda x: x['TotalTransferTime'].sum() / x['Iteration'].max()).reset_index(name='AvgTransferTime')

    sns.set(style="whitegrid")
    palette = sns.color_palette("husl", len(config_order))

    # Function to format ticks as full numbers
    def log_tick_formatter(val, pos=None):
        return f'{val:g}'

    # Calculate limits
    max_avg_transferred = avg_data['AvgTransferred'].max()
    max_execution_time = df['Real Time (s)'].max()
    max_transfer_time = avg_time['AvgTransferTime'].max()
    exec_transfer_ratio = max_execution_time / max_transfer_time

    # Calculate overall averages
    avg_execution_time = df['Real Time (s)'].mean()
    avg_data_transferred = avg_data['AvgTransferred'].mean()

    # Determine the normalization factor to align the lines
    normalization_factor = avg_execution_time / avg_data_transferred

    execution_base = 10
    execution_y_lim = execution_base ** (ceil(log(max_execution_time, execution_base)))

    transfer_base = 10
    transfer_y_lim = transfer_base ** ceil(log((max_avg_transferred), transfer_base))

    # Plotting
    fig, axes = plt.subplots(nrows=1, ncols=5, figsize=(16, 4), height_ratios=[0.75], sharey=True)
    queries = sorted(df['Query'].unique())

    handles, labels = None, None  # For capturing legend information
    for i, (ax, query) in enumerate(zip(axes.flatten(), queries)):
        if handles is None:  # Capture legend info from the first subplot
            handles, labels = ax.get_legend_handles_labels()
        subset = df[df['Query'] == query]
        pivot_table = subset.pivot_table(index='Scale Factor', columns='Configuration', values='Real Time (s)', aggfunc='mean')
        bars = pivot_table.plot(kind='bar', ax=ax, color=[palette[1], palette[0]], legend=False)

        # Data Transfer lines
        csv_subset = avg_data[avg_data['Query'] == query]

        ax.set_title(f'Q{query_map[query]}', fontsize=14)
        ax.set_xlabel('')
        ax.set_ylabel('Runtime (s)', fontsize=14)  # Change label to seconds
        ax.set_yscale('log', base=execution_base)
        ax.set_ylim(1, execution_y_lim)  # Set y-axis limits for execution time
        ax.yaxis.set_major_formatter(FuncFormatter(log_tick_formatter))
        ax.tick_params(labelsize=12)

        # Add secondary y-axis for data transfer
        ax2 = ax.twinx()
        if i == len(queries) - 1:  # Only add ylabel for the last subplot
            ax2.set_ylabel('Data Transferred (MB)', fontsize=14)
            ax2.tick_params(labelright=True, labelsize=12)  # Remove y-axis labels for other subplots
        else:
            ax2.tick_params(labelright=False)  # Remove y-axis labels for other subplots
        ax2.set_yscale('log', base=transfer_base)
        ax2.set_ylim(1, transfer_y_lim)  # Adjust right y-axis limits for normalization
        ax2.yaxis.set_major_formatter(FuncFormatter(log_tick_formatter))
        ax2.grid(False)  # Remove the grid for the right-hand axis

        for idx, row in csv_subset.iterrows():
            config = system_map[row['System']]
            scale_factor_idx = int(row['Scale'] - 3)  # Map scale factor to bar index and cast to int
            total_transfer = row['AvgTransferred']
            bar = bars.containers[0 if config == 'Data Vaults' else 1][scale_factor_idx]
            bar_x = bar.get_x() + bar.get_width() / 2
            bar_width = bar.get_width()
            line_color = 'b' if config == 'Data Vaults' else 'r'
            ax2.hlines(total_transfer, bar_x - bar_width / 2, bar_x + bar_width / 2, colors=line_color, linestyles='-', linewidth=2)

    fig.subplots_adjust(top=0.75, bottom=0.30)  # Adjust the top to make space for the legend
    fig.text(0.5, 0.015, 'Scale Factor', ha='center', va='center', fontsize=14)

    # Custom legends for execution time and data transfer
    time_legend_handles = [
        mlines.Line2D([], [], color=palette[1], marker='s', linestyle='None', markersize=10, label='Runtime (DV)'),
        mlines.Line2D([], [], color=palette[0], marker='s', linestyle='None', markersize=10, label='Runtime (LE)')
    ]

    transfer_legend_handles = [
        mlines.Line2D([], [], color='b', linestyle='-', linewidth=2, label='Data Transferred (DV)'),
        mlines.Line2D([], [], color='r', linestyle='-', linewidth=2, label='Data Transferred (LE)')
    ]

    # Place legends
    fig.legend(handles=time_legend_handles, loc='upper left', bbox_to_anchor=(0, 1), ncol=2, fontsize=12)
    fig.legend(handles=transfer_legend_handles, loc='upper right', bbox_to_anchor=(1, 1), ncol=2, fontsize=12)

    plt.tight_layout(rect=(0.0, 0.0, 1.0, 0.95))
    plt.savefig(plot_pdf)


def create_miniseed_queries_size_time_graph(json_path, transfer_csv_path, time_csv_path, plot_pdf):
    # Load JSON data for runtime
    with open(json_path, 'r') as file:
        runtime_data = json.load(file)

    # Load CSV data for total data transferred
    transfer_df = pd.read_csv(transfer_csv_path)

    # Calculate total data transferred per row and convert to MB
    transfer_df['TotalTransferred'] = (transfer_df['Loaded'] + transfer_df['Requested'] + transfer_df[
        'Headers']) / 1024 / 1024

    # Group by System and Query, then calculate the required average
    grouped_transfer = transfer_df.groupby(['System', 'Query'])
    avg_transfer_data = grouped_transfer.apply(lambda x: x['TotalTransferred'].sum() / (x['Iteration'].max() + 1))

    # Reset index to access multi-level group by data
    avg_transfer_data = avg_transfer_data.reset_index(name='AvgTransferred')

    # Load CSV data for transfer times
    time_df = pd.read_csv(time_csv_path)

    # Calculate total transfer time per row
    time_df['TotalTransferTime'] = (time_df['Pretransfer'] + time_df['Download']) / 1000 / 1000

    # Group by System and Query, then calculate the required average
    grouped_time = time_df.groupby(['System', 'Query'])
    avg_time_data = grouped_time.apply(lambda x: x['TotalTransferTime'].sum() / (x['Iteration'].max() + 1))

    # Reset index to access multi-level group by data
    avg_time_data = avg_time_data.reset_index(name='AvgTransferTime')

    # Prepare runtime data
    benchmarks_dict = defaultdict(list)
    for benchmark in runtime_data['benchmarks']:
        first_digit, second_digit = benchmark['name'].split('/')[1:3]
        benchmarks_dict[second_digit].append((first_digit, (benchmark['real_time'] / 1000)))

    # Sort benchmarks by second digit for plotting
    sorted_benchmark_keys = sorted(benchmarks_dict.keys())

    # Initialize seaborn for better aesthetics
    sns.set(style="whitegrid")

    # Prepare data for plotting
    names = []  # x-axis labels
    positions = []  # x-axis positions for bars
    mid_positions = []
    runtime_values = []  # y-axis values for runtime
    transfer_values = []  # y-axis values for data transfer
    colors = []  # Color code for bars
    color_map = {}  # Mapping from first digit to color
    unique_first_digits = sorted({b[0] for second_digit in benchmarks_dict for b in benchmarks_dict[second_digit]})
    palette = sns.color_palette("husl", len(unique_first_digits))
    for idx, digit in enumerate(unique_first_digits):
        color_map[digit] = palette[idx]

    # Setting up spacing parameters
    current_position = 0
    bar_width = 0.4  # Reduced bar width for better visibility of overlaying line
    space_between_pairs = 0.5  # Space between groups of bars
    space_within_pairs = 0.1  # Small gap between bars within the same pair

    for second_digit in sorted_benchmark_keys:
        inner_position = current_position
        group_positions = []
        for benchmark in reversed(sorted(benchmarks_dict[second_digit], key=lambda x: x[0])):
            positions.append(inner_position)
            runtime_values.append(benchmark[1])
            colors.append(color_map[benchmark[0]])
            group_positions.append(inner_position + 0.1 * 2)
            inner_position += bar_width + space_within_pairs
        mid_positions.append(sum(group_positions) / len(group_positions))  # Midpoint of the group for labels
        current_position += bar_width * 2 + space_between_pairs + space_within_pairs
        names.append(f"Q{second_digit}")

    # Extract transfer values for plotting
    max_runtime = max(runtime_values)
    max_transfer_time = avg_time_data['AvgTransferTime'].max()
    max_transfer_size = avg_transfer_data['AvgTransferred'].max()
    transfer_factor = max_runtime / max_transfer_time if max_transfer_time != 0 else 0

    for name in names:
        query = int(name[1:])
        dv_transfer = avg_transfer_data[(avg_transfer_data['Query'] == query) & (avg_transfer_data['System'] == 1)][
            'AvgTransferred'].values
        le_transfer = avg_transfer_data[(avg_transfer_data['Query'] == query) & (avg_transfer_data['System'] == 0)][
            'AvgTransferred'].values
        transfer_values.extend([(dv_transfer[0]) if dv_transfer.size > 0 else 0,
                                (le_transfer[0]) if le_transfer.size > 0 else 0])

    # Plotting each benchmark
    fig, ax1 = plt.subplots(figsize=(12, 6))
    bars = ax1.bar(positions, runtime_values, color=colors, width=bar_width)

    # Beautify the plot
    ax1.set_ylabel('Runtime (s)', fontsize=14)
    ax1.set_xticks(mid_positions)
    ax1.set_xticklabels(names, rotation=45, ha='right', fontsize=12)
    ax1.set_ylim(1, 2 ** 8)
    ax1.set_yscale('log', base=2)
    ax1.tick_params(labelsize=12)

    # Create secondary y-axis for data transferred
    ax2 = ax1.twinx()
    ax2.set_ylabel('Data Transferred (MB)', fontsize=14)
    ax2.set_ylim(1, 2 ** 16)
    ax2.set_yscale('log', base=2)
    ax2.tick_params(labelsize=12)
    ax2.grid(False)  # Remove the grid from the secondary y-axis

    # Function to format ticks as full numbers
    def log_tick_formatter(val, pos=None):
        return f'{val:g}'

    # Apply the formatter to the y-axes
    ax1.yaxis.set_major_formatter(FuncFormatter(log_tick_formatter))
    ax2.yaxis.set_major_formatter(FuncFormatter(log_tick_formatter))

    # Draw lines for data transferred
    for i, (x, height, transfer) in enumerate(zip(positions, runtime_values, transfer_values)):
        line_width = bar_width
        line_xs = [x - line_width / 2, x + line_width / 2]
        line_ys = [transfer, transfer]
        if i % 2 == 0:  # Data Vaults
            ax2.plot(line_xs, line_ys, 'b-', linewidth=2)
        else:  # Load-Evaluate
            ax2.plot(line_xs, line_ys, 'r-', linewidth=2)

    # Create custom legend for the execution time
    system_legend_patches = [
        plt.Line2D([0], [0], marker='o', color='w',
                   label=f'Runtime ({"LE" if digit == "0" else "DV"})',
                   markersize=10, markerfacecolor=color) for digit, color in color_map.items()]

    # Create custom legend for the data transferred lines
    dv_transfer_legend = plt.Line2D([0], [0], color='b', linewidth=2, label='Data Transferred (DV)')
    le_transfer_legend = plt.Line2D([0], [0], color='r', linewidth=2, label='Data Transferred (LE)')

    # Combine legends
    execution_legend = ax1.legend(handles=system_legend_patches, fontsize=11, title_fontsize='10', loc='upper left',
                                  bbox_to_anchor=(0, 1))
    transfer_legend = ax2.legend(handles=[dv_transfer_legend, le_transfer_legend], fontsize=11, title_fontsize='10',
                                 loc='upper right', bbox_to_anchor=(1, 1))

    # Add legends to the plot
    ax1.add_artist(execution_legend)
    ax2.add_artist(transfer_legend)

    # Save the plot as a PDF and show it
    plt.tight_layout()
    plt.savefig(plot_pdf, format='pdf')
    plt.close()  # Close the plot to free memory


def create_boss_framework_overhead_graph(csv_path, plot_pdf_path):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_path, sep="\t")

    # Calculate the total CPU Time for the "BOSS" module
    boss_time = df[(df['Module'] == 'libBOSS.so') | (df['Module'] == 'libBOSSLazyLoadingCoordinatorEngine.so')]['CPU Time'].sum()

    # Calculate the total CPU Time for all other modules
    other_time = df[(df['Module'] != 'libBOSS.so') & (df['Module'] != 'libBOSSLazyLoadingCoordinatorEngine.so')]['CPU Time'].sum()

    categories = ['BOSS', 'Others']
    values = [boss_time, other_time]

    fig, ax = plt.subplots(figsize=(8, 6))

    ax.barh(categories, values, color=['royalblue', 'orange'])
    # Add some text for labels, title, and custom x-axis tick labels, etc.
    ax.set_xlabel('CPU Time (s)', fontsize=14, fontweight='bold')
    ax.set_title('CPU Time Comparison: BOSS vs Others', fontsize=16, fontweight='bold')
    ax.legend()

    plt.xscale('log')
    # Beautify the plot
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)
    plt.grid(True, which='major', linestyle='--', linewidth=0.5, color='grey')
    plt.tight_layout()

    # Save the figure
    plt.savefig(plot_pdf_path)
    plt.close()

def plot_boss_framework_overhead_manual_vtune_graph(csv_path, pdf_path):
    # Read the CSV file
    data = pd.read_csv(csv_path)

    # Calculate the time excluding BOSS
    data['Kernel'] = data['Total'] - data['BOSS']

    # Reverse the order of the scale factors
    data = data.iloc[::-1].reset_index(drop=True)

    # Get the total time for SF 1
    sf1_total = data[data['Scale'] == 1]['Total'].values[0]

    # Create the figure and axes
    fig, axes = plt.subplots(nrows=len(data), ncols=1, figsize=(8, 1 * len(data)))
    fontsize = 14
    # Define pastel colors
    pastel_orange = '#FFB347'
    darker_pastel_blue = '#4682B4'

    bar_height = 0.2  # Adjust bar height to make bars thinner

    for i, ax in enumerate(axes):
        scale = data['Scale'][i]
        total_time = data['Total'][i]
        boss_time = data['BOSS'][i]
        kernel_time = data['Kernel'][i]

        # Scale the times
        scaling_factor = total_time / sf1_total
        boss_time_scaled = boss_time / scaling_factor
        kernel_time_scaled = kernel_time / scaling_factor

        # Plot BOSS time
        ax.barh(0, boss_time_scaled, height=bar_height, color=pastel_orange, label='BOSS Time' if i == 0 else "")
        # Plot Kernel time
        ax.barh(0, kernel_time_scaled, height=bar_height, left=boss_time_scaled, color=darker_pastel_blue,
                label='Kernel Time' if i == 0 else "")

        # Remove y-axis
        ax.get_yaxis().set_visible(False)
        # Set x-axis limit to the max scaled total time
        ax.set_xlim(0, sf1_total)
        # Remove x-axis ticks
        ax.set_xticks([])
        # Remove the top and right spines
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        # Remove y-axis spines
        ax.spines['left'].set_visible(False)

        # Add the BOSS time in a white box with a thin black border
        ax.text(boss_time_scaled + 0.1, 0, f'{boss_time:.0f}s', va='center', ha='left', color='black', fontsize=fontsize,
                bbox=dict(facecolor='white', edgecolor='black', boxstyle='round,pad=0.3'))
        # Add the total time in a white box with a thin black border
        ax.text(boss_time_scaled + kernel_time_scaled + 0.1, 0, f'{total_time:.0f}s', va='center', ha='left',
                color='black', fontsize=fontsize, bbox=dict(facecolor='white', edgecolor='black', boxstyle='round,pad=0.3'))
        # Add the scale label
        ax.text(-0.02 * sf1_total, 0, f'SF {scale}', va='center', ha='right', rotation=90, fontsize=fontsize, color='black')

    # Remove space between subplots
    plt.subplots_adjust(hspace=0)

    # Add legend on the far right of the plot
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc='center left', bbox_to_anchor=(1, 0.5), fontsize=fontsize)

    # Save the plot to the specified PDF file
    plt.savefig(pdf_path, format='pdf', bbox_inches='tight')

    # Close the plot to free memory
    plt.close()

def create_transfer_size_overhead_graph(csv_path, plot_pdf_path):
    # Improve plot aesthetics with seaborn
    sns.set_theme(style="whitegrid")

    # Read the CSV file
    df = pd.read_csv(csv_path)

    # Set the first column as the index (for the x-axis)
    df.set_index(df.columns[0], inplace=True)

    # Plotting each column after the first as a separate line
    plt.figure(figsize=(12, 8))  # Adjusted for better readability and presentation
    palette = sns.color_palette("husl", len(df.columns))  # Generates a color palette
    for column, color in zip(df.columns, palette):
        plt.plot(df.index, df[column], marker='o', linestyle='-', linewidth=2, label=column, color=color)

    # Adding titles and labels with improved font sizes
    plt.title('Data Comparison over Selectivity', fontsize=16, fontweight='bold')
    plt.xlabel(df.index.name.capitalize(), fontsize=14, fontweight='bold')
    plt.ylabel('Bytes', fontsize=14, fontweight='bold')

    plt.xscale('log')
    plt.yscale('log')

    # Customize the ticks for better readability
    plt.xticks(fontsize=12, fontweight='normal')
    plt.yticks(fontsize=12, fontweight='normal')

    # Add a legend outside the plot to avoid covering the data
    plt.legend(title='Metrics', bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=12)

    plt.tight_layout(rect=(0, 0, 0.85, 1))  # Adjust as necessary to fit legend and labels

    # Save the plot as a PDF file
    plt.savefig(plot_pdf_path, bbox_inches='tight')
    plt.close()


def create_transfer_time_overhead_graph(csv_path, plot_pdf_path):
    # Improve plot aesthetics with seaborn
    sns.set_theme(style="whitegrid")

    # Read the CSV file
    df = pd.read_csv(csv_path)

    # Set the first column as the index (for the x-axis)
    df.set_index(df.columns[0], inplace=True)

    # Plotting each column after the first as a separate line
    plt.figure(figsize=(12, 8))  # Adjusted for better readability and presentation
    palette = sns.color_palette("husl", len(df.columns))  # Generates a color palette
    for column, color in zip(df.columns, palette):
        plt.plot(df.index, df[column], marker='o', linestyle='-', linewidth=2, label=column, color=color)

    # Adding titles and labels with improved font sizes
    plt.title('Time Comparison over Selectivity', fontsize=16, fontweight='bold')
    plt.xlabel(df.index.name.capitalize(), fontsize=14, fontweight='bold')
    plt.ylabel('MS', fontsize=14, fontweight='bold')

    plt.xscale('log')
    plt.yscale('linear')

    # Customize the ticks for better readability
    plt.xticks(fontsize=12, fontweight='normal')
    plt.yticks(fontsize=12, fontweight='normal')

    # Add a legend outside the plot to avoid covering the data
    plt.legend(title='Metrics', bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=12)

    plt.tight_layout(rect=(0, 0, 0.85, 1))  # Adjust as necessary to fit legend and labels

    # Save the plot as a PDF file
    plt.savefig(plot_pdf_path, bbox_inches='tight')
    plt.close()


def create_ranges_graph(json_path, plot_pdf):
    sns.set(style="whitegrid")
    # Load JSON data
    with open(json_path, 'r') as file:
        data = json.load(file)

    # Create a DataFrame
    df = pd.DataFrame(data['benchmarks'])

    # Extracting the number of ranges and converting it to an integer
    df['number_of_ranges'] = df['name'].apply(lambda x: int(x.split('/')[1]))

    # Sorting the DataFrame by the number of ranges
    df = df.sort_values(by='number_of_ranges')

    # Plotting the data with seaborn for enhanced aesthetics
    plt.figure(figsize=(8, 4.5), dpi=1000)
    sns.lineplot(x='number_of_ranges', y='real_time', data=df, marker='o', linewidth=2.5, markersize=10,
                 color='dodgerblue')

    # Beautifying the plot
    plt.xlabel('Number of Ranges', fontsize=16)
    plt.ylabel('Real Time (ms)', fontsize=16)
    plt.xscale('log', base=2)
    plt.yscale('log')
    plt.xticks(fontsize=14)  # Set x-ticks to only the unique values of number of ranges
    plt.yticks(fontsize=14)
    plt.xscale('log', base=2)
    plt.yscale('log')
    plt.grid(True)

    # Save the plot as a PDF and show it
    plt.tight_layout()
    plt.savefig(plot_pdf, format='pdf')
    plt.close()  # Close the plot to free memory


def create_in_mem_convergence_graph(json_path, csv_path, plot_pdf):

    scan_time = 268.05778967316
    # Load JSON data
    with open(json_path, 'r') as file:
        data = json.load(file)

    # Process JSON data
    df_json = pd.DataFrame(data['benchmarks'])
    df_json['loading_strategy'] = df_json['name'].apply(lambda x: 'lazy' if '0' in x.split('/')[1] else 'eager')
    df_json['number_of_queries'] = df_json['name'].apply(lambda x: int(x.split('/')[2]))
    df_json_lazy = df_json[df_json['loading_strategy'] == 'lazy'].sort_values(by='number_of_queries')
    df_json_eager = df_json[df_json['loading_strategy'] == 'eager'].sort_values(by='number_of_queries')

    # Load CSV data
    df_csv = pd.read_csv(csv_path)
    df_csv['total_transfer_time'] = (df_csv['Pretransfer'] + df_csv['Download']) / 1000
    df_csv_lazy = df_csv[df_csv['BenchmarkState'] == 0].sort_values(by='NumQueries')
    df_csv_eager = df_csv[df_csv['BenchmarkState'] == 1].sort_values(by='NumQueries')

    df_json_lazy['real_time_no_transfer'] = df_json_lazy['real_time'] - df_csv_lazy['total_transfer_time']

    df_json_lazy['real_time_avg'] = df_json_lazy['real_time'] / df_json_lazy['number_of_queries']

    df_json_eager['real_total'] = df_json_eager['real_time'] + df_csv_eager['total_transfer_time']
    df_json_eager['real_time_no_scan'] = df_json_eager['real_total'] - (df_json_eager['number_of_queries'] * scan_time)

    df_json_lazy['real_time_no_transfer_avg'] = \
        df_json_lazy['real_time_no_transfer'] / df_json_lazy['number_of_queries']
    df_json_eager['real_time_no_transfer_avg'] = \
        df_json_eager['real_time'] / df_json_eager['number_of_queries']

    # Plotting
    plt.figure(figsize=(9, 5), dpi=1000)
    # Original data - Lazy and Eager with solid lines
    plt.plot(df_json_lazy['number_of_queries'], df_json_lazy['real_time'], label='Lazy (w/ Transfer)',
             marker='o', linestyle='-',
             color='blue')
    plt.plot(df_json_eager['number_of_queries'], df_json_eager['real_time_no_scan'], label='Eager (w/ Transfer)',
             marker='s', linestyle='-',
             color='red')

    # Data with transfer time - Lazy and Eager with dashed lines, same colors as their counterparts
    plt.plot(df_csv_lazy['NumQueries'], df_csv_lazy['total_transfer_time'], label='Lazy Transfer Time', marker='^',
             linestyle='--', color='blue')
    plt.plot(df_csv_eager['NumQueries'], df_csv_eager['total_transfer_time'], label='Eager Transfer Time',
             marker='v', linestyle='--', color='red')

    plt.xlabel('Number of Queries', fontsize=16)
    plt.ylabel('Real Time (ms)', fontsize=16)
    plt.xscale('log', base=2)
    plt.yscale('log')
    plt.legend(fontsize=14)
    plt.grid(True)

    # Increase font size of ticks
    plt.xticks(fontsize=14)
    plt.yticks(fontsize=14)

    # Save the plot as a PDF and show it
    plt.tight_layout()
    plt.savefig(plot_pdf, format='pdf')
    plt.close()  # Close the plot to free memory


def create_in_mem_convergence_size_graph(csv_path, plot_pdf):
    # Reading the CSV file
    df = pd.read_csv(csv_path)

    # Rename the 'Requested' column to 'Sent' in the DataFrame
    df = df.rename(columns={'Requested': 'Sent'})

    # Converting byte values to megabytes for relevant columns
    for column in ['Loaded', 'Sent', 'Headers']:
        df[column] = df[column] / 1048576  # Convert from bytes to MB

    # Splitting the DataFrame into lazy loading (0) and eager loading (1) data
    lazy_loading_df = df[df['BenchmarkState'] == 0]
    eager_loading_df = df[df['BenchmarkState'] == 1]

    # Line styles and markers for differentiation
    attributes = {
        'Loaded': {'linestyle': '-', 'marker': 'o'},
        'Sent': {'linestyle': ':', 'marker': 'o'},
        'Headers': {'linestyle': '--', 'marker': 'o'},
    }

    # Setting up the plot
    plt.figure(figsize=(10, 4), dpi=1000)

    # Plotting for lazy and eager loading
    for strategy, color in [('Lazy', 'blue'), ('Eager', 'red')]:
        df_filtered = lazy_loading_df if strategy == 'Lazy' else eager_loading_df
        for column, props in attributes.items():
            plt.plot(df_filtered['NumQueries'], df_filtered[column], label=f'{strategy} {column}', color=color, **props)

    # Creating custom legends
    from matplotlib.lines import Line2D
    legend = [Line2D([0], [0], color='blue', **attributes['Loaded'], label='Lazy Loaded'),
                    Line2D([0], [0], color='red', **attributes['Loaded'], label='Eager Loaded'),
                    Line2D([0], [0], color='blue', **attributes['Sent'], label='Lazy Sent'),
                    Line2D([0], [0], color='red', **attributes['Sent'], label='Eager Sent'),
                    Line2D([0], [0], color='blue', **attributes['Headers'], label='Lazy Headers'),
                    Line2D([0], [0], color='red', **attributes['Headers'], label='Eager Headers')]

    # Setting the x and y axis to log scale
    plt.xscale('log', base=2)
    plt.yscale('log', base=10)

    # Adding labels and titles
    plt.xlabel('Number of Queries', fontsize=16)
    plt.ylabel('Data Transferred (MB)', fontsize=16)

    # Adding and positioning the merged legend
    # plt.legend(handles=legend, loc='upper right', fontsize=13, bbox_to_anchor=(1, 0.95))
    plt.legend(handles=legend, loc='upper center', ncol=3, fontsize=13, bbox_to_anchor=(0.5, 1.4))

    # Increase font size of ticks
    plt.xticks(fontsize=14)
    plt.yticks(fontsize=14)

    # Display the plot
    plt.grid(True, which="both", ls="--")
    plt.tight_layout(rect=(0, 0, 1, 0.95))
    plt.savefig(plot_pdf, format='pdf')


def run_vtune_boss_overhead(benchmark_path, results_path, report_path, vtune_path):
    vars_path = vtune_path.joinpath("env", "vars.sh")
    vars_command = f". {str(vars_path)}"

    collect_options = ["-collect hotspots", f"-result-dir {results_path}",
                       f"-- {benchmark_path} --benchmark_filter=BenchmarkFrameworkOverhead"]
    report_options = ["-report hotspots", f"-result-dir {results_path}", "-format csv", f"-report-output {report_path}",
                      "-group-by=module"]

    run_executable(f"{vars_command} && vtune", collect_options, shell=True)
    run_executable(f"{vars_command} && vtune", report_options, shell=True)


def run_tpch_benchmark(benchmark_path, results_path):
    options = [f"--benchmark_out={results_path}", "--benchmark_out_format=json", "--benchmark_filter=BenchmarkQueriesTPCH"]
    run_executable(benchmark_path, options)


def run_queries_benchmark(benchmark_path, results_path):
    options = [f"--benchmark_out={results_path}", "--benchmark_out_format=json", "--benchmark_filter=BenchmarkQueries"]
    run_executable(benchmark_path, options)


def run_transfer_overhead_benchmark(benchmark_path):
    options = ["--benchmark_filter=BenchmarkOverhead"]
    run_executable(benchmark_path, options)


def run_ranges_benchmark(benchmark_path, results_path):
    options = [f"--benchmark_out={results_path}", "--benchmark_out_format=json", "--benchmark_filter=BenchmarkRanges"]
    run_executable(benchmark_path, options)


def run_in_mem_convergence_benchmark(benchmark_path, results_path):
    options = [f"--benchmark_out={results_path}", "--benchmark_out_format=json",
               "--benchmark_filter=BenchmarkInMemConvergence"]
    run_executable(benchmark_path, options)


def handle_queries_benchmark(build_path):
    benchmark_path = build_path.joinpath("Benchmarks")
    results_path = build_path.joinpath("results", "benchmark_results.json")
    output_pdf_path = build_path.joinpath("results", "plots", "benchmark_queries.pdf")
    size_results_path = build_path.joinpath("results", "miniseed_transfer_size_overhead.csv")
    time_results_path = build_path.joinpath("results", "miniseed_transfer_time_overhead.csv")
    run_queries_benchmark(str(benchmark_path), str(results_path))
    create_miniseed_queries_size_time_graph(str(results_path), str(size_results_path), str(time_results_path), str(output_pdf_path))


def handle_tpch_queries_benchmark(build_path):
    benchmark_path = build_path.joinpath("Benchmarks")
    results_path = build_path.joinpath("results", "benchmark_tpch_results.json")
    output_pdf_path = build_path.joinpath("results", "plots", "benchmark_tpch_queries.pdf")
    size_results_path = build_path.joinpath("results", "tpch_transfer_size_overhead.csv")
    time_results_path = build_path.joinpath("results", "tpch_transfer_time_overhead.csv")
    run_queries_benchmark(str(benchmark_path), str(results_path))
    create_tpch_queries_graph(str(results_path), str(size_results_path), str(time_results_path), str(output_pdf_path))


def handle_framework_overhead_benchmark(build_path, vtune_path):
    benchmark_path = build_path.joinpath("Benchmarks")
    results_path = build_path.joinpath("results", "hotspot_results")
    report_path = build_path.joinpath("results", "hotspot_report.csv")
    output_plot_pdf = build_path.joinpath("results", "plots", "framework_overhead.pdf")
    run_vtune_boss_overhead(str(benchmark_path), str(results_path), str(report_path), vtune_path)
    create_boss_framework_overhead_graph(str(report_path), str(output_plot_pdf))


def handle_overhead_benchmark(build_path):
    benchmark_path = build_path.joinpath("Benchmarks")
    size_results_path = build_path.joinpath("results", "transfer_size_overhead.csv")
    time_results_path = build_path.joinpath("results", "transfer_time_overhead.csv")
    output_plot_size_pdf = build_path.joinpath("results", "plots", "transfer_size_overhead.pdf")
    output_plot_time_pdf = build_path.joinpath("results", "plots", "transfer_time_overhead.pdf")
    run_transfer_overhead_benchmark(str(benchmark_path))
    create_transfer_size_overhead_graph(str(size_results_path), str(output_plot_size_pdf))
    create_transfer_time_overhead_graph(str(time_results_path), str(output_plot_time_pdf))


def handle_ranges_benchmark(build_path):
    benchmark_path = build_path.joinpath("Benchmarks")
    results_path = build_path.joinpath("results", "benchmark_ranges_results.json")
    output_pdf_path = build_path.joinpath("results", "plots", "benchmark_ranges.pdf")
    run_ranges_benchmark(str(benchmark_path), str(results_path))
    create_ranges_graph(str(results_path), str(output_pdf_path))


def handle_in_mem_convergence_benchmark(build_path):
    benchmark_path = build_path.joinpath("Benchmarks")
    results_path = build_path.joinpath("results", "benchmark_in_mem_convergence_results.json")
    csv_res_path = build_path.joinpath("results", "transfer_time_overhead_mem_conv.csv")
    output_pdf_path = build_path.joinpath("results", "plots", "benchmark_in_mem_convergence.pdf")
    run_in_mem_convergence_benchmark(str(benchmark_path), str(results_path))
    create_in_mem_convergence_graph(str(results_path), str(csv_res_path), str(output_pdf_path))


def setup_results(build_path):
    results_path = build_path.joinpath("results")
    plots_path = results_path.joinpath("plots")
    run_executable("mkdir", [str(results_path)])
    run_executable("mkdir", [str(plots_path)])


def delete_results(build_path):
    results_path = build_path.joinpath("results")
    if results_path.is_dir():
        shutil.rmtree(results_path)


def main():
    parser = argparse.ArgumentParser(
        description="Run benchmarks and produce plots for BOSSRemoteBinaryLoaderEngine when given the path to the build directory")
    parser.add_argument("build_dir_path", help="Path to build directory")
    parser.add_argument("-v", "--vtune_dir_path",
                        help="Path to vtune directory, if not present no vtune benchmarks will be run")
    parser.add_argument("-o", "--overwrite",
                        help="Overwrite results directory if set")

    args = parser.parse_args()
    build_dir_path = Path(args.build_dir_path)

    if args.overwrite:
        delete_results(build_dir_path)

    setup_results(build_dir_path)

    handle_queries_benchmark(build_dir_path)
    handle_tpch_queries_benchmark(build_dir_path)
    handle_overhead_benchmark(build_dir_path)
    handle_ranges_benchmark(build_dir_path)
    handle_in_mem_convergence_benchmark(build_dir_path)
    if args.vtune_dir_path:
        handle_framework_overhead_benchmark(build_dir_path, Path(args.vtune_dir_path))


if __name__ == "__main__":
    main()
