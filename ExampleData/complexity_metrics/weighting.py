#!/usr/bin/env python3

import subprocess
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from argparse import ArgumentParser
from scipy.signal import argrelmin
from pandas import DataFrame
from statsmodels.nonparametric.kde import KDEUnivariate
from matplotlib import cm
from matplotlib.patches import Rectangle


def token_histogram(dbfile):
    result = subprocess.run(
        ['cargo', 'run', '--release', '--', 'histogram', '--input', dbfile],
        capture_output=True,
        check=True,
        text=True,
    )

    df = DataFrame(json.loads(result.stdout))
    df['logf'] = np.log10(df.frequency)
    df['rank_avg'] = df.frequency.rank(method='average', ascending=False)
    df['rank_dense'] = df.frequency.rank(method='dense', ascending=False)
    df['rank_min'] = df.frequency.rank(method='min', ascending=False)

    return df

# Simple 1D clustering based on kernel density estimation.
# The returned array assigns a 0-based cluster index for
# the corresponding element in `x`.
def cluster(x, adjust=1/3):
    xs = np.asanyarray(x).ravel(order='K')
    idx = np.argsort(xs)
    xs = xs[idx]

    kde = KDEUnivariate(xs)
    kde.fit(adjust=adjust)
    pdf = kde.evaluate(xs)

    # Cumulatively count how many of the
    # cluster boundaries each value exceeds.
    boundinds, = argrelmin(pdf)
    bounds = xs[boundinds]
    invidx = np.argsort(idx)
    clusters = np.sum(xs.reshape(-1, 1) >= bounds, axis=1)
    clustinds = clusters[invidx].reshape(x.shape)

    return clustinds, kde

def fib(n):
    rootfive = 5 ** 0.5
    phi = (1 + rootfive) / 2
    psi = (1 - rootfive) / 2

    return np.round((phi**n - psi**n) / rootfive).astype(int)

def parse_args():
    parser = ArgumentParser(
        description='Calculate node weights based on their frequency.'
    )
    parser.add_argument('--codedb', type=str, required=False,
                        default='sede_stackoverflow_favorites.sqlite3',
                        help='SQLite database containing SQL code corpus')
    parser.add_argument('--plot', type=str, required=False,
                        default='sede_stackoverflow_favorites.pdf',
                        help='Output frequency distribution plot')
    parser.add_argument('--table', type=str, required=False,
                        default='sede_stackoverflow_favorites.csv',
                        help='Output CSV table of ranks, frequencies, and weights')
    return parser.parse_args()

def main():
    args = parse_args()
    df = token_histogram(args.codedb)
    clust, kde = cluster(df.logf)

    # Add computed probability density function.
    # Reverse cluster indices (sort decreasing).
    # Add 2 so that weights start from 1 and do not repeat.
    df['pdf'] = kde.evaluate(df.logf.to_numpy())
    df['cluster'] = clust.max() - clust
    df['weight'] = fib(df.cluster + 2)

    # Write table to file for reproducibility, but also
    # print it in a human-readable format.
    df.to_csv(args.table)
    print(df.to_string())

    # Evaluate the PDF at more points
    # in order to make the plot smoother
    xs = np.linspace(df.logf.min(), df.logf.max(), num=1024)
    pdf = kde.evaluate(xs)

    # Plot the frequency distribution of
    # the tokens, just for reference.
    sns.set()
    sns.rugplot(df.logf)
    sns.lineplot(x=xs, y=pdf, color='red')
    ax = plt.gca()

    # Plot regions and their weights
    ylim = pdf.max() * 1.1
    ranges = df.groupby('weight').logf.aggregate(['min', 'max'])
    cmap = cm.get_cmap('Pastel1')
    colors = [cmap(i) for i in range(len(ranges))]
    hatches = ['/', '\\', 'O', 'x', '.', '+', '*', '-', '|']

    for row, color, hatch in zip(ranges.itertuples(), colors, hatches):
        weight = str(row.Index)
        x, y = row.min, 0.0
        width, height = row.max - row.min, ylim

        patch = ax.add_patch(Rectangle((x, y), width, height))
        patch.set_label(weight)
        patch.set_facecolor(color)
        patch.set_edgecolor('grey')
        patch.set_hatch(hatch * 2) # increase density
        patch.set_alpha(3/4)

    ax.set_xlim(df.logf.min(), df.logf.max())
    ax.set_ylim(0, ylim)
    ax.set_xlabel('$log_{10}(frequency)$')
    ax.set_ylabel('Probability Density')
    plt.title('\n'.join(s.strip() for s in
        '''
        SQL Token Frequency Distribution
        and weights assigned by clustering
        '''
        .splitlines()
    ))
    plt.legend()
    plt.tight_layout()
    plt.savefig(args.plot)
    plt.close()

if __name__ == '__main__':
    main()
