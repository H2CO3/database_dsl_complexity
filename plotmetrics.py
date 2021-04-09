#!/usr/bin/env python3

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import rcParams


# The reference DB/language is the SQL implementation,
# and the reference metric is the dumbest one, raw
# token count. Therefore, we sort queries in increasing
# order of raw token count _of the SQL version_.
def read_metrics(filename, reference=None):
    df = pd.read_csv(filename, index_col='query')
    if 'elapsed' in df.columns:
        del df['elapsed']

    if reference is None:
        df.sort_values('token_count', inplace=True)
    else:
        df = df.loc[reference.index]

    return df

def normalize(df, minima, maxima):
    return (df - minima) / (maxima - minima)

# Return the first and last part of the name, separated by
# an ellipsis. Do not add an ellipsis to one-word names.
def abbrev_name(names):
    return names.str.split('_').apply(
        lambda parts: '…'.join(parts[::max(len(parts) - 1, 1)])
    )

if __name__ == '__main__':
    inputs = [
        ('SQL',       'ExampleData/metrics.csv'),
        ('C#/Entity', 'DotNETEntity/metrics.csv'),
        ('MongoDB',   'PythonMongoDB/metrics.csv'),
        ('CoreData',  'SwiftCoreStore/metrics.csv'),
    ]

    ref = read_metrics('ExampleData/metrics.csv')
    dfs = [read_metrics(fname, reference=ref) for _, fname in inputs]

    long_df = pd.concat(dfs)
    minima, maxima = long_df.min(), long_df.max()

    dfs = [normalize(df, minima, maxima) for df in dfs]

    for df, (tech, _) in zip(dfs, inputs):
        df['db'] = tech

    norm_dfs = pd.concat(dfs).reset_index().set_index(['db', 'query'])
    norm_dfs.to_csv('all_metrics.csv')

    rcParams.update({ 'figure.autolayout': True })
    sns.set()
    plotted_metrics = [
        'token_count',
        'token_entropy',
        'halstead_difficulty',
        'halstead_effort',
        'weighted_node_count',
    ]

    # Plot metrics for each database (all queries at once)
    for (name, _), df in zip(inputs, dfs):
        df = df[plotted_metrics].reset_index().melt('query')
        df['query'] = abbrev_name(df['query'])
        sns.catplot(
            data=df,
            x='query',
            y='value',
            hue='variable',
            ci=None,
            kind='point',
            legend=False,
            palette='Set2',
        )
        plt.legend(loc='upper left')
        plt.xticks(rotation='vertical')
        plt.savefig('metrics_bymetric_{}.pdf'.format(name.replace('/', '-')))
        plt.close()

    # Plot a given metric for all databases and queries
    # (bar plot for comparing complexity across databases)
    df_byquery = pd.concat([df.reset_index() for df in dfs])
    df_byquery.sort_index(inplace=True)
    df_byquery.set_index(['query', 'db'], inplace=True)

    for metric in plotted_metrics:
        df = df_byquery[[metric]].reset_index().melt(['db', 'query'])

        sns.catplot(
            data=df,
            x='db',
            y='value',
            col='query',
            ci=None,
            kind='bar',
            legend=False,
            palette='Set2',
        )
        plt.savefig('metrics_byquery_bydb_{}.pdf'.format(metric))
        plt.close()

        df['query'] = abbrev_name(df['query'])
        grid = sns.catplot(
            data=df,
            x='query',
            y='value',
            col='db',
            ci=None,
            kind='bar',
            legend=False,
            palette='Set2',
        )

        # plt.xticks(rotation='vertical') # buggy
        for ax in grid.axes.ravel():
            ax.set_xticklabels(ax.get_xticklabels(), rotation=90)

        plt.savefig('metrics_bydb_byquery_{}.pdf'.format(metric))
        plt.close()

        sns.catplot(
            data=df,
            x='query',
            y='value',
            hue='db',
            ci=None,
            kind='point',
            legend=False,
            palette='Set2',
        )
        plt.legend(loc='upper left')
        plt.xticks(rotation='vertical')
        plt.savefig('metrics_bydb_{}.pdf'.format(metric))
        plt.close()
