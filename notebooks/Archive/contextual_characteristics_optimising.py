import warnings
import mapclassify
import libpysal
import momepy as mm
import numpy as np
import pandas as pd
import scipy as sp
import dask
from dask.distributed import Client
from dask import delayed
from dask.distributed import progress
from dask.diagnostics import ProgressBar
from tqdm.auto import tqdm


def _theil(y):
    y = np.array(y)
    n = len(y)
    plus = y + np.finfo('float').tiny * (y == 0)  # can't have 0 values
    yt = plus.sum(axis=0)
    s = plus / (yt * 1.0)
    invalid_mask = (s <= 0) | (n * s <= 0)
    s_invalid = np.where(invalid_mask, np.nan, s)  # Set invalid values to NaN
    lns = np.where(invalid_mask, 0, np.log(n * s_invalid))  # Set invalid values to 0
    slns = s * lns
    t = np.nansum(slns)  # Use np.nansum to handle NaN values
    return t


def _simpson_di(data):
    def p(n, N):
        if n == 0:
            return 0
        return float(n) / N

    N = sum(data.values())

    return sum(p(n, N) ** 2 for n in data.values() if n != 0)


def process_row(row, chars, bins, gdf, spatial_weights):
    result_means = {}
    result_ranges = {}
    result_theils = {}
    result_simpsons = {}

    index = row.name
    neighbours = [index]
    neighbours += spatial_weights.neighbors[index]
    subset = gdf.loc[neighbours]

    for ch in chars:
        values_list = subset[ch]
        idec = mm.limit_range(values_list, rng=(10, 90))
        iquar = mm.limit_range(values_list, rng=(25, 75))

        result_means[ch] = np.mean(iquar)
        result_ranges[ch] = max(iquar) - min(iquar)
        result_theils[ch] = _theil(idec)

        sample_bins = mapclassify.UserDefined(values_list, list(bins[ch]))
        counts = dict(zip(bins[ch], sample_bins.counts))
        result_simpsons[ch] = _simpson_di(counts)

    return result_means, result_ranges, result_theils, result_simpsons


def process_data(gdf, chars, bins, spatial_weights, client):
    # Create a Dask DataFrame from the input GeoDataFrame
    ddf = client.gather(client.scatter(gdf))

    # Create a Dask bag from the Dask DataFrame
    bag = dask.bag.from_sequence(ddf.iterrows(), partition_size=100)

    # Process rows in parallel
    results = bag.map(lambda x: process_row(x[1], chars, bins, gdf, spatial_weights)).compute()
    print(results)

    # Collect the results
    means = {ch: [] for ch in chars}
    ranges = {ch: [] for ch in chars}
    theils = {ch: [] for ch in chars}
    simpsons = {ch: [] for ch in chars}

    for result_means_row, result_ranges_row, result_theils_row, result_simpsons_row in results:
        for ch, result in zip(chars, result_means_row):
            if isinstance(result, np.ndarray):
                means[ch].append(result.item())
            else:
                means[ch].append(result)
        for ch, result in zip(chars, result_ranges_row):
            if isinstance(result, np.ndarray):
                ranges[ch].append(result.item())
            else:
                ranges[ch].append(result)
        for ch, result in zip(chars, result_theils_row):
            if isinstance(result, np.ndarray):
                theils[ch].append(result.item())
            else:
                theils[ch].append(result)
        for ch, result in zip(chars, result_simpsons_row):
            if isinstance(result, np.ndarray):
                simpsons[ch].append(result.item())
            else:
                simpsons[ch].append(result)

    # Vertically stack the arrays
    means = {ch: np.vstack(vals) for ch, vals in means.items()}
    ranges = {ch: np.vstack(vals) for ch, vals in ranges.items()}
    theils = {ch: np.vstack(vals) for ch, vals in theils.items()}
    simpsons = {ch: np.vstack(vals) for ch, vals in simpsons.items()}

    for ch, vals in means.items():
        print(f"{ch}_means:")
        print(vals)

    return means, ranges, theils, simpsons


def get_contextual_characteristics_for_buildings(primary, buildings, spatial_weights, client):
    print('Contextual characteristics are being calculated!')
    gdf = primary.set_index('uID')
    unique_id = 'uID'

    means = {}
    ranges = {}
    theils = {}
    simpsons = {}

    gdf = gdf.replace(np.inf, np.nan).fillna(0)

    chars = gdf.columns

    gdf['lcdMes'] = gdf['lcdMes'].clip(lower=0)

    skewness = pd.DataFrame(index=chars)
    for c in chars:
        skewness.loc[c, 'skewness'] = sp.stats.skew(gdf[c])

    headtail = list(skewness.loc[skewness.skewness >= 1].index)
    to_invert = skewness.loc[skewness.skewness <= -1].index

    for inv in to_invert:
        gdf[inv + '_r'] = gdf[inv].max() - gdf[inv]
    inverted = [x for x in gdf.columns if '_r' in x]
    headtail = headtail + inverted
    natural = [x for x in chars if x not in headtail]

    bins = {}
    warnings.filterwarnings("ignore", category=UserWarning)
    for c in headtail:
        try:
            bins[c] = mapclassify.HeadTailBreaks(gdf[c]).bins
        except:
            bins[c] = mapclassify.EqualInterval(gdf[c]).bins
    for c in natural:
        bins[c] = mapclassify.gadf(gdf[c], method='NaturalBreaks')[1].bins

    means, ranges, theils, simpsons = process_data(gdf, chars, bins, spatial_weights, client)

    contextual = pd.DataFrame(index=gdf.index)
    for ch in chars:
        ch_means = means[ch] if ch in means else np.array([])
        ch_ranges = ranges[ch] if ch in ranges else np.array([])
        ch_theils = theils[ch] if ch in theils else np.array([])
        ch_simpsons = simpsons[ch] if ch in simpsons else np.array([])

        if len(ch_means) > 0:
            contextual[ch + '_meanIQ3'] = ch_means.flatten()
        if len(ch_ranges) > 0:
            contextual[ch + '_rangeIQ3'] = ch_ranges.flatten()
        if len(ch_theils) > 0:
            contextual[ch + '_theilIQ3'] = ch_theils.flatten()
        if len(ch_simpsons) > 0:
            contextual[ch + '_simpsonIQ3'] = ch_simpsons.flatten()

    building_ids = list(buildings.uID)
    data = contextual.copy()
    data = data.loc[data.index.isin(building_ids)]

    print('Calculation done!')
    return data
