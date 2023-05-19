

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


def get_contextual_characteristics_for_buildings(primary,spatial_weights, serial=False, n_workers=4)
    
    gdf = primary.set_index('uID')
    unique_id = 'uID'
    
    means = {}
    ranges = {}
    theils = {}
    simpsons = {}

    for ch in gdf.columns:
        means[ch] = []
        ranges[ch] = []
        theils[ch] = []
        simpsons[ch] = []
        
    gdf = gdf.replace(np.inf, np.nan).fillna(0)  # normally does not happen, but to be sure
    chars = gdf.columns
    
    gdf['lcdMes'] = gdf.apply(
            lambda row: row.lcdMes if row.lcdMes >= 0 else 0,
            axis=1,
        )  # normally does not happen, but to be sure
    
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
        # Added an exception where we do not have enough unique values.
        try:
            bins[c] = mapclassify.HeadTailBreaks(gdf[c]).bins
        except:
            # Handle the warning by setting a fallback classification method. Not sure if this is what we want.
            bins[c] = mapclassify.EqualInterval(gdf[c]).bins
    for c in natural:
        bins[c] = mapclassify.gadf(gdf[c], method='NaturalBreaks')[1].bins
        
    if serial == TRUE:
        for index in tqdm(gdf.index):
            neighbours = [index]
            neighbours += spatial_weights.neighbors[index]

            subset = gdf.loc[neighbours]
            for ch in chars:

                values_list = subset[ch] 
                idec = mm.limit_range(values_list, rng=(10, 90))
                iquar = mm.limit_range(values_list, rng=(25, 75))

                means[ch].append(np.mean(iquar))
                ranges[ch].append(max(iquar) - min(iquar))
                theils[ch].append(_theil(idec))

                sample_bins = mapclassify.UserDefined(values_list, list(bins[ch]))
                counts = dict(zip(bins[ch], sample_bins.counts))
                simpsons[ch].append(_simpson_di(counts))
    else:
        client = Client(n_workers=n_workers)
        print('Dask Dashboard: http://127.0.0.1:8787/status')
        
        
        # Create delayed computation for all characteristics
        @delayed
        def compute_characteristics(subset, bins):
            results = {}
            for ch in chars:
                values_list = subset[ch]
                idec = mm.limit_range(values_list, rng=(10, 90))
                iquar = mm.limit_range(values_list, rng=(25, 75))
                sample_bins = mapclassify.UserDefined(values_list, list(bins[ch]))
                counts = dict(zip(bins[ch], sample_bins.counts))

                results[ch] = {
                    'meanIQ3': np.mean(iquar),
                    'rangeIQ3': max(iquar) - min(iquar),
                    'theilID3': _theil(idec),
                    'simpson': _simpson_di(counts)
                }
            return results

        # List to store delayed computations
        delayed_results = []

        for index in tqdm(gdf.index):
            neighbours = [index]
            neighbours += spatial_weights.neighbors[index]
            subset = gdf.loc[neighbours]

            # Create delayed computation for subset
            delayed_result = compute_characteristics(subset, bins)
            delayed_results.append(delayed_result)

        # Compute delayed results and store the computed values in dictionaries
        with ProgressBar():
            results = dask.compute(*delayed_results)

        # Loop over the results and extract the computed statistics
        for result in results:
            for ch, stats in result.items():
                means[ch].append(stats['meanIQ3'])
                ranges[ch].append(stats['rangeIQ3'])
                theils[ch].append(stats['theilID3'])
                simpsons[ch].append(stats['simpson'])
                
        client.close()
        
    contextual = {}
    for ch in chars:
        contextual[ch + '_meanIQ3'] = means[ch]
        contextual[ch + '_rangeIQ3'] = ranges[ch]
        contextual[ch + '_theilID3'] = theils[ch]
        contextual[ch + '_simpson'] = simpsons[ch]

    contextual = pd.DataFrame(contextual, index=gdf.index)
    
    building_ids = list(buildings.uID)
    data = contextual.copy()
    data = data.loc[data.index.isin(building_ids)]
    
    return data

        
