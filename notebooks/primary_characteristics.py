# Calculate floor area with simple assumption of standard floor height of 2.65m
import momepy as mm
import libpysal


def get_primary_characteristics_for_buildings(buildings, tessellation, blocks, streets) -> None:
    print('Primary characteristics being calculated!')

    # print('Calculating Floor Area \n')
    buildings['floor_area'] = (buildings["height_70p"] / 2.65) * buildings.area

    # print('Calculating Area \n')
    buildings['sdbAre'] = mm.Area(buildings).series

    # print('Calculating Volume \n')
    buildings['sdbVol'] = mm.Volume(buildings, 'height_70p', 'sdbAre').series

    # print('Calculating Perimeter\n')
    buildings['sdbPer'] = mm.Perimeter(buildings).series

    # print('Calculating Courtyard Area \n')
    # Might have to be removed. There seems to be some faulty courtyards (negative values)
    buildings['sdbCoA'] = mm.CourtyardArea(buildings, 'sdbAre').series

    # # print('Calculating Form Factor \n')
    # buildings['ssbFoF'] = mm.FormFactor(buildings, 'sdbVol', 'sdbAre').series

    # print('Calculating Volume Facade Ratio\n')
    # Calculates the volume/facade ratio of each object in a given GeoDataFrame.
    buildings['ssbVFR'] = mm.VolumeFacadeRatio(buildings, 'height_70p', 'sdbVol', 'sdbPer').series

    # print('Calculating Circular Compactness \n')
    # Calculates the compactness index (area/area if enclosing circle) of each object in a given GeoDataFrame.
    buildings['ssbCCo'] = mm.CircularCompactness(buildings, 'sdbAre').series

    # print('Calculating Corners \n')
    # Calculates the number of corners of each object in a given GeoDataFrame. Uses only external shape (shapely.geometry.exterior), courtyards are not included.
    buildings['ssbCor'] = mm.Corners(buildings, verbose=False).series

    # print('Calculating Squareness \n')
    buildings['ssbSqu'] = mm.Squareness(buildings, verbose=False).series

    # print('Calculating Rectangular Index \n')
    buildings['ssbERI'] = mm.EquivalentRectangularIndex(buildings, 'sdbAre', 'sdbPer').series

    # print('Calculating Elongation \n')
    buildings['ssbElo'] = mm.Elongation(buildings).series

    # print('Calculating Mean Distance centroid - corners and standard deviation \n')
    # Calculates the mean distance centroid - corners and standard deviation
    cencon = mm.CentroidCorners(buildings, verbose=False)
    buildings['ssbCCM'] = cencon.mean
    buildings['ssbCCD'] = cencon.std

    # print('Calculating Building Orientation \n')
    buildings['stbOri'] = mm.Orientation(buildings, verbose=False).series

    # print('Calculating tessellation orientation\n')
    tessellation['stcOri'] = mm.Orientation(tessellation, verbose=False).series

    # print('Calculate Cell Alignment')
    # DEACTIVATED TO TRY WITH ALL TESSELLATION CELLS
    buildings['stbCeA'] = mm.CellAlignment(buildings, tessellation, 'stbOri', 'stcOri', 'uID', 'uID').series

    # print('Calculate Longest Axis Length of Tessellation \n')
    tessellation['sdcLAL'] = mm.LongestAxisLength(tessellation).series

    # print('Calculate Tessellation Area\n')
    tessellation['sdcAre'] = mm.Area(tessellation).series

    # print('Calculate Tessellation Circular Compactness \n')
    tessellation['sscCCo'] = mm.CircularCompactness(tessellation, 'sdcAre').series

    # print('Calculate Tessellation Equivalent Rectangular Index \n')
    tessellation['sscERI'] = mm.EquivalentRectangularIndex(tessellation, 'sdcAre').series
    
    # print('Calculate Shared Walls Ratio \n')
    buildings["mtbSWR"] = mm.SharedWallsRatio(buildings, "sdbPer").series

    # print('Calculate weights \n')
    # A spatial weights matrix is a representation of the spatial structure of your data. 
    # It is a quantification of the spatial relationships that exist among the features in your dataset. 
    # (or, at least, a quantification of the way you conceptualize those relationships)
    # Changed 15.05 by LB from 'queen_1 = libpysal.weights.contiguity.Queen.from_dataframe(tessellation, ids="uID", silence_warnings=True)'
    # Suspect the original code didn't get the right input for ids (list required) which was causing issues down the line.
    # https://pysal.org/libpysal/_modules/libpysal/weights/contiguity.html#Queen.from_dataframe
    queen_1 = libpysal.weights.contiguity.Queen.from_dataframe(tessellation, ids=list(tessellation["uID"]), silence_warnings=True)

    # print('Calculate Building Alignment')    
    # buildings["mtbAli"] = mm.Alignment(buildings, queen_1, "uID", "stbOri", verbose=False).series

    # print('Calculate Distance to Neighbouring Buildings')
    # buildings["mtbNDi"] = mm.NeighborDistance(buildings, queen_1, "uID", verbose=False).series

    # print('Calculate Neighbors \n')
    # Calculates number of Neighbors, relative to the perimeter of the tesselation object
    tessellation["mtcWNe"] = mm.Neighbors(tessellation, queen_1, "uID", weighted=True, verbose=False).series

    # print('Calculate Covered Area \n')
    tessellation["mdcAre"] = mm.CoveredArea(tessellation, queen_1, "uID", verbose=False).series


    # Calculate weights for buildings
    buildings_q1 = libpysal.weights.contiguity.Queen.from_dataframe(buildings, silence_warnings=True)

    blocks["ldkAre"] = mm.Area(blocks).series
    blocks["ldkPer"] = mm.Perimeter(blocks).series
    blocks["lskCCo"] = mm.CircularCompactness(blocks, "ldkAre").series
    blocks["lskERI"] = mm.EquivalentRectangularIndex(blocks, "ldkAre", "ldkPer").series
    blocks["lskCWA"] = mm.CompactnessWeightedAxis(blocks, "ldkAre", "ldkPer").series
    blocks["ltkOri"] = mm.Orientation(blocks, verbose=False).series

    blo_q1 = libpysal.weights.contiguity.Queen.from_dataframe(blocks, ids="bID", silence_warnings=True)

    blocks["ltkWNB"] = mm.Neighbors(blocks, blo_q1, "bID", weighted=True, verbose=False).series
    blocks["likWBB"] = mm.Count(blocks, buildings, "bID", "bID", weighted=True).series

    queen_3 = mm.sw_high(k=3, weights=queen_1)
    # buildings['ltbIBD'] = mm.MeanInterbuildingDistance(buildings, queen_1, 'uID', queen_3, verbose=False).series
    # buildings['ltcBuA'] = mm.BuildingAdjacency(buildings, queen_3, 'uID', buildings_q1, verbose=False).series

    tessellation = tessellation.merge(buildings[['floor_area', 'uID']], on='uID', how='left')
    # print('Calculate Tessellation Density \n')
    tessellation['licGDe'] = mm.Density(tessellation, 'floor_area', queen_3, 'uID', 'sdcAre', verbose=False).series
    tessellation = tessellation.drop(columns='floor_area')

    # print('Calculate Tessellation Block Count \n')
    tessellation['ltcWRB'] = mm.BlocksCount(tessellation, 'bID', queen_3, 'uID', verbose=False).series

    # print('Calculate Tessellation Area Ratio\n')
    tessellation['sicCAR'] = mm.AreaRatio(tessellation, buildings, 'sdcAre', 'sdbAre', 'uID').series
    tessellation['sicFAR'] = mm.AreaRatio(tessellation, buildings, 'sdcAre', 'floor_area', 'uID').series

 
    streets["sdsLen"] = mm.Perimeter(streets).series
    tessellation["stcSAl"] = mm.StreetAlignment(tessellation, streets, "stcOri", "nID").series
    buildings["stbSAl"] = mm.StreetAlignment(buildings, streets, "stbOri", "nID").series

    profile = mm.StreetProfile(streets, buildings, heights='height_70p', distance=3)
    streets["sdsSPW"] = profile.w
    streets["sdsSPH"] = profile.h
    streets["sdsSPR"] = profile.p
    streets["sdsSPO"] = profile.o
    streets["sdsSWD"] = profile.wd
    streets["sdsSHD"] = profile.hd

    streets["sssLin"] = mm.Linearity(streets).series
    streets["sdsAre"] = mm.Reached(streets, tessellation, "nID", "nID", mode="sum", values="sdcAre", verbose=False).series
    streets["sisBpM"] = mm.Count(streets, buildings, "nID", "nID", weighted=True).series

    str_q1 = libpysal.weights.contiguity.Queen.from_dataframe(streets, silence_warnings=True)
 
    streets["misRea"] = mm.Reached(
        streets, tessellation, "nID", "nID", spatial_weights=str_q1, mode="count", verbose=False
    ).series
    streets["mdsAre"] = mm.Reached(streets, tessellation, "nID", "nID", spatial_weights=str_q1,
                                mode="sum", verbose=False).series
    
    graph = mm.gdf_to_nx(streets)
    graph = mm.node_degree(graph)
    graph = mm.subgraph(
        graph,
        radius=5,
        meshedness=True,
        cds_length=False,
        mode="sum",
        degree="degree",
        length="mm_len",
        mean_node_degree=False,
        proportion={0: True, 3: True, 4: True},
        cyclomatic=False,
        edge_node_ratio=False,
        gamma=False,
        local_closeness=True,
        closeness_weight="mm_len", 
        verbose=False
    )
    graph = mm.cds_length(graph, radius=3, name="ldsCDL", verbose=False)
    graph = mm.clustering(graph, name="xcnSCl")
    graph = mm.mean_node_dist(graph, name="mtdMDi", verbose=False)

    nodes, edges, sw = mm.nx_to_gdf(graph, spatial_weights=True)

    edges_w3 = mm.sw_high(k=3, gdf=edges)
    edges["ldsMSL"] = mm.SegmentsLength(edges, spatial_weights=edges_w3, mean=True, verbose=False).series

    edges["ldsRea"] = mm.Reached(edges, tessellation, "nID", "nID", spatial_weights=edges_w3, verbose=False).series
    edges["ldsRea"] = mm.Reached(
        edges, tessellation, "nID", "nID", spatial_weights=edges_w3, mode="sum", values="sdcAre", verbose=False
    ).series

    nodes_w5 = mm.sw_high(k=5, weights=sw)
    nodes["lddNDe"] = mm.NodeDensity(nodes, edges, nodes_w5, verbose=False).series
    nodes["linWID"] = mm.NodeDensity(
        nodes, edges, nodes_w5, weighted=True, node_degree="degree", verbose=False
    ).series

    buildings["nodeID"] = mm.get_node_id(buildings, nodes, edges, "nodeID", "nID", verbose=False)
    tessellation = tessellation.merge(buildings[["uID", "nodeID"]], on="uID", how="left")

    nodes_w3 = mm.sw_high(k=3, weights=sw)

    nodes["lddRea"] = mm.Reached(nodes, tessellation, "nodeID", "nodeID", nodes_w3, verbose=False).series
    nodes["lddARe"] = mm.Reached(
        nodes, tessellation, "nodeID", "nodeID", nodes_w3, mode="sum", values="sdcAre", verbose=False
    ).series

    nodes["sddAre"] = mm.Reached(
        nodes, tessellation, "nodeID", "nodeID", mode="sum", values="sdcAre", verbose=False
    ).series
    nodes["midRea"] = mm.Reached(nodes, tessellation, "nodeID", "nodeID", spatial_weights=sw, verbose=False).series
    nodes["midAre"] = mm.Reached(
        nodes, tessellation, "nodeID", "nodeID", spatial_weights=sw, mode="sum", values="sdcAre", verbose=False
    ).series

    nodes.rename(
        columns={
            "degree": "mtdDeg",
            "meshedness": "lcdMes",
            "local_closeness": "lcnClo",
            "proportion_3": "linP3W",
            "proportion_4": "linP4W",
            "proportion_0": "linPDE",
        }, inplace=True
    )
    spatial_weights = queen_3

    print('Calculation done!')
    return (tessellation, edges, nodes, spatial_weights)
