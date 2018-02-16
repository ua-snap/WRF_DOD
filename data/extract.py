def return_min_max( fn, lat, lon, variable ):
    ''' return min/max WRF output T2 data '''
    print('working with {} starting'.format( fn ))
    ds = xr.open_dataset( fn, autoclose=True )
    da = ds[ variable ] # get the array as it is easier to process on

    # try to slice it...
    da_pt = da.where((da.lat == lat) )

    #fill the dataframe with min and max value with kelvin to C conversion
    day_min = da_pt.resample(time='D').min() - 273.15
    day_max = da_pt.resample(time='D').max() - 273.15

    df = pd.DataFrame( index=day_min.time )

    df['min'] = day_min
    df['max'] = day_max

    # return dataframe rounded at 2 decimals --> SNAP Temperature standard
    return df.round( 2 )

def closest_point( lon, lat, da ):
    ''' the spatial way to do the lookup aka the RIGHT WAY  :)'''
    from shapely.geometry import Point
    from shapely.ops import nearest_points, unary_union

    lons = da.lon.data.ravel()
    lats = da.lat.data.ravel()

    pts = unary_union([ Point(*i) for i in zip(lons, lats) ])
    pt = Point(lon, lat)

    # get the nearest point
    nearest = nearest_points(pt, pts)[1]
    return nearest.x, nearest.y

if __name__ == '__main__':
    import xarray as xr
    import os, glob
    import numpy as np
    import pandas as pd
    from multiprocessing import Pool
    from functools import partial
    import warnings

    warnings.filterwarnings( 'ignore' ) # so we dont have to look at xarray warnings about pd.TimeGrouper

    location = {
            'Fairbanks' : ( -147.71, 64.83 ),
            'Greely' : ( -145.6076, 63.8858 ),
            'Whitehorse' : ( -135.0568 , 60.7212 ),
            'Coldfoot' : ( -150.1772 , 67.2524 )
            }
    # global setup arguments
    variable = 'T2'

    path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/hourly/{}'.format( variable.lower() )
    wildcard = '*gfdl*.nc'

    for k, v in location.items() :

        out_fn = '/workspace/Shared/Users/jschroder/TMP/WRF_extract_GFDL_1970-2100_{}v3.csv'.format(k)
        lat,lon = v
        # list the data we want to extract from
        files = sorted( glob.glob( os.path.join( path, wildcard ) ) )

        # grab lat/lon from a single file
        tmp_ds = xr.open_dataset( files[0], autoclose=True )
        lon, lat = closest_point( lon, lat, tmp_ds ) # update to the closest
        # close and cleanup -- the hard way to avoid memory leaks
        tmp_ds.close()
        tmp_ds = None

        # run in parallel
        pool = Pool( 32 )
        func = partial( return_min_max, lat=lat, lon=lon, variable=variable )
        ls_df = pool.map( func, files )
        pool.close()
        pool.join()

        # concat and write to disk
        df = pd.concat( ls_df )
        df = df.sort_index()
        df.to_csv( out_fn )
