def return_min_max( fn, row, col, variable ):
    ''' return min/max WRF output T2 data '''
    print('working with {} starting'.format( fn ))
    ds = xr.open_dataset( fn, autoclose=True )
    da = ds[ variable ] # get the array as it is easier to process on

    # try to slice it...
    da_pt = da[:,row, col]

    #fill the dataframe with min and max value with kelvin to C conversion
    day_min = da_pt.resample(time='D').min() - 273.15
    day_max = da_pt.resample(time='D').max() - 273.15

    df = pd.DataFrame( index=day_min.time )

    df['min'] = day_min
    df['max'] = day_max

    # return dataframe rounded at 2 decimals --> SNAP Temperature standard
    return df.round( 2 )

def reproject_wgs84_to_wrf( x,y ):
    ''' simple wrapper around pyproj.transform to project the coords between wgs84 latlong and WRF polar grid'''
    import pyproj
    wrf = pyproj.Proj( '+units=m +proj=stere +lat_ts=64.0 +lon_0=-152.0 +lat_0=90.0 +x_0=0 +y_0=0 +a=6370000 +b=6370000 ' )
    wgs = pyproj.Proj( '+units=m +datum=WGS84 +proj=latlong ' )
    return pyproj.transform( wgs, wrf, np.array(x), np.array(y) )

def affine_from_wrfds( fn ):
    ''' make an affine transform from a template wrf file... '''
    ds = xr.open_dataset( fn, autoclose=True )
    res = 20000
    x0,y0 = np.array( ds.xc.min()-(res/2.)), np.array(ds.yc.max()+(res/2.) )
    ds.close()
    ds = None
    return rasterio.transform.from_origin( x0, y0, res, res )

def rasterize_shapes( shapes, arr, transform, fill=0, all_touched=False, default_value=1, dtype='float32' ):
    from rasterio.features import rasterize
    return rasterize( geoms, out_shape=arr.shape, fill=fill, out=None, 
                        transform=transform, all_touched=all_touched, 
                        default_value=default_value, dtype=dtype )


if __name__ == '__main__':
    import xarray as xr
    import os, glob, rasterio
    import numpy as np
    import pandas as pd
    from multiprocessing import Pool
    from functools import partial
    import warnings
    from shapely.geometry import Point
    import geopandas as gpd

    # global setup arguments
    variable = 'T2'

    path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data/hourly_fix/{}'.format( variable.lower() )
    output_path = '/workspace/Shared/Tech_Projects/DOD_Ft_Wainwright/project_data/wrf_data_app'
    warnings.filterwarnings( 'ignore' ) # so we dont have to look at xarray warnings about pd.TimeGrouper

    location = {
            'Fairbanks' : ( -147.716, 64.8378 ),
            'Greely' : ( -145.6076, 63.8858 ),
            'Whitehorse' : ( -135.074, 60.727 ),
            'Coldfoot' : ( -150.1772 , 67.2524 )
            }

    # reproject the points to the 3338...
    location = { i:Point(j) for i,j in location.items() }
    df = pd.Series( location ).to_frame( 'geometry' )
    wrf_crs = '+units=m +proj=stere +lat_ts=64.0 +lon_0=-152.0 +lat_0=90.0 +x_0=0 +y_0=0 +a=6370000 +b=6370000 '
    pts_proj = gpd.GeoDataFrame(df, crs={'init':'epsg:4326'}).to_crs( wrf_crs )

    # list (and filter) historicals
    wildcard = '*GFDL*historical*.nc'
    historicals = sorted( glob.glob( os.path.join( path, wildcard ) ) )
    historicals = list(filter(lambda x: '_2006' not in x, historicals ))

    # list futures 
    wildcard = '*GFDL*rcp85*.nc'
    futures = sorted( glob.glob( os.path.join( path, wildcard ) ) )

    # append 'em
    files = historicals + futures
    
    # get an affine transform to make the lookups faster
    a = affine_from_wrfds( files[0] )

    # loop through the locations for extraction
    for k, pt in pts_proj.geometry.to_dict().items():
        print( k )
        out_fn = os.path.join( output_path , '{}_daily_WRF_extract_GFDL_1970-2100_{}.csv'.format( variable, k ) )
        
        # get row/col from x/y using affine
        col, row = ~a * (pt.x, pt.y)
        col, row = [ int(i) for i in [col, row] ]

        # run in parallel
        pool = Pool( 64 )
        func = partial( return_min_max, row=row, col=col, variable=variable.lower() )
        ls_df = pool.map( func, files )
        pool.close()
        pool.join()

        # concat and write to disk
        df = pd.concat( ls_df )
        df = df.sort_index()
        df.to_csv( out_fn )