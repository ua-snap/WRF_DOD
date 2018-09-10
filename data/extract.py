def return_min_max( fn, lon, lat, variable ):
    ''' return min/max WRF output T2 data '''
    print('working with {} starting'.format( fn ))
    ds = xr.open_dataset( fn, autoclose=True )
    da = ds[ variable ] # get the array as it is easier to process on

    # # try to slice it...
    # da_pt = da[:,lat,lon]
    da_pt2 = da.where( (da.lat == lat) & (da.lon == lon), drop=True ).squeeze()

    #fill the dataframe with min and max value with kelvin to C conversion
    day_min = da_pt.resample(time='D').min() - 273.15
    day_max = da_pt.resample(time='D').max() - 273.15

    df = pd.DataFrame( index=day_min.time )

    df['min'] = np.array(day_min)
    df['max'] = day_max

    # return dataframe rounded at 2 decimals --> SNAP Temperature standard
    return df.round( 2 )

def return_val( fn, row, col, variable ):
    ''' return min/max WRF output T2 data '''
    print('working with {} starting'.format( fn ))
    ds = xr.open_dataset( fn, autoclose=True )
    da = ds[ variable ] # get the array as it is easier to process on

    # try to slice it...
    da_pt = da[:,row, col]

    # day_min = da_pt.resample(time='D').min() - 273.15
    df = pd.DataFrame( {'mean':da_pt}, index=da_pt.time )

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
    nearest = nearest_points(pt, pts)
    return nearest.x, nearest.y

if __name__ == '__main__':
    import xarray as xr
    import os, glob
    import numpy as np
    import pandas as pd
    from multiprocessing import Pool
    from functools import partial
    import warnings
    from affine import Affine
    import geopandas as gpd
    from shapely.geometry import Point
    import rasterio

    warnings.filterwarnings( 'ignore' ) # so we dont have to look at xarray warnings about pd.TimeGrouper

    location = {
            'Fairbanks' : ( -147.716, 64.8378 ),
            'Greely' : ( -145.6076, 63.8858 ),
            'Whitehorse' : ( -135.074, 60.727 ),
            'Coldfoot' : ( -150.1772 , 67.2524 )
            }

    # location = {
    #     'Fairbanks' : ( 64.83, -147.71 ),
    #     'Greely' : ( 63.8858, -145.6076 ),
    #     'Whitehorse' : ( 60.7212, -135.0568  ),
    #     'Coldfoot' : ( 67.2524, -150.1772  )
    #     }

    # reproject the points to the 3338...
    geom = [Point(j) for i,j in location.items()]
    df = pd.DataFrame(location).T
    df.columns = ['lon', 'lat']
    df['geometry'] = geom
    pts_proj = gpd.GeoDataFrame(df, crs={'init':'epsg:4326'}).to_crs( '+proj=stere +lat_0=90 +lat_ts=90 +lon_0=-150 +k=0.994 +x_0=2000000 +y_0=2000000 +datum=WGS84 +units=m +no_defs' )
    pts_proj['lat'] = pts_proj.geometry.apply( lambda x: x.y )
    pts_proj['lon'] = pts_proj.geometry.apply( lambda x: x.x )

    location = {}
    for j,i in pts_proj.iterrows():
        location[j] = (i['lon'], i['lat'])

    # global setup arguments
    variable = 'T2'

    path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data/hourly_fix/{}'.format( variable.lower() )
    wildcard = '*GFDL*.nc'

    for k, v in location.items() :

        out_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/app_data_extraction/WRF_extract_GFDL_1970-2100_{}.csv'.format( k )
        # lon,lat = v
        
        # list (and filter) historicals
        wildcard = '*GFDL*historical*.nc'
        historicals = sorted( glob.glob( os.path.join( path, wildcard ) ) )
        historicals = list(filter(lambda x: '_2006' not in x, historicals ))
    

        ds = xr.open_dataset( historicals[0] )

        # list futures 
        wildcard = '*GFDL*rcp85*.nc'
        futures = sorted( glob.glob( os.path.join( path, wildcard ) ) )

        files = historicals + futures

        # grab lat/lon from a single file
        tmp_ds = xr.open_dataset( files[0], autoclose=True )
        # transform = rasterio.transform.from_origin( ds.xc.data.min()-(res/2.), ds.yc.data.max()+(res/2.), res, res )
        # a2 = Affine( 20000, 0, tmp_ds.yc.min().data-10000, 0, -20000, tmp_ds.xc.max().data-10000 )
        res = 20000
        # a = rasterio.transform.from_origin( ds.xc.data.min()-(res/2.), ds.yc.data.max()+(res/2.), res, res )
        a = rasterio.transform.from_origin( ds.yc.data.max()+(res/2.), ds.xc.data.min()-(res/2.), res, res )
        x,y = v
        col, row = ~a * (x, y)
        col, row = [ np.round(int(i),0) for i in [col,row] ]
        tmp_ds.close()
        tmp_ds = None
        

        # # # # # TEST
        # gt = a.to_gdal()
        # # col, row to x, y
        # x = (col * gt[1]) + gt[0]
        # y = (row * gt[5]) + gt[3]

        # # x,y to col,row
        # col = int((x - gt[0]) / gt[1]) 
        # row = int((y - gt[3]) / gt[5])

        # # # # # # # #

        ds = ds.isel(time=slice(0,1)).copy()
        ds.t2.data[:,col,row] = -5555
        ds.xc.data

        xc, yc, data = np.meshgrid( ds.xc.data, ds.yc.data )

        pt_df = gpd.GeoDataFrame([ {'xc':x,'yc':y, 'data':z, 'geometry':Point(x,y)} for x,y,z in zip( xc.ravel(), yc.ravel(), ds.t2.data[0].ravel() ) ], crs=ds.proj_parameters )
        pt_df.to_file( 'TEST_POINT_DATA_WRF.shp' )

        transform = rasterio.transform.from_origin( ds.xc.data.min()-(res/2.), ds.yc.data.max()+(res/2.), res, res )
        res = 20000
        meta = {'compress':'lzw', 
                'count':1,
                'crs':ds.proj_parameters,
                'height':ds.t2.data.shape[1],
                'width':ds.t2.data.shape[2],
                'transform':transform,
                'dtype':'float32',
                'driver':'GTiff' }

        with rasterio.open( 'TEST_RASTER_POINT_WRF3.tif', 'w', **meta ) as out:
            # out.write( ds.t2.data[0].astype(np.float32), 1 )
            out.write( np.nan_to_num(da_pt.data[0].astype(np.float32)), 1 )

        break
        # lon, lat = closest_point( lon, lat, tmp_ds ) # update to the closest

        # run in parallel
        pool = Pool( 32 )
        func = partial( return_val, row=row, col=col, variable=variable.lower() )
        ls_df = pool.map( func, files )
        pool.close()
        pool.join()

        # concat and write to disk
        df = pd.concat( ls_df )
        df = df.sort_index()
        df.to_csv( out_fn )
