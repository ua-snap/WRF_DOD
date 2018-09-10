# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# pickle the data
# Michael Lindgren -- Sept 2018
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

import pickle, glob, os
import pandas as pd

files = glob.glob('/workspace/Shared/Tech_Projects/DOD_Ft_Wainwright/project_data/wrf_data_app/*.csv')
frames = { os.path.basename(fn).split('.')[0].split('_')[-1]:pd.read_csv(fn, index_col=0, parse_dates=True ) for fn in files }
output_filename = '/workspace/Shared/Tech_Projects/DOD_Ft_Wainwright/project_data/wrf_data_app/WRF_extract_GFDL_1970-2100_multiloc_dod.p'

with open( output_filename, 'wb' ) as handle:
    pickle.dump( frames, handle, protocol=pickle.HIGHEST_PROTOCOL )




