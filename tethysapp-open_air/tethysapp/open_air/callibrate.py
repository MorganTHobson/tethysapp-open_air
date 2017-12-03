import pandas as pd

def callibrate_o3(o3df):
    # read in metadata 
    ## CHANGE ADDRESS` 
    meta = pd.read_csv('~/bmoreopen_data/data.csv').set_index('id').drop(28)
    # we updated code on Sept 25, but didn't update all monitors

    # find the ids of  monitors deployed before Sept. 25, divide by 100
    old_ids = meta[(pd.to_datetime(meta.install_time) < '2017-09-25 13:45:00')].index
    # find the monitors deployed after (+ Turner Station, 40, 32, 39), don't 
    updated_ids = meta[(pd.to_datetime(meta.install_time) > '2017-09-25 13:45:00')].index.append(meta[meta.notes.str.contains('Larry')==True].index)
    
    #Resample dataframe from 15 min to hour
    o3df_cal = o3df.resample('H').mean().copy()
    #arbitrary threshold for hourly std above which we throw out data
    o3_thresh = 100

   # loop through the ids and find the callibration codes, m_o3 
    for sel_id in old_ids: 
        try: 
            m_o3 = float(meta.loc[sel_id]['qr_ozone'].dropna().values[0][28:])
        except AttributeError: 
            m_o3 = float(meta.loc[sel_id]['qr_ozone'][28:])
	# now that we have the callibration codes, m_o3
	# We resample the data taking the mean of each hour, and only select data where the standard deviation in an hour is less than the o3_thresh defined above
        o3df_cal[sel_id] = (o3df[sel_id].resample('H').mean()[(o3df[sel_id].resample('H').std()<o3_thresh)])/m_o3
# same thing, but for the updated ids 
    for sel_id in updated_ids : 
        try: 
            m_o3 = float(meta.loc[sel_id]['qr_ozone'].dropna().values[0][28:])
        except AttributeError: 
            m_o3 = float(meta.loc[sel_id]['qr_ozone'][28:])
        o3df_cal[sel_id] = (o3df[sel_id].resample('H').mean()[(o3df[sel_id].resample('H').std()<o3_thresh)])/m_o3
    
    # remove columns with all nans   
    o3df_cal = o3df_cal.dropna(axis=1, how = 'all')
    # remove data points outside of -300,300
    o3df_cal[np.abs(o3df_cal) > 300] = np.nan
    # subtract 6am data (ie, assume that 6am data is ~0ppb)
    o3df_cal = o3df_cal.subtract(o3df_cal[o3df_cal.index.hour ==6].mean())
    # remove data sets that don't have standard deviations above 6 ppb 
    # (we could be off by factor of 2, so keeping these )
# some of these are just returning 0s, so we don't care about these
    o3df_cal = o3df_cal.drop(o3df_cal.std()[o3df_cal.std()<6].index, axis=1)

