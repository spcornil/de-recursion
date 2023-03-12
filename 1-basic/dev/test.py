

def get_data():
    import json
    import pandas as pd
    import requests as rq
    from datetime import datetime as dt

    url = 'https://opensky-network.org/api/states/all'
    response = rq.get(url)
    data = response.json()
    df = pd.json_normalize(data, 'states')
    df.columns = ['icao24'
                 ,'callsign'
                 ,'origin_country'
                 ,'time_position'
                 ,'last_contact'
                 ,'longitude'
                 ,'latitude'
                 ,'baro_altitude'
                 ,'on_ground'
                 ,'velocity'
                 ,'true_track'
                 ,'vertical_rate'
                 ,'sensors'
                 ,'geo_altitude'
                 ,'squawk'
                 ,'spi'
                 ,'position_source'
                 ]
    df['time_position'] = df['time_position'].astype("datetime64[s]")
    df['last_contact'] = df['last_contact'].astype("datetime64[s]")
    df['callsign'] = df['callsign'].str.replace(' ','')
    timestamp = dt.now().strftime("%Y-%m-%d-%H%M%S")
    df.to_csv(f'/home/sean.cornillie/de-recursion/1-basic/data/airtraffic_{timestamp}.csv', index=False)

get_data()