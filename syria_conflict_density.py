# Syria Oct - Dec 2013 conflict query
import gdelt2pg
import datetime

sdate = datetime.date(2013, 10, 1)
edate = datetime.date(2013, 12, 31)

# download GDELT extract
gdelt2pg.batch_download(sdate, edate)

# create DB gdelt
# in postgres, run: create database gdelt

# import all events in Syria
gdelt2pg.batch_import('syria', sdate, edate, GDELT['syriaQuery'])

# import polygon grid of syria (1/16 degree square) as syria_0625
# shp2pgsql -s 4326 -g geom -I <file.shp> public.syria_0625 | psql -d <'gdelt'>

# subset syria to material conflict (quadclass = '4', Material Conflict),
    # w/ high geographic precision (actiongeo_type='4', WORLDCITY),
    # occuring between October 1, 2013 and December 31, 2013
    # in postgres, run:
    # CREATE TABLE syria_conflict AS(
    #     SELECT * FROM syria WHERE quadclass = '4' AND actiongeo_type = '4' AND sqldate BETWEEN '2013-10-01' AND '2013-12-31'
    # )

# build binned density map
    # this might take a while (15+ minutes)
gdelt2pg.binned_density_map('syria_conflict', 'syria_1_16', sdate, edate, t_interval = 'WEEKLY')

# and for aleppo
gdelt2pg.binned_density_map('aleppo_conflict', 'aleppo_1_128', sdate, edate, t_interval = 'WEEKLY')
