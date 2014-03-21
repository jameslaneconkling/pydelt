# Required: pygexf, dateutil
import sys
import os
import zipfile
import psycopg2
import urllib2
import datetime

# third party modules
import gexf
from dateutil.relativedelta import relativedelta


GDELT = {
    'gdeltSchema': """(
         GLOBALEVENTID bigint ,
         SQLDATE date ,
         MonthYear varchar(6) ,
         Year varchar(4) ,
         FractionDate real ,
         Actor1Code varchar(20) , --char(3)
         Actor1Name varchar(255) ,
         Actor1CountryCode varchar(3) ,
         Actor1KnownGroupCode varchar(3) ,
         Actor1EthnicCode varchar(3) ,
         Actor1Religion1Code varchar(3) ,
         Actor1Religion2Code varchar(3) ,
         Actor1Type1Code varchar(3) ,
         Actor1Type2Code varchar(3) ,
         Actor1Type3Code varchar(3) ,
         Actor2Code varchar(20) , --char(3)
         Actor2Name varchar(255) ,
         Actor2CountryCode varchar(3) ,
         Actor2KnownGroupCode varchar(3) ,
         Actor2EthnicCode varchar(3) ,
         Actor2Religion1Code varchar(3) ,
         Actor2Religion2Code varchar(3) ,
         Actor2Type1Code varchar(3) ,
         Actor2Type2Code varchar(3) ,
         Actor2Type3Code varchar(3) ,
         IsRootEvent int ,
         EventCode varchar(4) ,
         EventBaseCode varchar(4) ,
         EventRootCode varchar(4) ,
         QuadClass int ,
         GoldsteinScale real ,
         NumMentions int ,
         NumSources int ,
         NumArticles int ,
         AvgTone real ,
         Actor1Geo_Type varchar(20) , --int
         Actor1Geo_FullName varchar(255) ,
         Actor1Geo_CountryCode varchar(2) ,
         Actor1Geo_ADM1Code varchar(4) ,
         Actor1Geo_Lat float ,
         Actor1Geo_Long float ,
         Actor1Geo_FeatureID varchar(20) , --int
         Actor2Geo_Type varchar(20) , --int
         Actor2Geo_FullName varchar(255) ,
         Actor2Geo_CountryCode varchar(2) ,
         Actor2Geo_ADM1Code varchar(4) ,
         Actor2Geo_Lat float ,
         Actor2Geo_Long float ,
         Actor2Geo_FeatureID varchar(20) , --int
         ActionGeo_Type varchar(20) , --int
         ActionGeo_FullName varchar(255) ,
         ActionGeo_CountryCode varchar(2) ,
         ActionGeo_ADM1Code varchar(4) ,
         ActionGeo_Lat float ,
         ActionGeo_Long float ,
         ActionGeo_FeatureID varchar(20) , --int
         DATEADDED int
    )""",

    'timelineSchema': """(
        sqldate date,
        count int,
        geo_conflict_count int,
        vcoop_count int,
        mcoop_count int,
        vconf_count int,
        mconf_count int,
        goldstein_avg int,
        goldstein_dev int,
        gs_10 int,
        gs_9 int,
        gs_8 int,
        gs_7 int,
        gs_6 int,
        gs_5 int,
        gs_4 int,
        gs_3 int,
        gs_2 int,
        gs_1 int,
        gs_0 int,
        gs_n1 int,
        gs_n2 int,
        gs_n3 int,
        gs_n4 int,
        gs_n5 int,
        gs_n6 int,
        gs_n7 int,
        gs_n8 int,
        gs_n9 int,
        gs_n10 int,
        tone_avg int,
        tone_dev int
    )""",

    'greatlakesQuery': """
        SELECT * FROM gdelt_temp
        WHERE ActionGeo_CountryCode IN ('BY', 'CG', 'KE', 'RW', 'TZ', 'UG')
    """,

    'drcConflictGeoPrecision': """
        SELECT * FROM gdelt_temp
        WHERE quadclass IN ('3', '4')
        AND actiongeo_type IN ('3', '4')
        AND (actiongeo_long > 11.5 AND actiongeo_long < 31.0 AND actiongeo_lat > -14.0 AND actiongeo_lat < 6.3);
    """,

    'timelineQuery': """
        SELECT sqldate,
        count(*) AS count,
        count(CASE WHEN quadclass IN ('3', '4') AND actiongeo_type IN ('3', '4') THEN 1 ELSE NULL END) AS geo_conflict_count,
        count(CASE WHEN quadclass = '1' THEN 1 ELSE NULL END) AS vcoop_count,
        count(CASE WHEN quadclass = '2' THEN 1 ELSE NULL END) AS mcoop_count,
        count(CASE WHEN quadclass = '3' THEN 1 ELSE NULL END) AS vconf_count,
        count(CASE WHEN quadclass = '4' THEN 1 ELSE NULL END) AS mconf_count,
        avg(goldsteinscale) AS goldstein_avg,
        stddev(goldsteinscale) AS goldstein_dev,
        count(CASE WHEN goldsteinscale = 10.0 THEN 1 ELSE NULL END) AS gs_10,
        count(CASE WHEN goldsteinscale < 10.0 AND goldsteinscale >= 9.0 THEN 1 ELSE NULL END) AS gs_9,
        count(CASE WHEN goldsteinscale < 9.0 AND goldsteinscale >= 8.0 THEN 1 ELSE NULL END) AS gs_8,
        count(CASE WHEN goldsteinscale < 8.0 AND goldsteinscale >= 7.0 THEN 1 ELSE NULL END) AS gs_7,
        count(CASE WHEN goldsteinscale < 7.0 AND goldsteinscale >= 6.0 THEN 1 ELSE NULL END) AS gs_6,
        count(CASE WHEN goldsteinscale < 6.0 AND goldsteinscale >= 5.0 THEN 1 ELSE NULL END) AS gs_5,
        count(CASE WHEN goldsteinscale < 5.0 AND goldsteinscale >= 4.0 THEN 1 ELSE NULL END) AS gs_4,
        count(CASE WHEN goldsteinscale < 4.0 AND goldsteinscale >= 3.0 THEN 1 ELSE NULL END) AS gs_3,
        count(CASE WHEN goldsteinscale < 3.0 AND goldsteinscale >= 2.0 THEN 1 ELSE NULL END) AS gs_2,
        count(CASE WHEN goldsteinscale < 2.0 AND goldsteinscale >= 1.0 THEN 1 ELSE NULL END) AS gs_1,
        count(CASE WHEN goldsteinscale < 1.0 AND goldsteinscale >= -1.0 THEN 1 ELSE NULL END) AS gs_0,
        count(CASE WHEN goldsteinscale < -1.0 AND goldsteinscale >= -2.0 THEN 1 ELSE NULL END) AS gs_n1,
        count(CASE WHEN goldsteinscale < -2.0 AND goldsteinscale >= -3.0 THEN 1 ELSE NULL END) AS gs_n2,
        count(CASE WHEN goldsteinscale < -3.0 AND goldsteinscale >= -4.0 THEN 1 ELSE NULL END) AS gs_n3,
        count(CASE WHEN goldsteinscale < -4.0 AND goldsteinscale >= -5.0 THEN 1 ELSE NULL END) AS gs_n4,
        count(CASE WHEN goldsteinscale < -5.0 AND goldsteinscale >= -6.0 THEN 1 ELSE NULL END) AS gs_n5,
        count(CASE WHEN goldsteinscale < -6.0 AND goldsteinscale >= -7.0 THEN 1 ELSE NULL END) AS gs_n6,
        count(CASE WHEN goldsteinscale < -7.0 AND goldsteinscale >= -8.0 THEN 1 ELSE NULL END) AS gs_n7,
        count(CASE WHEN goldsteinscale < -8.0 AND goldsteinscale >= -9.0 THEN 1 ELSE NULL END) AS gs_n8,
        count(CASE WHEN goldsteinscale < -9.0 AND goldsteinscale >= -10.0 THEN 1 ELSE NULL END) AS gs_n9,
        count(CASE WHEN goldsteinscale = -10.0 THEN 1 ELSE NULL END) AS gs_n10,
        avg(avgtone) AS tone_avg,
        stddev(avgtone) AS tone_dev
        FROM gdelt_temp
        GROUP BY sqldate ORDER BY sqldate;
    """
}

##############################################
########## Postgres Read/Write ###############
##############################################

def gdelt_datelist(startdate, enddate):
    if not isinstance(startdate, datetime.date) or not isinstance(enddate, datetime.date):
        sys.exit("Start date and end date must be of type datetime.date")
    if startdate > enddate:
        sys.exit("Start date cannot be greater than end date")
    if startdate == enddate:
        sys.exit("Start date and end date cannot be equal")
    if startdate < datetime.date(1979, 1, 1):
        sys.exit("Start date cannot be earlier than 1979-01-01")
    if enddate > datetime.date.today():
        sys.exit("End date cannot be later than today: " + str(datetime.today()))

    dates = []
    if startdate < datetime.date(2006, 1, 1):
        # round startdate down to the nearest year
        # and enddate down to nearest year (that is below 2005-01-01)
        sdate = startdate.replace(month = 1, day = 1)
        edate = min(enddate.replace(month = 1, day = 1), datetime.date(2005, 1, 1))

        # calculate number of years to enddate
        year_delta = relativedelta(edate, sdate).years + 1
        for year in range(0, year_delta):
            date = sdate + relativedelta(years = year)
            date = str(date.year)
            dates.append(date)

    if startdate < datetime.date(2013, 1, 1) or enddate >= datetime.date(2006, 1, 1):
        # round startdate down to nearest month (that is above 2006-01-01)
        # and enddate down to the nearest month (that is below 2013-12-01)
        sdate = max(startdate.replace(day = 1), datetime.date(2006, 01, 01))
        edate = min(enddate.replace(day = 1), datetime.date(2012, 12, 1))

        # calculate number of months to enddate
        month_delta = relativedelta(edate, sdate).years * 12 + relativedelta(edate, sdate).months + 1
        for month in range(0, month_delta):
            date = sdate + relativedelta(months = month)
            date = str(date.year) + '{0:02d}'.format(date.month)
            dates.append(date)

    if enddate >= datetime.date(2013, 1, 1):
        # no rounding, but ensure startdate is >= 2013-12-01
        sdate = max(startdate, datetime.date(2013, 1, 1))

        # calculate number of days to enddate
        day_delta = (enddate - sdate).days + 1
        for day in range(0, day_delta):
            date = sdate + relativedelta(days = day)
            date = str(date.year) + '{0:02d}'.format(date.month) + '{0:02d}'.format(date.day)
            dates.append(date)

    return dates


def batch_download(
        startdate, enddate, importpath='/Users/jamesconkling/Documents/Projects/gdelt/export/'):
    url = 'http://gdelt.utdallas.edu/data/backfiles/'
    dates = gdelt_datelist(startdate, enddate)

    # download gdelt zip file for every month in dates
    t1 = datetime.datetime.now()

    for date in dates:
        print "retrieving gdelt file date", date
        # gdeltZip = urllib2.urlopen(url + date + '.zip')
        # localFile = open(importpath + date, 'w')
        # localFile.write(gdeltZip.read())
        # localFile.close()

    dt = datetime.datetime.now() - t1
    print "FINISHED DOWNLOADING FILES.  Total time elapsed: %s minutes" %(dt.seconds / 60)


def batch_import(
        table, startdate, enddate, query, database='gdelt',
        user='jamesconkling', table_schema=GDELT['gdeltSchema'],
        gdeltPath='/Users/jamesconkling/Documents/Projects/gdelt/export/'):
    # batch import query to postgres from a directory of gdelt files
    # must initialize the table first w/ the same schema and name as defined below

    def unzip(source_filename, dest_dir):
        with zipfile.ZipFile(source_filename) as zf:
            for member in zf.infolist():
                # Path traversal defense copied from
                # http://hg.python.org/cpython/file/tip/Lib/http/server.py#l789
                words = member.filename.split('/')
                path = dest_dir
                for word in words[:-1]:
                    drive, word = os.path.splitdrive(word)
                    head, word = os.path.split(word)
                    if word in (os.curdir, os.pardir, ''): continue
                    path = os.path.join(path, word)
                zf.extract(member, path)

    t1 = datetime.datetime.now()
    dates = gdelt_datelist(startdate, enddate)
    files_length = len(dates)
    file_num = 1

    for date in dates:
        # print "\t unzipping gdelt %s (%s / %s)" %(date, file_num, files_length)
        # unzip(gdeltPath + date + '.zip', gdeltPath)


        #load csv into database via psycopg2; see http://zetcode.com/db/postgresqlpythontutorial/
        connection = None

        try:
            print "\t connecting to postgres"
            connection = psycopg2.connect(database = database, user = user)
            cursor = connection.cursor()

            print "\t\t copying %s.csv into gdelt_temp" %date
            cursor.execute("CREATE TABLE gdelt_temp" + table_schema)
            cursor.execute("COPY gdelt_temp FROM '" + gdeltPath + date + ".csv'")

            print "\t\t inserting into %s query %s" %(table, query)
            cursor.execute("INSERT INTO " + table + " " + query)
            cursor.execute("DROP TABLE gdelt_temp")

            connection.commit()
            print "\t\t commit successful - inserted %s into %s" %(date, table)
            file_num = file_num + 1

        except psycopg2.DatabaseError, e:
            if connection:
                connection.rollback()

            print '\t\t commit failed - error %s \n' % e
            # os.remove(localPath + date + '.csv')
            sys.exit(1)

        finally:
            if connection:
                connection.close()
                print "\t connection closed \n"


        #delete .csv file in directory
        #os.remove(localPath + date + '.csv')
        #delete .zip file in directory
        #os.remove(localPath + date + '.zip')

    dt = datetime.datetime.now() - t1
    print "FINISHED LOADING FILES INTO POSTGRES.  Total time elapsed: %s minutes" %(dt.seconds / 60)


def init_table(table, table_schema, database='gdelt', user='jamesconkling'):

    connection = None
    try:
        connection = psycopg2.connect(database = database, user = user)
        cursor = connection.cursor()

        print "initializing empty table " + table
        cursor.execute("CREATE TABLE " + table + table_schema)

        connection.commit()
        print "COMMIT SUCCESSFUL - CREATED EMPTY TABLE " + table + "\n"

    except psycopg2.DatabaseError, e:
        if connection:
            connection.rollback()

        print 'commit failed - Error %s' % e + "\n"
        sys.exit(1)

    finally:
        if connection:
            connection.close()
            print 'connection closed'

def binned_density_map(
        point_table, grid_table, date_range, database='gdelt',
        user='jamesconkling', create_pntcnt_table=True):
    s_date = datetime.datetime(int(date_range[0][:4]), int(date_range[0][5:7]), int(date_range[0][8:10]))
    e_date = datetime.datetime(int(date_range[1][:4]), int(date_range[1][5:7]), int(date_range[1][8:10]))
    dates = rrule.rrule(rrule.MONTHLY, dtstart=s_date, until=e_date)

    t1 = datetime.datetime.now()

    ##initialize a copy of the grid table, to be later filled w/ point in polygon count for each month
    #the spatial column must be named geom and by of type geometry, rather than geography, as the PostGIS
    #function ST_Contains only works w/ the geometry type.
    #to create a grid table from a shpfile, use the following PostGIS command:
    #   shp2pgsql -s 4326 -g geom -I <file.shp> public.<grid_table> | psql -d <database>
    #I do not know how much error this introduces

    ##the code below uses parameterized queries, which allow for sql injection and are VERY dangerous
    #if another user is allowed to set the parameters.  So long as this is only run locally, sql injection
    #shouldn't be an issue
    if create_pntcnt_table:
        connection = None
        try:
            print "connecting to postgres"
            connection = psycopg2.connect(database = database, user = user)
            cursor = connection.cursor()

            print "Copying grid table %s to new table %s_pntcnt" %(grid_table, point_table)
            cursor.execute("CREATE TABLE %(point_table)s_pntcnt AS SELECT * FROM %(grid_table)s" %{
                'point_table': point_table, 'grid_table': grid_table })

            connection.commit()
            print "commit successful: created table %s_pntcnt \n" %(point_table)

        except psycopg2.DatabaseError, e:
            if connection:
                connection.rollback()

            print 'commit failed - Error %s' % e + "\n"
            sys.exit(1)

        finally:
            if connection:
                connection.close()


    ###add point in polygon count for each month
    ## for each month, run the following commands
    ## 1)add an empty column names after the month
    #ALTER TABLE greatlakes_pntcnt ADD COLUMN c90_01 int
    #
    ## 2)create a tmp table that joins each grid cell id to the count of points w/i
    #CREATE TABLE pntcnt_tmp AS
    #    SELECT drc_quarter_deg.gid, count(greatlakes_timequery.geo)::int
    #    FROM drc_quarter_deg LEFT JOIN (SELECT geo FROM greatlakes
    #                                    WHERE date_trunc('month', sqldate) = '1990-01-01'
    #                                   ) AS greatlakes_timequery
    #    ON ST_Contains(drc_quarter_deg.geom, greatlakes_timequery.geo::geometry)
    #    GROUP BY drc_quarter_deg.gid
    #
    ## 3)join the
    #UPDATE greatlakes_pntcnt
    #SET c90_01 = (SELECT pntcnt_tmp.count
    #              FROM pntcnt_tmp
    #              WHERE greatlakes_pntcnt.gid = pntcnt_tmp.gid)
    #
    ## 4)drop tmp table
    #DROP TABLE pntcnt_tmp

    date_length = len(dates[:])
    date_num = 1

    for date in dates:
        date_col = "d" + str(date.year)[2:] + "_" + '{0:02d}'.format(date.month)
        date_query = str(date.year) + "-" + '{0:02d}'.format(date.month) + "-01"

        connection = None
        try:
            print "connect to postgres"
            connection = psycopg2.connect(database = database, user = user)
            cursor = connection.cursor()

            print "add point in polygon count for %s (%s/%s)" %(date, date_num, date_length)
            print "\t add column %s to %s_pntcnt" %(date_col, grid_table)
            cursor.execute("ALTER TABLE %(point_table)s_pntcnt ADD COLUMN %(date_col)s int" %{
                            'point_table': point_table, 'date_col': date_col })

            print "\t create pntcnt_tmp to store point in polygon count"
            cursor.execute("""CREATE TABLE pntcnt_tmp AS
                            SELECT %(grid_table)s.gid, count(greatlakes_timequery.geo)::int
                            FROM %(grid_table)s LEFT JOIN
                                (SELECT geo FROM %(point_table)s
                                 WHERE date_trunc('month', sqldate) = '%(date_query)s'
                                ) AS greatlakes_timequery
                            ON ST_Contains( %(grid_table)s.geom, greatlakes_timequery.geo::geometry)
                            GROUP BY %(grid_table)s.gid""" %{
                            'grid_table': grid_table, 'point_table': point_table })

            print "\t join pntcnt_tmp to %s_pntcnt" %(point_table)
            cursor.execute("""UPDATE %(point_table)s_pntcnt
                                SET %(date_col)s = (
                                    SELECT pntcnt_tmp.count
                                    FROM pntcnt_tmp
                                    WHERE %(point_table)s_pntcnt.gid = pntcnt_tmp.gid
                                )""" %{'point_table': point_table, 'date_col': date_col })

            print "\t drop pntcnt_tmp"
            cursor.execute("""DROP TABLE pntcnt_tmp""")

            connection.commit()
            date_num += 1
            print "commit successful: added %s to %s_pntcnt \n" %(date_col, point_table)

        except psycopg2.DatabaseError, e:
            if connection:
                connection.rollback()

            print 'commit failed - Error %s' % e + "\n"
            sys.exit(1)

        finally:
            if connection:
                connection.close()

    dt = datetime.datetime.now() - t1
    print "FINISHED COUNTING POINT IN POLYGON.  Total time elapsed: %s minutes" %(dt.seconds / 60)




##############################################
############# Network Profile ################
##############################################
def update_value(database, table, column, change_from, change_to, user='jamesconkling'):
    connection = None

    try:
        print "connecting to postgres"
        connection = psycopg2.connect(database = database, user = user)
        cursor = connection.cursor()

        print "updating %s column %s: setting %s to %s" %(table, )
        cursor.execute("""UPDATE %(table)s set %(column)s = %(change_to)s
                        where %(column)s = %(change_from)s""" %{
                        'table': table, 'column': column,
                        'change_to': change_to, 'change_from': change_from })

        connection.commit()
        print "COMMIT SUCCESSFUL - updated value %s to %s \n" %(change_from, change_to)

    except psycopg2.DatabaseError, e:
        if connection:
            connection.rollback()

        print 'commit failed - Error %s' % e + "\n"
        sys.exit(1)

    finally:
        if connection:
            connection.close()



def actor_query(database, table, user='jamesconkling', threshold = 100):
    # w/i Postgres, import all cameo code dictionary cameo_total
    # (merge of all cameo codesheets w/ duplicates deleted).

    # Build a list of each Actor Name in the Database
    connection = None
    try:
        connection = psycopg2.connect(database = database, user = user)
        cursor = connection.cursor()
        cursor.execute("""
    SELECT COALESCE(A.Actor1Name, B.Actor2Name),
    COALESCE(A.count, 0) + COALESCE(B.count, 0) As total
    FROM(
    SELECT Actor1Name, count(*) FROM %(table)s GROUP BY Actor1Name) AS A
    FULL OUTER JOIN(
    SELECT Actor2Name, count(*) FROM %(table)s GROUP BY Actor2Name) AS B
    ON A.Actor1Name = B.Actor2Name WHERE A.count + B.count > %(threshold)s
    ORDER BY total DESC
    """ %{ 'table': table, 'threshold': threshold })

        actorNames = cursor.fetchall()
        print "successfully created actorNames from %s \n" %table

    except psycopg2.DatabaseError, e:
        if connection:
            connection.rollback()

        print 'commit failed - Error %s' % e + "\n"
        sys.exit(1)

    finally:
        if connection:
            connection.close()

    return actorNames


# require cameo_total
def actor_profile(actor_names_object, database, table, user='jamesconkling'):
    actorsProfile = {}
    name_length = len(actor_names_object)
    name_count = 1

    for actorName in actor_names_object:
        name = actorName[0]
        count = actorName[1]

        try:
            connection = psycopg2.connect(database = database, user = user)
            cursor = connection.cursor()

            print "Querying %s" %(table)
            #save to a temporary actor_profile_tmp the count of each actorCode for each actorName
            cursor.execute("""
    CREATE TABLE actor_profile_tmp AS
    SELECT COALESCE(A.actor1code, B.actor2code) AS actorcode,
    COALESCE(A.count, 0) + COALESCE(B.count, 0) As total
    FROM(
    SELECT Actor1Code, count(*) FROM %(table)s WHERE actor1name = '%(name)s' GROUP BY Actor1Code) AS A
    FULL OUTER JOIN(
    SELECT Actor2Code, count(*) FROM %(table)s WHERE actor2name = '%(name)s' GROUP BY Actor2Code) AS B
    ON A.Actor1Code = B.Actor2Code
    ORDER BY total DESC
    """ %{ 'table': table, 'name': name })

            #join cameo labels to each actorCode
            cursor.execute("""
    SELECT A.actorcode, A.total, B.label AS code1, C.label AS code2, D.label AS code3, E.label AS code4
    FROM actor_profile_tmp AS A
    LEFT JOIN cameo_total AS B ON (substring(A.actorcode from 1 for 3) = B.code)
    LEFT JOIN cameo_total AS C ON (substring(A.actorcode from 4 for 3) = C.code)
    LEFT JOIN cameo_total AS D ON (substring(A.actorcode from 7 for 3) = D.code)
    LEFT JOIN cameo_total AS E ON (substring(A.actorcode from 10 for 3) = E.code)
    """)

            actorCodes = cursor.fetchall()
            actorsProfile[name] = actorCodes

            cursor.execute("DROP TABLE actor_profile_tmp")

            print "successfully calculated actor codes for %s (%s / %s) \n" %(
                name, name_count, name_length
            )

            name_count += 1

        except psycopg2.DatabaseError, e:
            if connection:
                connection.rollback()

            print 'commit failed - Error %s' % e + "\n"
            sys.exit(1)

        finally:
            if connection:
                connection.close()
    return actorsProfile


def combine_actors(joinToName, joinFromNames, actorsProfile):
    """Combine the profile of all actors in joinFromNames
    to the profile of joinToName

    E.G.
    combine('RWANDA', ['RWANDAN', 'KIGALI'])
    """

    if not isinstance(joinFromNames, list):
        print "converting join from names to list"
        joinFromNames = [joinFromNames]

    for joinFromName in joinFromNames:
        print "joinFromName: " + joinFromName
        #for each of the entries for the actorName you are joining from
        for joinFromEntry in actorsProfile[joinFromName]:
            print "joinFromEntry: " + str(joinFromEntry)
            #if the cameo code is already in the joined to actorName,
            # create a new tuple that adds the count of the joinedfrom and joinedto
            if joinFromEntry[0] in [joinToEntry[0] for joinToEntry in actorsProfile[joinToName]]:
                for joinToEntry in actorsProfile[joinToName]:
                    if joinFromEntry[0] == joinToEntry[0]:
                        code = joinToEntry[0]
                        count = joinToEntry[1] + joinFromEntry[1]
                        label1 = joinToEntry[2]
                        label2 = joinToEntry[3]
                        label3 = joinToEntry[4]
                        label4 = joinToEntry[5]
                        # rewrite as: label1, label2, label3, label4 = joinToEntry[2:]
                        # double check syntax
                        new_tuple = (code, count, label1, label2, label3, label4)

                        actorsProfile[joinToName].remove(joinToEntry)
                        actorsProfile[joinToName].append(new_tuple)
                        print "added combined tuple: " + str(new_tuple)
                        break
            else:
                actorsProfile[joinToName].append(joinFromEntry)
                print "added new tuple: " + str(joinFromEntry)

        #delete the old actorName
        del actorsProfile[joinFromName]
        print "deleted from actorsProfile " + joinFromName

#all actors can be retrieved via the dictionary keys method
# actorsProfile.keys()

#return all actors and their event count, sorted by event
def count_actors(actorsProfile):
    actorsCount = []
    for key in actorsProfile:
        actorCount = sum([entry[1] for entry in actorsProfile[key]])
        actorsCount.append( (key, actorCount) )
    return sorted(actorsCount, key = lambda count: count[1])

