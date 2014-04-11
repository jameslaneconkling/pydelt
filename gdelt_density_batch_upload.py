import re
import shutil
import os

account = '*****'
token = '*****'

# syria
# proj_path = '/Users/jamesconkling/Documents/MapBox/project/'
# proj_template = 'syria_conflict_total'
# proj_name = 'syria'
# minzoom = 1
# maxzoom = 10
# bbox = '35.5,32.1,42.5,37.2' # --bbox=[xmin,ymin,xmax,ymax]
# upload_template = True

# aleppo
proj_path = '/Users/jamesconkling/Documents/MapBox/project/'
proj_template = 'aleppo_conflict_total'
proj_name = 'aleppo'
minzoom = 11
maxzoom = 14
bbox = '36.5,35.5,37.5,36.5' # --bbox=[xmin,ymin,xmax,ymax]
upload_template = True

os.chdir('/Applications/TileMill.app/Contents/Resources/')

dates = ['d13_10_01', 'd13_10_08', 'd13_10_15', 'd13_10_22', 'd13_10_29',
        'd13_11_05', 'd13_11_12', 'd13_11_19', 'd13_11_26', 'd13_12_03',
        'd13_12_10', 'd13_12_17', 'd13_12_24', 'd13_12_31']



if upload_template:
    print "creating mbtile ", proj_template
    export_command = """./index.js export %(project)s ~/Documents/MapBox/export/%(project)s.mbtiles --format=mbtiles \
                        --bbox="%(bbox)s" --minzoom=%(minzoom)s --maxzoom=%(maxzoom)s \
                        """ %{'project': proj_template, 'bbox': bbox, 'minzoom': minzoom, 'maxzoom': maxzoom }
    os.system(export_command)

    print "uploading mbtile ", proj_template
    export_command = """./index.js export %(project)s ~/Documents/MapBox/export/%(project)s.mbtiles --format=upload \
                        --syncAccount="%(account)s" --syncAccessToken"%(token)s" \
                        """ %{'project': proj_template, 'account': account, 'token': account }
    os.system(export_command)


for date in dates:
    # copy template project
    newProjectName = proj_name + "_" + date
    print 'uploading: ', newProjectName

    shutil.copytree(proj_path + '/' + proj_template, proj_path + '/' + newProjectName)

    # change mss to style according to date column
    with open(proj_path + newProjectName + '/' + 'style.mss', "r+") as f:
        contents = f.read()
        contents = re.sub('total', date, contents)
        f.seek(0)
        f.write(contents)
        f.truncate()

    # change project name in mml file
    with open(proj_path + newProjectName + '/' + 'project.mml', "r+") as f:
        contents = f.read()
        contents = re.sub(proj_template, newProjectName, contents)
        f.seek(0)
        f.write(contents)
        f.truncate()


    print "creating mbtile ", date
    export_command = """./index.js export %(project)s ~/Documents/MapBox/export/%(project)s.mbtiles --format=mbtiles \
                        --bbox="%(bbox)s" --minzoom=%(minzoom)s --maxzoom=%(maxzoom)s \
                        """ %{'project': newProjectName, 'bbox': bbox, 'minzoom': minzoom, 'maxzoom': maxzoom }
    os.system(export_command)

    print "uploading mbtile ", date
    export_command = """./index.js export %(project)s ~/Documents/MapBox/export/%(project)s.mbtiles --format=upload \
                        --syncAccount="%(account)s" --syncAccessToken"%(token)s" \
                        """ %{'project': newProjectName, 'account': account, 'token': account }
    os.system(export_command)

    print "**********************************"


##############################################
    # iteratively
# with open("/Users/jamesconkling/Desktop/gdelt_test_style.mss", "r") as in_file:
#     with open("/Users/jamesconkling/Desktop/gdelt_t2.mss", "w") as out_file:
#         i = 1
#         for line in in_file:
#             if 'pnt_10km' in line:
#                 new_line = re.sub('pnt_10km', 'pnt_40km', line)
#                 out_file.write(new_line)
#                 print 'line %s: rewrite to %s' %(i, new_line)
#             else:
#                 out_file.write(line)
#                 print 'line %s: no rewrite' %i
#             i += 1

    # at once
# with open(proj_path + newProjectName, "r+") as f:
#     contents = f.read()
#     contents = re.sub('total', date, contents)
#     f.write(contents)
