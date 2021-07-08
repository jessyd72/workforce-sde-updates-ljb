'''
 ____________________________________________________________________
 LJB
_____________________________________________________________________

   Program:    workforce-updates-ljb.py
   Purpose:    1. Read Workforce assignments from AGO.
               2. Query into Operator Inspection and Professional Engineer 
               Inspection
               3. For both OI and PEI:
                3a. If 'Completed'- update relative Last Inspection Date and
               Next Inspection Date fields in Asset Dataset using concat of 
               HazardTypeOption and HazardID fields.
               Write asset and assignment information to Excel WB under '* Completed'.
                3b. If 'Declined'/'In Progress'/'Not Started'- update relative
               Next Inspection Date field in Asset Dataset using concat of 
               HazardTypeOption and HazardID fields. 
               Wirte asset and assignment information to Excel WB under 
               '* Upcoming' noting its status at time of update. 
                3c. Identify attachment related to the assignment in the Asset
               Dataset.
                3d.Update Workforce assignment with new Inspection Date and status of 
                'Assigned', carry attributes, and attachment included. 
               4. Send email with Excel WB attached to client and LJB.
_____________________________________________________________________
   History:     GTG     06/2021    Created
_____________________________________________________________________
'''

import arcpy
from arcgis.gis import GIS
from arcgis.features import FeatureLayer

import json
import logging
from os import path, sys
import shutil

from datetime import datetime
from dateutil.relativedelta import relativedelta

import urllib
from urllib.request import urlopen
import contextlib

from xlsxwriter import Workbook

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

arcpy.env.overwriteOutput = True
arcpy.env.preserveGlobalIds = True


def printLog(words):

    logging.info(words)
    print(words)


def backupAssetDataset(assets,  fldr):
    '''Creates a backup of the SDE Asset Dataset 
    and a working copy to be used for later append.''' 

    today = datetime.today().strftime('%Y%m%d')
    printLog('Creating working file geodatabase...')
    gdb = arcpy.CreateFileGDB_management(fldr, 'backup_{}'.format(today), 'CURRENT')

    printLog('Copying Asset Dataset as "AssetData_working" and "AssetData_backup{}'.format(today))
    arcpy.FeatureClassToFeatureClass_conversion(assets, gdb[0], 'AssetData_backup{}'.format(today))
    copy_fc = arcpy.FeatureClassToFeatureClass_conversion(assets, gdb[0], 'AssetData_working')
    copy_att_table = gdb[0] + r'\AssetData_working__ATTACH'

    printLog('Copy was successful! Working copy and backup created...')
    return(copy_fc, copy_att_table, gdb[0])


def createWorkbook(output_dir):
    ''' Creates Excel workbook of Workforce assignment/
    Asset dataset updates. Contains Completes and 
    Upcoming assignments. '''

    today = datetime.today().strftime('%Y%m%d')
    wb_path = output_dir + r'\AssetWorkforceUpdates_{}.xlsx'.format(today)

    workbook = Workbook(wb_path)
    printLog('Excel workbook created...')

    return(workbook, wb_path)


def addWorksheet(wb, sheetname, cols, values):
    '''Adds worksheet to supplied Excel workbook'''

    sheet = wb.add_worksheet(sheetname)
    for c in cols:
        sheet.write(c[0], c[1])

    row = 1

    for k, v in values.items():

        row += 1

        sheet.write('A{}'.format(str(row)), v[0])
        sheet.write('B{}'.format(str(row)), v[1])
        sheet.write('C{}'.format(str(row)), v[2])
        sheet.write('D{}'.format(str(row)), v[3])
        sheet.write('E{}'.format(str(row)), v[4])
        sheet.write('F{}'.format(str(row)), v[5])
        sheet.write('G{}'.format(str(row)), v[6])
        sheet.write('H{}'.format(str(row)), v[7])
        sheet.write('I{}'.format(str(row)), k)

    printLog('Worksheet {} was created and populated...'.format(sheetname))

    return(wb, sheet)


def getQueries(orgURL, username, password, services):
    '''Gets queried feature layers from AGO'''

    gis = GIS(orgURL, username, password)
    printLog('Connected as {}'.format(username))

    for service in services:

        try:

            assign_service = FeatureLayer(service['assign url'], gis)
            worker_service = FeatureLayer(service['worker url'], gis)

            complete_opi = assign_service.query(service['complete opi query'])
            printLog('{} \n'.format(str(complete_opi)))

            upcoming_opi = assign_service.query(service['upcoming opi query'])
            printLog('{} \n'.format(str(upcoming_opi)))

            complete_pei = assign_service.query(service['complete pei query'])
            printLog('{} \n'.format(str(complete_pei)))

            upcoming_pei = assign_service.query(service['upcoming pei query'])
            printLog('{} \n'.format(str(upcoming_pei)))

            completed = {'opi': complete_opi, 'pei': complete_pei}
            upcoming = {'opi': upcoming_opi, 'pei': upcoming_pei}

            printLog('Queried for completed OP and PE inspections and upcoming OPI and PE inspections...')

            return(assign_service, worker_service, completed, upcoming)

        except Exception:
            logging.error("EXCEPTION OCCURRED", exc_info=True)
            printLog("Quitting! \n ------------------------------------ \n\n")

        
def updateAssets(assets, att_table, att_path, completed, upcoming, assign_service, worker_service, wb):
    '''Updates the Workforce assignment, Asset Dataset in the working
    gdb, and populates the Excel workbook.'''

    # used to calculate next inspection date
    freq_dict = {'Weekly': 7, 'Monthly': 30, 'Yearly': 365, 'BiAnnual': 182}

    try:

        #### loop through completed tasks ####
        for k, v in completed.items():

            printLog('Working on the completed {} inspections...'.format(k))

            if k == 'opi':
                sheet_name = 'Operator Completed'
                last_insp_date = 'LastOpInspect'
                next_insp_date = 'NextOpInspect'
                freq_field = 'OperatorInspection'

            elif k == 'pei':
                sheet_name = 'PE Completed'
                last_insp_date = 'InspectDate'
                next_insp_date = 'NextPEInspect'
                freq_field = 'PEInspection'

            else:
                logging.error('Invalid key for Completed - check script!!')

            # dictionary to populate Excel/update local assets/create new assignments
            # d = {assetid: [area, location, description, asset_desc, complete_date, status, insp_len, worker_name, next_due_date, concat_assetid, att_name]}
            complete_dict = {}
            # Excel columns
            complete_cols = [['A1','Area'],['B1', 'Location'], ['C1', 'Description'],['D1', 'Asset Description '],['E1', 'Completion Date'],
                            ['F1', 'Inspection Status'],['G1', 'Total Length of Inspection Time (Stop time - start time)'],
                            ['H1', 'Who (Username that completed inspection)'],['I1', 'AssetID (Hazard ID)']]
            # fields to read local assets
            fields = [freq_field, 'Building', 'subarea', 'Name', 'AssetNameNotes', 'globalid']

            for row in v:

                # service info - write to Excel
                concat_assetid = row.attributes['location']
                status = row.attributes['status']
                assign_date = row.attributes['assigneddate']
                start_date = row.attributes['inprogressdate']
                complete_date = row.attributes['completeddate']
                worker_guid = row.attributes['workerid']

                printLog('Getting worker name from service....')
                worker_query = "GlobalID = '{{{0}}}'".format(str(worker_guid))
                worker = worker_service.query(worker_query)
                for w in worker:
                    worker_name = w.attributes['name']

                printLog('Calculating inspection length...')
                # Add statement for when assigned date is used 
                start = ''
                end = datetime.fromtimestamp(complete_date/1000).strftime("%Y-%m-%d %H:%M:%S")
                if start_date != None:
                    start = datetime.fromtimestamp(start_date/1000).strftime("%Y-%m-%d %H:%M:%S")
                    no_inprogess = ''
                elif assign_date != None:
                    start = datetime.fromtimestamp(assign_date/1000).strftime("%Y-%m-%d %H:%M:%S")
                    no_inprogess = "**No 'In Progress' timestamp, 'Assign Date' used as start time"
                else:
                    insp_len = 'No valid start time found in Workforce assignment...'

                if start: 
                    diff = relativedelta(end, start)
                    insp_len = '{0} month(s), {1} day(s), {2}:{3}:{4} {5}'.format(diff.months, diff.days, diff.hours, 
                                                                            diff.minutes, diff.seconds, no_inprogess)                 

                printLog('Get asset id to update in Asset Dataset...')
                assetid = ''
                
                split = concat_assetid.split('#')
                if len(split) > 1:
                    assetid = split[1].strip(')')
                else:
                    printLog('OBJECTID {} does not have an acceptable Asset ID in the Location \
                    field of the Assignments feature service!'.format(str(row.attributes['OBJECTID'])))
                    printLog('Location field carries value: {}'.format(concat_assetid))
                
                if assetid != '':
                    printLog('Assetid is valid, get info from local assets...')

                    info = [[x[0], x[1], x[2], x[3], x[4], x[5]] for x in arcpy.da.SearchCursor(assets, fields, 'HazardID = {}'.format(assetid))]

                    asset_info = info[0]
                    # asset info - write to Excel
                    frequency = asset_info[0]
                    area = asset_info[1]
                    location = asset_info[2]
                    description = asset_info[3]
                    asset_desc = asset_info[4]
                    globalid = asset_info[5]
                    printLog('Finding attachment...')
                    att_name = ['ATT{0}_{1}'.format(str(y[0]), str(y[1])) for y in arcpy.da.SearchCursor(att_table, ['ATTACHMENTID', 'ATT_NAME'], "REL_GLOBALID = '{}'".format(globalid))][0]

                    printLog('Calculating next inspection date...')
                    next_due_date = complete_date + (freq_dict[frequency]*86400000)
                    
                    printLog('Populate dictionary...')
                    complete_dict[assetid] = [area, location, description, asset_desc, complete_date, status, insp_len, worker_name, next_due_date, concat_assetid, att_name]

            printLog('Update excel workbook...')
            addWorksheet(wb, sheet_name, complete_cols, complete_dict)

            printLog('Update Last Inspection date and Next Inspection date fields in working Asset Dataset...')
            with arcpy.da.UpdateCursor(assets, ['HazardID', last_insp_date, next_insp_date]) as ucur:
                for row in ucur:
                    if row[0] in complete_dict.keys():
                        completedate_str = datetime.fromtimestamp(complete_dict[row[0]][4]/1000).strftime("%Y-%m-%d %H:%M:%S")
                        row[1] = completedate_str
                        nextdate_str = datetime.fromtimestamp(complete_dict[row[0]][8]/1000).strftime("%Y-%m-%d %H:%M:%S")
                        row[2] = nextdate_str

                    ucur.updateRow(row)

            printLog('Update Workforce assignment...')
            dict_len = len(complete_dict)
            if dict_len > 0:
                fset = v
                features = fset.features
                for k_com, v_com in complete_dict.items():

                    feat = [f for f in features if f.attributes['location'] == '{}'.format(v_com[9])][0]
                    feat_edit = feat
                    feat_edit.attributes['status'] = 1
                    feat_edit.attributes['duedate'] = v_com[8]

                    fl = feat[0]
                    oid = fl.get_value('OBJECTID')
                    atts = assign_service.attachments.get_list(oid)
                    if atts:
                        img_names = [a['name'] for a in atts]
                        if v_com[10] in img_names:
                            printLog('Attachment already exists for oid {}'.format(str(oid)))
                        else:    
                            assign_service.attachments.add(oid, att_path + r'\{}'.format(v_com[10]))
                            new_atts = assign_service.attachments.get_list(oid)
                            img_names = [a['name'] for a in new_atts]
                            if v_com[10] in img_names:
                                printLog('Attachment successfully added to oid {}'.format(str(oid)))

                    update_feat = assign_service.edit_features(updates = [feat_edit])

                    for result in update_feat['updateResults']:
                        if not result['success']:
                            logging.error('error {}: {}'.format(result['error']['code'],
                                                            result['error']['description']))
                        else:
                            printLog('Assignments layer is successfully updated!')

            
        #### loop through upcoming tasks ####
        for k, v in upcoming:

            printLog('Working on the upcoming {} inspections...'.format(k))

            if k == 'opi':
                sheet_name = 'Operator Upcoming'
                next_insp_date = 'NextOpInspect'
                freq_field = 'OperatorInspection'

            elif k == 'pei':
                sheet_name = 'PE Upcoming'
                next_insp_date = 'NextPEInspect'
                freq_field = 'PEInspection'

            else:
                logging.error('Invalid key for Upcoming - check script!!')

            # dictionary to populate Excel/update local assets/create new assignments
            # d = {assetid: [area, location, description, asset_desc, due_date, frequency, last_insp_date, status, concat_assetid, att_name]}
            upcoming_dict = {}
            # Excel columns
            upcoming_cols = [['A1', 'Area'],['B1', 'Location '],['C1', 'Description'],['D1', 'Asset Description '],['E1', 'Due Date'],
            ['F1', 'Frequency of Inspection'],['G1', 'Last Inspection Date'],['H1', 'Last Inspection Status'],['I1', 'Asset ID (Hazard  ID)']]
            # fields to read local assets
            fields = [freq_field, 'Building', 'subarea', 'Name', 'AssetNameNotes', last_insp_date, 'globalid']

            for row in v:

                # service info - write to Excel
                concat_assetid = row.attributes['location']
                due_date = row.attributes['duedate']
                status = row.attributes['status']

                printLog('Get asset id to update in Asset Dataset...')
                assetid = ''
                
                split = concat_assetid.split('#')
                if len(split) > 1:
                    assetid = split[1].strip(')')
                else:
                    printLog('OBJECTID {} does not have an acceptable Asset ID in the Location \
                    field of the Assignments feature service!'.format(str(row.attributes['OBJECTID'])))
                    printLog('Location field carries value: {}'.format(concat_assetid))
                
                if assetid != '':
                    printLog('Assetid is valid, get info from local assets...')

                    info = [[x[0], x[1], x[2], x[3], x[4], x[5], x[6]] for x in arcpy.da.SearchCursor(assets, fields, 'HazardID = {}'.format(assetid))]

                    asset_info = info[0]
                    # asset info - write to Excel
                    frequency = asset_info[0]
                    area = asset_info[1]
                    location = asset_info[2]
                    description = asset_info[3]
                    asset_desc = asset_info[4]
                    last_inspect = asset_info[5]
                    globalid = asset_info[6]
                    printLog('Finding attachment...')
                    att_name = ['ATT{0}_{1}'.format(str(y[0]), str(y[1])) for y in arcpy.da.SearchCursor(att_table, ['ATTACHMENTID', 'ATT_NAME'], "REL_GLOBALID = '{}'".format(globalid))][0]

                    printLog('Convert due date from millisec to sec, then datetime obj...')
                    dt_due_date = datetime.fromtimestamp(due_date/1000)
                    today = datetime.now()

                    if today > dt_due_date:
                        printLog('Due date has passed, use frequency to calculate from TODAY...')
                        epoch = datetime.utcfromtimestamp(0)
                        dt = (today-epoch).total_seconds()*1000

                        next_due_date = dt + (freq_dict[frequency]*86400000)
                    else:
                        printLog('Due date has not passed, keep due date as next inspection date...')
                        next_due_date = due_date

                    printLog('Populating dictionary...')
                    upcoming_dict[assetid] = [area, location, description, asset_desc, next_due_date, frequency, last_inspect, status, concat_assetid, att_name]

            printLog('Update excel workbook...')
            addWorksheet(wb, sheet_name, upcoming_cols, upcoming_dict)

            printLog('Update Next Inspection date fields in working Asset Dataset...')
            with arcpy.da.UpdateCursor(assets, ['HazardID', next_insp_date]) as ucur:
                for row in ucur:
                    if row[0] in upcoming_dict.keys():
                        row[1] = upcoming_dict[row[0]][4]

                    ucur.updateRow(row)

            printLog('Update Workforce assignmen...')
            dict_len = len(upcoming_dict)
            if dict_len > 0:
                fset = v
                features = fset.features
                for k_com, v_com in upcoming_dict.items():

                    feat = [f for f in features if f.attributes['location'] == '{}'.format(v_com[8])][0]
                    feat_edit = feat
                    feat_edit.attributes['duedate'] = v_com[4]

                    fl = feat[0]
                    oid = fl.get_value('OBJECTID')
                    atts = assign_service.attachments.get_list(oid)
                    if atts:
                        img_names = [a['name'] for a in atts]
                        if v_com[9] in img_names:
                            printLog('Attachment already exists for oid {}'.format(str(oid)))
                        else:    
                            assign_service.attachments.add(oid, att_path + r'\{}'.format(v_com[9]))
                            new_atts = assign_service.attachments.get_list(oid)
                            img_names = [a['name'] for a in new_atts]
                            if v_com[9] in img_names:
                                printLog('Attachment successfully added to oid {}'.format(str(oid)))

                    update_feat = assign_service.edit_features(updates = [feat_edit])

                    for result in update_feat['updateResults']:
                        if not result['success']:
                            logging.error('error {}: {}'.format(result['error']['code'],
                                                            result['error']['description']))
                        else:
                            printLog('Assignments layer is successfully updated!')

        wb.close()
        return(assets)

    except Exception:
        logging.error("EXCEPTION OCCURRED", exc_info=True)
        printLog("Quitting! \n ------------------------------------ \n\n")                   


def updateAssetsSDE(working_assets, sde_assets):
    '''Updates Asset Dataset within the SDE.
    This function will stop the service: ######
    Delete rows from SDE assets and append
    from working asset FC. The service will then
    be restarted.'''

    printLog("Updating Asset Dataset service...")
    try:
        token = get_token(admin_user, admin_pass, server_name, port, expiration)

        # stopping service
        action = 'stop'
        json_output = serviceStartStop(server_name, port, service_name, action, token)
        # verify success
        if json_output['status'] == 'success':
            printLog('{} was stopped successfully'.format(service_name))
        else:
            printLog('Failed to stop {}'.format(service_name))
            raise Exception(json_output)

    except Exception as e:
        printLog(e)

    printLog("updating rows in Asset Dataset...")
    arcpy.DeleteRows_management(sde_assets)
    arcpy.Append_management(working_assets, sde_assets, "NO_TEST")

    try:
        # starting service
        action = 'start'
        json_output = serviceStartStop(server_name, port, service_name, action, token)
        # verify success
        if json_output['status'] == 'success':
            printLog('{} was started successfully!'.format(service_name))
        else:
            printLog('Failed to start {}'.format(service_name))
            raise Exception(json_output)

    except Exception as e:
        printLog(e)


def sendemail(eto, subject, message, email_un, email_pass, att = ''):
    '''Sends email to client and to LJB with the
    Excel workbook of assignment updates attached'''

    efrom = '' # replace with intended sender 
    msg = MIMEMultipart()
    msg['From'] = efrom
    msg['To'] = eto
    msg['Subject'] = subject

    msg.attach(MIMEText(message, 'plain'))

    if att != '':
        filename = path.basename(att)
        attachment = open(att, "rb")
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', "attachment; filename= %s" % filename)
        msg.attach(part)

    try:
        server = smtplib.SMTP('smtp.office365.com', 587)
        server.ehlo()
        server.starttls()
        server.login(email_un, email_pass)
        text = msg.as_string()
        server.sendmail(efrom, eto, text)
        printLog('email out')
        server.quit()
    except:
        printLog('SMPT server connection error :(')


def cleanUp(archive_fldr, gdb_path, wb_path):
    '''Moves created gdb and workbook to archived
    folder. '''

    gdb_name = path.basename(gdb_path)
    wb_name = path.basename(wb_path)

    printLog('moving geodatabase to archived...')
    shutil.move(gdb_path, archive_fldr + r'\{}'.format(gdb_name))
    printLog('moving workbook to archived...')
    shutil.move(wb_path, archive_fldr + r'\{}'.format(wb_name))

    printLog('Successfully moved to "archived"!')


def get_token(adminuser, adminpass, server, port, exp):
    '''Generates token'''
    printLog("getting token")
    
    # build url
    url = r"https://{}:{}/arcgis/admin/generateToken?f=json".format(server, port)

    # dict for query string, used to request token
    query_dict = {"username":adminuser, "password":adminpass, "expiration":str(exp), "client":"requestip"}
    query_string = urllib.urlencode(query_dict)

    try:
        # request token, will close url after completed
        with contextlib.closing(urlopen(url, query_string)) as json_response:
            token_result = json.loads(json_response.read())
            if 'token' not in token_result or token_result == None:
                raise Exception('Failed to get token: {}'.format(token_result['messages']))
            else:
                return token_result['token']

    except Exception as e:
        printLog('Could not connect to {} on port {}. {}'.format(server, port, e))


def serviceStartStop(server, port, service, action, token):
    '''Starts or stops service'''
    printLog("{} service".format(action))

    # build url
    url = r"https://{0}:{1}/arcgis/admin".format(server, port)
    request_url = url + r"/services/{0}/{1}/{2}".format(service_name, service, action)
    printLog(request_url)

    # dict for query string, used to send request to start/stop
    query_dict = {"token": token, "f": "json"}
    query_string = urllib.urlencode(query_dict)

    # send request, close after complete
    with contextlib.closing(urllib.urlopen(request_url, query_string)) as json_response:
        printLog(json_response)
        return json.loads(json_response.read())


if __name__ == "__main__":

    # ------------------------------------------ maintain log file ------------------------------------------
    logfile = (path.abspath(path.join(path.dirname(__file__), '..', r'logs\workforce-updates-ljb-LOG.txt')))
    logging.basicConfig(filename=logfile,
                        level=logging.INFO,
                        format='%(levelname)s: %(asctime)s %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S')
    printLog("Starting run... \n")

    #  ------------------------------------------ get AGOL creds ------------------------------------------
    ago_text = open(path.join(sys.path[0], 'ags-creds.json')).read()
    json_ago = json.loads(ago_text)

    # URL to ArcGIS Online organization or ArcGIS Portal
    orgURL = json_ago['orgURL']
    # Username of an account in the org/portal that can access and edit all services listed below
    username = json_ago['username']
    # Password corresponding to the username provided above
    password = json_ago['password']
    
    # ------------------------------------------ workforce services ------------------------------------------
    services = [{'assign url': '',
                'worker url': '',
                'assign type url': '',
                'complete opi query': "status=3 AND assignmenttype = '3102e6c0-872d-4f84-9a48-42ba04e35655'",
                'upcoming opi query': "status IN (0, 1, 2, 4, 5) AND assignmenttype = '3102e6c0-872d-4f84-9a48-42ba04e35655'",
                'complete pei query': "status=3 AND assignmenttype = 'ef32bd2b-6134-47b1-a08c-bbfb33e07ef9'",
                'upcoming pei query': "status IN (0, 1, 2, 4, 5) AND assignmenttype = 'ef32bd2b-6134-47b1-a08c-bbfb33e07ef9'"}]

    # ------------------------------------------ get email creds ------------------------------------------
    email_text = open(path.join(sys.path[0], 'email-creds.json')).read()
    json_email = json.loads(email_text)

    # email login
    email_un = json_email['eusername']
    # email pass
    email_pass = json_email['epassword']

    # email details
    email_recipient = '' 
    email_subject = 'Asset Dataset and Workforce Updates- LJB'
    email_message = 'Please see attached Excel Workbook'

    # ------------------------------------------ service inputs  ------------------------------------------
    # credentials
    admin_user = ""
    admin_pass = ""
    # server info
    server_name = ""
    port = "6443"
    # service name
    service_name = ""
    # token expires in 12 hours
    expiration = 720

    # ------------------------------------------ local inputs  ------------------------------------------
    # asset dataset
    sde_assets = r'C:\data\gtg-data\projects\ljb-consultant\data\working\data\GTG_LJB_WFDB.gdb\AssetExampleData'
    # attachment path
    att_path = r'C:\data\gtg-data\projects\ljb-consultant\data\working\data\Pics'
    # working path
    workingfldr = r'C:\data\gtg-data\projects\ljb-consultant\data\working\data'
    # archive 
    archlfdr =  r'C:\data\gtg-data\projects\ljb-consultant\data\working\archived'
    
    # ------------------------------------------ execute functions ------------------------------------------
    printLog('Backing up data and creating working copy...')
    gdb_assets, gdb_att_table, gdb_path = backupAssetDataset(sde_assets, workingfldr)
    printLog('Grabbing Workforce services...')
    assignservice, workerservice, completed_d, upcoming_d = getQueries(orgURL, username, password, services)
    printLog('Creating Excel workbook...')
    wb, wb_path = createWorkbook(workingfldr)
    printLog('Updating assets and Workforce...')
    final_assets = updateAssets(gdb_assets, gdb_att_table, att_path, completed_d, upcoming_d, assignservice, workerservice, wb)
    printLog('Updating AssetDataset in SDE...')
    updateAssetsSDE(final_assets, sde_assets)
    printLog('Sendning e-mail...')
    sendemail(email_recipient, email_subject, email_message, email_un, email_pass, att=wb_path)
    printLog('Lookin'' good... Cleaning up....')
    cleanUp(archlfdr, gdb_path, wb_path)

    printLog("Success! \n ------------------------------------ \n\n")
