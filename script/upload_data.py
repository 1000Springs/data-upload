#-------------------------------------------------------------------------------
# Name:        1000 Springs Tablet Data Uploader
# Purpose:     Upload data and images collected using the 1000 Springs
#              Android tablet app to the 1000 Springs database.
#
# Author:      duncanw
# Created:     07/10/2013
# Updated:     $Date:  $
# Revision:    $Revision: $
#-------------------------------------------------------------------------------

import os
import ConfigParser
from datetime import date, timedelta, datetime
import logging
import sys
import smtplib
from email.mime.text import MIMEText
import re
import codecs

import MySQLdb
from PIL import Image
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import xlrd

log = logging.getLogger('Springs Uploader')

def main():

    upload_error = False
    db_conn = None
    log_file = None
    try:
        config = load_config('upload_data.cfg')
        log_file = init_logging(config)
        log.info('upload_tablet_data.py '+str(sys.argv))
        db_conn = db_connect(config)
        new_files_dir = get_new_files_dir(config)
        feature_files, sample_files, image_files, other_xls_files = find_files(new_files_dir)

        process_feature_files(db_conn, feature_files)
        process_sample_files(db_conn, sample_files)
        process_image_files(config, db_conn, image_files)
        process_geochem_files(db_conn, other_xls_files)

        unmount_data_share(config)

    except Exception as e:
        upload_error = True
        log.exception(e)

    finally:
        if upload_error and log_file is not None and config is not None:
            log.info('Sending error notification')
            #send_error_notification(log_file.baseFilename, config)

        log.info('upload_tablet_data.py exiting\n')
        if db_conn is not None:
            db_conn.close()
        if log_file is not None:
            log_file.close()

IMAGE_SAMPLE_NUMBER = 'sample_number'
IMAGE_TYPE = 'image_type'

def find_files(new_files_dir):

    feature_file_re = re.compile('data-features-[0-9]+\.xls')
    sample_file_re = re.compile('data-samples-[0-9]+\.xls')
    other_xls_file_re = re.compile('.*\.xls')
    image_file_re = re.compile('(P1\.\d{4})_([A-Z]*)_\d+\.jpg')
    feature_files = []
    sample_files = []
    other_xls_files = []
    image_files = {}
    for dirpath, dirnames, filenames in os.walk(new_files_dir):
        for filename in filenames:
            file_path = os.path.join(dirpath,filename)
            if (feature_file_re.match(filename)):
                feature_files.append(file_path)

            elif (sample_file_re.match(filename)):
                sample_files.append(file_path)

            elif (other_xls_file_re.match(filename)):
                other_xls_files.append(file_path)

            else:
                image = image_file_re.match(filename)
                if (image):
                    image_files[file_path] = {
                        IMAGE_SAMPLE_NUMBER: image.group(1),
                        IMAGE_TYPE: image.group(2)
                    }

    return feature_files, sample_files, image_files, other_xls_files

#-------------------------------------------------------------------------------
# FEATURE FILE PROCESSING
#-------------------------------------------------------------------------------
def process_feature_files(db_conn, files_to_process):
    # Files must be processed in chronological order to ensure
    # updates are applied in the correct order
    with db_conn:
        cursor = db_conn.cursor()
        for feature_file in sorted(files_to_process):
            log.debug('Processing feature file ' + feature_file)
            rows = get_tablet_data_rows(feature_file)
            for row in rows:
                sql, sql_params = get_location_update_sql(db_conn, row)
                cursor.execute(sql, sql_params)

# data-feature spreadsheet column -> DB location table column
FEATURE_NAME_COLUMN = '#FeatureName'
FEATURE_COLUMN_MAP = {
    'GeothermalField': 'feature_system',
    'LocationLatitude': 'lat',
    'LocationLongitude': 'lng',
    'Description': 'description',
    'AccessType': 'access'
}

def get_location_update_sql(db_conn, row):

    column_names, values = get_column_names_and_values(row, FEATURE_COLUMN_MAP)
    feature_name = row[FEATURE_NAME_COLUMN]
    feature_id = get_feature_id(db_conn, feature_name)

    if feature_id != None:
        sql = 'update location set ' + '=%s,'.join(column_names) + '=%s where id=%s'
        values.append(feature_id)
    else:
        sql = 'insert into location (' + ','.join(column_names) + ', feature_name) values ('+ ('%s,'*len(values)) +'%s)'
        values.append(feature_name)

    return sql, values

def get_feature_id(db_conn, feature_name):
    cursor = db_conn.cursor()
    try:
        cursor.execute('select id from location where feature_name=%s', feature_name)
        rows = cursor.fetchall()
        if len(rows) == 0:
            return None

        return rows[0][0]

    finally:
        cursor.close()

#-------------------------------------------------------------------------------
# SAMPLE FILE PROCESSING
#-------------------------------------------------------------------------------
def process_sample_files(db_conn, files_to_process):

    with db_conn:
        cursor = db_conn.cursor()
        for sample_file in sorted(files_to_process):
            log.debug('Processing sample file ' + sample_file)
            rows = get_tablet_data_rows(sample_file)
            for row in rows:
                # Note the order is important here - the sample insertion
                # SQL uses the MySQL last_insert_id() function to get the
                # ID of the physical_data record
                sql, sql_params, sample = get_physical_data_insert_sql(db_conn, row)
                cursor.execute(sql, sql_params)
                sql, sql_params = get_sample_insert_sql(db_conn, row, sample)
                cursor.execute(sql, sql_params)

# data-sample spreadsheet column -> DB sample table column
SAMPLE_COLUMN_MAP = {
    'SampleNumber': 'sample_number',
    'SurveyDate': 'date_gathered',
    'LeadObserverName': 'sampler',
    'Comments': 'comments'
}

DATE_NO_SECONDS_RE = re.compile('^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{1,2}$')
DATE_NO_SECONDS_FORMAT = '%d/%m/%Y %H:%M'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

def get_sample_insert_sql(db_conn, row, sample):

    # Older files have a different date format, need to canonicalise it
    survey_date = row['SurveyDate']
    if DATE_NO_SECONDS_RE.match(survey_date):
        row['SurveyDate'] = datetime.strptime(survey_date, DATE_NO_SECONDS_FORMAT).strftime(DATE_FORMAT)

    column_names, values = get_column_names_and_values(row, SAMPLE_COLUMN_MAP)
    feature_name = row[FEATURE_NAME_COLUMN]
    feature_id = get_feature_id(db_conn, feature_name)

    if sample == None:
        # Assume the physical_data row will be inserted immediately before this
        # sample row is inserted
        sql = 'insert into sample (phys_id,' + ','.join(column_names) + ',location_id) values (last_insert_id(),'+ ('%s,'*len(values)) +'%s)'
        values.append(feature_id)
    elif 'phys_id' in sample:
        # sample and physical data records already exist, perform update
        sql = 'update sample set ' + '=%s,'.join(column_names) + '=%s, location_id=%s where id=%s'
        values.append(feature_id)
        values.append(sample['id'])
    else:
         # Update existing sample record, assume physical_data record inserted immediately before this
        sql = 'update sample set ' + '=%s,'.join(column_names) + '=%s, phys_id=last_insert_id() where id=%s'

    return sql, values

# data-sample spreadsheet column -> DB physical_data table column
SAMPLE_TO_PHYSICAL_COLUMN_MAP = {
    'SampleTemperature': 'sampleTemp',
    'pH': 'pH',
    'OxidationReductionPotential': 'redox',
    'Conductivity': 'conductivity',
    'DissolvedOxygen': 'dO',
    'Turbidity': 'turbidity',
    'DnaVolume': 'dnaVolume',
    'FerrousIronAbs': 'ferrousIronAbs',
    'GasVolume': 'gasVolume',
    'FeatureSize': 'size',
    'ColourRgbHex': 'colour',
    'Ebullition': 'ebullition',
    'FeatureTemperature': 'initialTemp',
    'SoilCollected': 'soilCollected',
    'WaterColumnCollected': 'waterColumnCollected'
}

COLOUR_RE = re.compile('ff([a-f0-9]{6})', re.IGNORECASE)

def get_physical_data_insert_sql(db_conn, row):

    colourData = COLOUR_RE.match(row['ColourRgbHex'])
    row['ColourRgbHex'] = colourData.group(1) if colourData else None

    set_soil_collected(row)
    set_water_column_collected(row)

    column_names, values = get_column_names_and_values(row, SAMPLE_TO_PHYSICAL_COLUMN_MAP)
    sample = get_sample(db_conn, row['SampleNumber'])
    if (sample != None and 'phys_id' in sample):
        sql = 'update physical_data set ' + '=%s,'.join(column_names) + '=%s where id=%s'
        values.append(sample['phys_id'])
    else:
        sql = 'insert into physical_data (' + ','.join(column_names) + ') values ('+ ('%s,'*(len(values) - 1)) +'%s)'

    return sql, values, sample


# The soil and water column collection flag was not recorded for early samples
# but was recorded in the comments field instead.
SOIL_COLLECTED_RE = re.compile('(?:.*soil taken.*)|(?:.*lots of soil.*)', re.IGNORECASE)
SOIL_NOT_COLLECTED_RE = re.compile('.*no soil.*', re.IGNORECASE)
WATER_COLUMN_COLLECTED_RE = re.compile('.*water column taken.*', re.IGNORECASE)
WATER_COLUMN_NOT_COLLECTED_RE = re.compile(
    '(?:.*no water column.*)'
    '|(?:.*not deep enough for water column.*)'
    '|(?:.*Too fast flowing for water sampler.*)'
    '|(?:.*no column, too fast flowing.*)',
    re.IGNORECASE)

def set_soil_collected(row):
    set_boolean_column(row, 'SoilCollected', SOIL_COLLECTED_RE, SOIL_NOT_COLLECTED_RE)

def set_water_column_collected(row):
    set_boolean_column(row, 'WaterColumnCollected', WATER_COLUMN_COLLECTED_RE, WATER_COLUMN_NOT_COLLECTED_RE)

def set_boolean_column(row, column_name, comment_true_re, comment_false_re):
    if column_name in row:
        row[column_name] = 1 if row[column_name] == 'true' else 0
    elif comment_true_re.match(row['Comments']):
        row[column_name] = 1
    elif comment_false_re.match(row['Comments']):
        row[column_name] = 0


#-------------------------------------------------------------------------------
# IMAGE FILE PROCESSING
#-------------------------------------------------------------------------------
def process_image_files(config, db_conn, files_to_process):

    image_config = 'ImageProcessing'
    working_dir = config.get(image_config, 'working_dir')
    s3_conn = S3Connection(
        config.get(image_config, 'aws_access_key_id'),
        config.get(image_config, 'aws_secret_access_key')
        )
    s3_bucket = s3_conn.get_bucket(config.get('ImageProcessing', 's3_bucket_name'))
    s3_bucket_url = config.get(image_config, 's3_bucket_url')
    s3_folder = config.get(image_config, 's3_folder')

    cursor = db_conn.cursor()
    try:
        for image_file, image_data in files_to_process.iteritems():
            if (image_data[IMAGE_TYPE] != ''):
                sample_id = get_sample_id(db_conn, image_data[IMAGE_SAMPLE_NUMBER])
                if (sample_id != None):
                    log.debug('Processing image file ' + image_file)
                    # reduce image size
                    reduced_image_file = reduce_image(working_dir, image_file)
                    try:
                        # upload reduced image to Amazon S3 bucket
                        key = Key(s3_bucket)
                        key.key = '/'.join([s3_folder, os.path.basename(image_file)])
                        key.set_contents_from_filename(reduced_image_file)
                        key.set_metadata('Content-Type', 'image/jpeg')
                        key.make_public()
                        image_url = '/'.join([s3_bucket_url, key.key])

                        # insert image record into database
                        cursor.execute(
                            'insert into image (sample_id, image_path, image_type) values (%s, %s, %s)',
                            [sample_id, image_url, image_data[IMAGE_TYPE]])
                        db_conn.commit()

                    except Exception:
                        if key != None:
                            key.delete()
                        raise

                    finally:
                        os.remove(reduced_image_file)

    finally:
        cursor.close()


def get_sample_id(db_conn, sample_number):
    sample = get_sample(db_conn, sample_number)
    if sample == None:
        return None
    else:
        return sample['id']

def reduce_image(working_dir, raw_image_file):
    image = Image.open(raw_image_file)
    height = 300
    max_width = 400
    image.thumbnail((max_width, height), Image.ANTIALIAS)
    reduced_image_file = os.path.join(working_dir, os.path.basename(raw_image_file))
    image.save(reduced_image_file)
    return reduced_image_file

#-------------------------------------------------------------------------------
# NZGAL GEOCHEMISTRY FILE PROCESSING
#-------------------------------------------------------------------------------
def process_geochem_files(db_conn, files_to_process):
    # open all .xls files in the
    for xls_file in files_to_process:
        # open excel spreadsheet - this loads the file into memory then closes it
        workbook = xlrd.open_workbook(xls_file)
        worksheet = workbook.sheet_by_index(0)
        if (is_geochem(worksheet)):
            log.debug('Processing geochem file ' + xls_file)
            process_geochem_worksheet(db_conn, worksheet)



# geochemistry spreadsheet row -> DB chemical_data table column
GEOCHEMISTRY_COLUMN_MAP = {
    'Bicarbonate (Total)': 'bicarbonate',
    'Chloride': 'chloride',
    'Sulphate': 'sulfate',
    'Sulphide (total as H2S)': 'H2S'
}

# Matches 'P1.0023', 'P1-0023', etc
SAMPLE_NUMBER_RE = re.compile('^P1.(\d{4})$', re.IGNORECASE)
def process_geochem_worksheet(db_conn, worksheet):

    param_column = 0
    geochem_data = {}
    for col_index in range (2, worksheet.ncols):
        sample_number = None
        row_data = {}
        for row_index in range (0, worksheet.nrows):
            if (sample_number != None and worksheet.cell_type(row_index, param_column) == xlrd.XL_CELL_TEXT):
                row_data[worksheet.cell_value(row_index, param_column)] = worksheet.cell_value(row_index, col_index)

            if worksheet.cell_type(row_index, col_index) == xlrd.XL_CELL_TEXT:
                sample_match = SAMPLE_NUMBER_RE.match(worksheet.cell_value(row_index, col_index))
                if (sample_match):
                    sample_number = 'P1.' + sample_match.group(1)

        if sample_number != None and len(row_data) > 0:
            sample = get_sample(db_conn, sample_number)
            geochem_id = get_geochem_id(db_conn, sample_number)
            if sample == None:
                # Sample data not yet added, add stub
                pass
            elif 'chem_id' in sample:
                # Update to existing chemical data
                geochem_data[sample['chem_id']] = row_data
            elif 'id' in sample:
                # New chemical data
                geochem_data[sample_number] = row_data


    with db_conn:
        cursor = db_conn.cursor()
        for row_id, row_data in geochem_data.iteritems():
            geochem_id = None if SAMPLE_NUMBER_RE.match(row_id) else row_id
            sql, sql_params = get_geochem_update_sql(geochem_id, row_data)
            cursor.execute(sql, sql_params)
            if geochem_id == None:
                cursor.execute('update sample set chem_id=last_insert_id() where sample_number=%s', row_id)


def get_geochem_update_sql(geochem_id, row):

    column_names, values = get_column_names_and_values(row, GEOCHEMISTRY_COLUMN_MAP)
    if geochem_id == None:
         sql = 'insert into chemical_data (' + ','.join(column_names) + ') values ('+ ('%s,'*(len(values) - 1)) +'%s)'

    else:
        sql = 'update chemical_data set ' + '=%s,'.join(column_names) + '=%s where id=%s'
        values.append(row_id)


    return sql, values

def is_geochem(worksheet):
    return worksheet.cell_type(0, 0) == xlrd.XL_CELL_TEXT and worksheet.cell_value(0,0) == 'Geochemistry Results'


#-------------------------------------------------------------------------------
# UTILITY FUNCTIONS
#-------------------------------------------------------------------------------

def get_sample(db_conn, sample_number):
    cursor = db_conn.cursor()
    try:
        cursor.execute('select * from sample where sample_number=%s', sample_number)
        rows = cursor.fetchall()
        if len(rows) == 0:
            return None
        return dict(zip([i[0] for i in cursor.description], [i for i in rows[0]]))

    finally:
        cursor.close()

# file_path: path of tab-delimited file containing a header line of column names
#
# Returns a list where each element is a map in the form
# {column_name_1 => value_1, column_name_2 => value_2}, where the
# column names are read from the first row in the file and the values
# are read from the subsequent rows.
def get_tablet_data_rows(file_path):

    rows = []
    with codecs.open( file_path, 'r', 'utf-8') as f:
        first_line = f.readline().strip()
        column_names = first_line.split('\t')
        for line in f:
            rows.append(dict(zip(column_names,line.strip().split('\t'))))

    return rows


# row:  a map in the form {file_column_name_1 => value_1,
#       file_column_name_2 => value_2}
# column_map: map in the form {file_column_name => db_column_name}
#
# Returns a tuple containing two lists of the same length. The first is the list
# of database column names, and the second is the values for those columns
# take from the given row.
def get_column_names_and_values(row, column_map):
    column_names = []
    values = []
    for key, value in row.items():
        if value != None and key in column_map:
            column_names.append(column_map[key])
            values.append(value)

    return column_names, values

def send_error_notification(log_file_name, config):
    try:
        send_email(
            log_file_name,
            "1000 Springs data upload error",
            config.get('ScriptErrorEmail', 'from'),
            re.split('\s*,\s*', config.get('ScriptErrorEmail', 'to_csv')),
            config.get('ScriptErrorEmail', 'host'))

    except BaseException as e:
        log.error("Failed to send error notification");
        log.exception(e)

# email_to is an array of email addresses
def send_email(message_file_name, subject, email_from, email_to, smtp_host):

    fp = open(message_file_name, 'rb')
    msg = MIMEText(fp.read())
    fp.close()

    msg['Subject'] = subject
    msg['From'] = email_from
    msg['To'] = ", ".join(email_to)

    s = smtplib.SMTP(smtp_host)
    s.sendmail(email_from, email_to, msg.as_string())
    s.quit()


def init_logging(config):
    # Required to prevent duplicate log messages when the script
    # is run repeatedly in PyScripter (testing)
    log.handlers = []

    today = date.today().strftime('%Y-%m-%d')
    log_dir = config.get('Logging', 'dir')
    level = config.get('Logging', 'level')
    if not os.path.isdir(log_dir):
        script_dir = os.path.dirname(os.path.realpath(__file__))
        print "Log dir '"+log_dir+"' not found, logging to "+script_dir
        log_dir = script_dir

    log_file_name = os.path.join(log_dir, "tablet_data_uploader_"+today+".log")
    formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
    log.setLevel(level)

    # logging to file
    fh = logging.FileHandler(log_file_name)
    fh.setLevel(level)
    fh.setFormatter(formatter)
    log.addHandler(fh)

    # logging to std out - for dev only
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level)
    ch.setFormatter(formatter)
    log.addHandler(ch)

    return fh

def load_config(config_file):
    script_dir = os.path.dirname(os.path.realpath(__file__))
    config = ConfigParser.ConfigParser()
    config.read(os.path.join(script_dir, config_file))
    return config

def db_connect(config):
    db_section = 'DB'
    return MySQLdb.connect(
        host=config.get(db_section, 'host'),
        user=config.get(db_section, 'user'),
        passwd=config.get(db_section, 'password'),
        db=config.get(db_section, 'db'),
        charset='utf8'
        )

MOUNT_SECTION = 'DataShare'
def mount_data_share(config):
    global MOUNT_SECTION

    use_local_dir = config.get(MOUNT_SECTION, 'use_local_dir')
    if (use_local_dir != ''):
        return use_local_dir

    mount_drive = config.get(MOUNT_SECTION, 'mount_drive')
    local_path = mount_drive + ":\\"
    if not os.path.isdir(local_path):
        mount_command = "net use "+ mount_drive +": " + config.get(MOUNT_SECTION, 'path') + " /user:" + config.get(MOUNT_SECTION, 'user')
        log.info("Mounting with "+ mount_command)
        os.system(mount_command)
        if os.path.isdir(local_path):
            log.info('Mount successful')
        else:
            raise Exception("Failed to mount data share with "+mount_command)
    else:
        log.info("Data share already mounted")

    return local_path

def get_new_files_dir(config):
    return get_sub_dir(config, 'new_files_dir')

def get_archive_dir(config):
    return get_sub_dir(config,  'archive_dir')

def get_error_dir(config):
    return get_sub_dir(config, 'error_dir')

def get_sub_dir(config, dir_type):
    global MOUNT_SECTION
    base_dir = mount_data_share(config)
    return os.path.join(base_dir, config.get(MOUNT_SECTION, dir_type))

def unmount_data_share(config):
    global MOUNT_SECTION

    use_local_dir = config.get(MOUNT_SECTION, 'use_local_dir')
    if (use_local_dir != ''):
        return

    mount_drive = config.get(MOUNT_SECTION, 'mount_drive')
    local_path = mount_drive + ":"
    if os.path.isdir(local_path):
        unmount_command = "net use /delete "+ local_path
        log.info("Unmounting data share with "+ unmount_command)
        os.system(unmount_command)
    else:
        log.info("Data share not mounted, no need to unmount")


if __name__ == '__main__':
    main()