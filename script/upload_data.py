#-------------------------------------------------------------------------------
# Name:        1000 Springs data uploader
# Purpose:     Upload data data to the 1000 Springs database. The data includes
#              images collected using the 1000 Springs Android tablet app
#              and test results spreadsheets from the laboratories.
#
#              Images tagged as 'BESTPHOTO' or 'BESTSKETCH' etc are uploaded to
#              an Amazon S3 bucket. Spreadsheet data is uploaded to an Amazon
#              RDS MySQL database.
#
#              If data has a record identifier (sample number or feature name)
#              which already exists, the record will be updated otherwise a
#              new record will be inserted.
#
#              If a data file contains one or more corrupt records (or if an
#              error occurs during file processing) no records from that file
#              will be uploaded - it's all or nothing.
#
#              After data upload is completed, an email summarising the record
#              upload is sent.
#
#              If an error occurs during processing, a notification email is sent
#              containing the debug log.
#
#              Script written for 32-bit Windows Python 2.7
#
# Author:      duncanw
# Created:     07/10/2013
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
notification_msg = '1000 Springs data upload results'
new_files_dir = None

def main():

    upload_error = False
    db_conn = None
    log_file = None
    global new_files_dir
    try:
        config = load_config('upload_data.cfg')
        log_file = init_logging(config)
        log.info('upload_tablet_data.py '+str(sys.argv))
        db_conn = db_connect(config)
        new_files_dir = get_new_files_dir(config)

        feature_files, sample_files, image_files, other_xls_files = find_files(new_files_dir)

        f_files_uploaded, f_files_error = process_feature_files(db_conn, feature_files)
        add_upload_summary('Feature',  f_files_uploaded, f_files_error, [])

        s_files_uploaded, s_files_error =  process_sample_files(db_conn, sample_files)
        add_upload_summary('Sample', s_files_uploaded, s_files_error, [])

        g_files_uploaded, g_files_error, g_files_skipped = process_geochem_files(db_conn, other_xls_files)
        add_upload_summary('Geochemistry', g_files_uploaded, g_files_error, g_files_skipped)

        i_files_uploaded, i_files_error, i_files_skipped, i_files_to_archive = process_image_files(config, db_conn, image_files)
        add_upload_summary('Image',  i_files_uploaded, i_files_error, i_files_skipped)

        move_files(f_files_uploaded + s_files_uploaded + i_files_uploaded + i_files_to_archive + g_files_uploaded, get_archive_dir(config))
        move_files(f_files_error + s_files_error + i_files_error + g_files_error, get_error_dir(config))

        if feature_files or sample_files or image_files or other_xls_files:
            send_upload_notification(config)

        unmount_data_share(config)

    except Exception as e:
        upload_error = True
        log.exception(e)


    finally:
        if config is not None and upload_error:
            log.info('Sending error notification')
            send_error_notification(log_file.baseFilename, config)

        log.info('upload_tablet_data.py exiting\n')
        if db_conn is not None:
            db_conn.close()
        if log_file is not None:
            log_file.close()


# keys used for attributes contained in image file names
IMAGE_SAMPLE_NUMBER = 'sample_number'
IMAGE_TYPE = 'image_type'

# Recursively looks for files in the given directory, looking for files
# with names that match expected image and data file name formats.
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

    files_uploaded = []
    files_error = []
    for feature_file in sorted(files_to_process):
        try:
            log.info('Processing feature file ' + feature_file)
            row_count = 0
            with db_conn:
                cursor = db_conn.cursor()
                rows = get_tablet_data_rows(feature_file)
                for row in rows:
                    sql, sql_params = get_location_update_sql(db_conn, row)
                    cursor.execute(sql, sql_params)
                    row_count += 1
            files_uploaded.append([feature_file, row_count])

        except Exception as e:
            log.error('Error processing feature file ' + feature_file)
            log.exception(e)
            files_error.append(feature_file)

    return files_uploaded, files_error


# data-feature spreadsheet column -> DB location table column
FEATURE_NAME_COLUMN = '#FeatureName'
FEATURE_COLUMN_MAP = {
    'GeothermalField': 'feature_system',
    'LocationLatitude': 'lat',
    'LocationLongitude': 'lng',
    'Description': 'description',
    'AccessType': 'access'
}


# row: a dict in the form {column_name_1 => value_1, column_name_2 => value_2},
#      where column_names are those from the data spreadsheet.
#
# Returns an insert or an update SQL statement and a list of parameter values
# to be inserted into the statement.
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


# Returns the location.id of the geothermal feature with the given
# location.feature_name, or None if there is no such record in the database
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

    files_uploaded = []
    files_error = []
    for sample_file in sorted(files_to_process):
        try:
            log.info('Processing sample file ' + sample_file)
            row_count = 0
            with db_conn:
                cursor = db_conn.cursor()
                rows = get_tablet_data_rows(sample_file)
                for row in rows:
                    # Note the order is important here - the sample insertion
                    # SQL uses the MySQL last_insert_id() function to get the
                    # ID of the physical_data record
                    sql, sql_params, sample = get_physical_data_insert_sql(db_conn, row)
                    cursor.execute(sql, sql_params)
                    sql, sql_params = get_sample_insert_sql(db_conn, row, sample)
                    cursor.execute(sql, sql_params)
                    row_count += 1

            files_uploaded.append([sample_file, row_count])

        except Exception as e:
            log.error('Error processing sample file ' + sample_file)
            log.exception(e)
            files_error.append(sample_file)

    return files_uploaded, files_error


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

# row: a dict in the form {column_name_1 => value_1, column_name_2 => value_2},
#      where column_names are those from the DATA SPREADSHEET.
# sample: a dict in the form {column_name_1 => value_1, column_name_2 => value_2},
#      where column_names are those from the DATABASE.
#
# Returns an insert or an update SQL statement and a list of parameter values
# to be inserted into the statement.
def get_sample_insert_sql(db_conn, row, sample):

    # Older files have a different date format, need to canonicalise it
    survey_date = row['SurveyDate']
    if DATE_NO_SECONDS_RE.match(survey_date):
        row['SurveyDate'] = datetime.strptime(survey_date, DATE_NO_SECONDS_FORMAT).strftime(DATE_FORMAT)

    column_names, values = get_column_names_and_values(row, SAMPLE_COLUMN_MAP)
    feature_name = row[FEATURE_NAME_COLUMN]
    feature_id = get_feature_id(db_conn, feature_name)

    if feature_id != None:
        if sample == None:
            # Assume the physical_data row will be inserted immediately before this
            # sample row is inserted
            sql = 'insert into sample (phys_id,' + ','.join(column_names) + ',location_id) values (last_insert_id(),'+ ('%s,'*len(values)) +'%s)'
            values.append(feature_id)
        elif sample['phys_id'] != None:
            # sample and physical data records already exist, perform update
            sql = 'update sample set ' + '=%s,'.join(column_names) + '=%s, location_id=%s where id=%s'
            values.append(feature_id)
            values.append(sample['id'])
        else:
             # Update existing sample record, assume physical_data record inserted immediately before this
            sql = 'update sample set ' + '=%s,'.join(column_names) + '=%s, phys_id=last_insert_id(), location_id=%s where id=%s'
            values.append(feature_id)
            values.append(sample['id'])

    else:
        if sample == None:
            sql = 'insert into sample (phys_id,' + ','.join(column_names) + ') values (last_insert_id(),'+ ('%s,'*(len(values) - 1)) +'%s)'
        elif sample['phys_id'] != None:
            sql = 'update sample set ' + '=%s,'.join(column_names) + '=%s where id=%s'
            values.append(sample['id'])
        else:
            sql = 'update sample set ' + '=%s,'.join(column_names) + '=%s, phys_id=last_insert_id() where id=%s'
            values.append(sample['id'])

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

# row: a dict in the form {column_name_1 => value_1, column_name_2 => value_2},
#      where column_names are those from the data spreadsheet.
#
# Returns an insert or an update SQL statement and a list of parameter values
# to be inserted into the statement.
def get_physical_data_insert_sql(db_conn, row):

    colourData = COLOUR_RE.match(row['ColourRgbHex'])
    row['ColourRgbHex'] = colourData.group(1) if colourData else None

    set_soil_collected(row)
    set_water_column_collected(row)

    column_names, values = get_column_names_and_values(row, SAMPLE_TO_PHYSICAL_COLUMN_MAP)
    sample = get_sample(db_conn, row['SampleNumber'])
    if (sample != None and sample['phys_id']):
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

    files_uploaded = []
    files_error = []
    files_skipped = []
    files_to_archive = []
    for image_file, image_data in files_to_process.iteritems():
        if (image_data[IMAGE_TYPE] != ''):
            sample_id = get_sample_id(db_conn, image_data[IMAGE_SAMPLE_NUMBER])
            if (sample_id != None):
                upload_image(db_conn, working_dir, image_file, image_data,
                             sample_id, s3_bucket, s3_folder, s3_bucket_url,
                             files_uploaded, files_error)
            else:
                # sample not in the database...ignore
                files_skipped.append(image_file)

        else:
            # Image not uploaded to S3, but put in files_to_archive so
            # the file is moved to the archive folder
            files_to_archive.append(image_file)

    return files_uploaded, files_error, files_skipped, files_to_archive


# Returns the sample.id of the sample with the given
# sample.sample_number, or None if there is no such record in the database
def get_sample_id(db_conn, sample_number):
    sample = get_sample(db_conn, sample_number)
    if sample == None:
        return None
    else:
        return sample['id']


# working_dir: absolute path of directory for putting reduced images in.
# raw_image_file: absolute path of an image file.
#
# Returns the absolute path of a new image file with reduced size and resolution
# for easier use on the web.
def reduce_image(working_dir, raw_image_file):
    image = Image.open(raw_image_file)
    height = 300
    max_width = 400
    image.thumbnail((max_width, height), Image.ANTIALIAS)
    reduced_image_file = os.path.join(working_dir, os.path.basename(raw_image_file))
    image.save(reduced_image_file)
    return reduced_image_file


# Uploads the image_file to the given Amazon S3 bucket, and creates
# an image record in the database
def upload_image(db_conn, working_dir, image_file, image_data, sample_id,
                 s3_bucket, s3_folder, s3_bucket_url,
                 files_uploaded, files_error):

    reduced_image_file = None
    key = None
    try:
        log.info('Processing image file ' + image_file)
         # reduce image size
        reduced_image_file = reduce_image(working_dir, image_file)
        # upload reduced image to Amazon S3 bucket
        key = Key(s3_bucket)
        key.key = '/'.join([s3_folder, os.path.basename(image_file)])
        key.set_contents_from_filename(reduced_image_file)
        key.set_metadata('Content-Type', 'image/jpeg')
        key.make_public()
        image_url = '/'.join([s3_bucket_url, key.key])

        # insert image record into database
        with db_conn:
            cursor = db_conn.cursor()
            sql, sql_params = get_image_data_insert_sql(db_conn, sample_id, image_url, image_data)
            cursor.execute(sql, sql_params)

        files_uploaded.append(image_file)

    except Exception as e:
        log.error('Error processing image file ' + image_file)
        log.exception(e)
        files_error.append(image_file)
        if key != None:
            key.delete()

    finally:
        if reduced_image_file != None:
            os.remove(reduced_image_file)


# sample_id: sample.id of the sample the image is associated with.
# image_url: Amazon S3 URL of the image.
# image_data: dict containing the image type, e.g {image_type: 'BESTPHOTO'}
#
# Returns an insert or an update SQL statement and a list of parameter values
# to be inserted into the statement.
def get_image_data_insert_sql(db_conn, sample_id, image_url, image_data):

    image_id = get_image_id(db_conn, sample_id,  image_data[IMAGE_TYPE])
    if (image_id != None):
        sql = 'update image set image_path=%s where id=%s'
        values = [image_url, image_id]
    else:
        sql = 'insert into image (sample_id, image_path, image_type) values (%s, %s, %s)'
        values = [sample_id, image_url, image_data[IMAGE_TYPE]]

    return sql, values


# Returns the image.id of the image record with the given
# image.sample_id and image.image_type, or None if there is no such record in the database
def get_image_id(db_conn, sample_id, image_type):
    cursor = db_conn.cursor()
    try:
        cursor.execute('select id from image where sample_id=%s and image_type=%s', [sample_id, image_type])
        rows = cursor.fetchall()
        if len(rows) == 0:
            return None

        return rows[0][0]

    finally:
        cursor.close()



#-------------------------------------------------------------------------------
# NZGAL GEOCHEMISTRY FILE PROCESSING
#-------------------------------------------------------------------------------
def process_geochem_files(db_conn, files_to_process):
    files_uploaded = []
    files_error = []
    files_skipped = []
    for xls_file in files_to_process:
        # open excel spreadsheet - this loads the file into memory then closes it
        try:
            workbook = xlrd.open_workbook(xls_file)
            worksheet = workbook.sheet_by_index(0)
            if (is_geochem(worksheet)):
                log.info('Processing geochem file ' + xls_file)
                row_count = process_geochem_worksheet(db_conn, worksheet, get_relative_path(xls_file))
                if row_count == 0:
                    files_skipped.append(xls_file)
                else:
                    files_uploaded.append([xls_file, row_count])

            else:
                files_skipped.append(xls_file)

        except Exception as e:
            log.error('Error processing geochemistry file ' + xls_file)
            log.exception(e)
            files_error.append(xls_file)

    return files_uploaded, files_error, files_skipped


# geochemistry spreadsheet row -> DB chemical_data table column
GEOCHEMISTRY_COLUMN_MAP = {
    'Bicarbonate (Total)': 'bicarbonate',
    'Chloride': 'chloride',
    'Sulphate': 'sulfate',
    'Sulphide (total as H2S)': 'H2S'
}

# Matches 'P1.0023', 'P1-0023', etc
SAMPLE_NUMBER_RE = re.compile('^P1.(\d{4})$', re.IGNORECASE)

# Matches '1234', '1.234', '.1234', etc
NUMERIC_RE= re.compile('^[0-9]+|(?:[0-9]*\.[0-9]+)$')

# Matches '<1234', '<1.234', '<.1234', etc
BELOW_DETECTION_LIMIT_RE = re.compile('^<([0-9]+|(?:[0-9]*\.[0-9]+))$')

# worksheet: xlrd worksheet instance created from an Excel workbook
# file_name: absolute path of the Excel workbook, just used for error logging.
#
# Parses the given worksheet, and inserts relevant results into the database.
# Returns the number of records inserted or updated.
def process_geochem_worksheet(db_conn, worksheet, file_name):

    param_column = 0
    geochem_updates = []
    for col_index in range (2, worksheet.ncols):
        sample_number = None
        row_data = {}
        for row_index in range (0, worksheet.nrows):
            parameter_name = worksheet.cell_value(row_index, param_column)
            if (sample_number != None and parameter_name in GEOCHEMISTRY_COLUMN_MAP):
                # result values should be '[numeric value]' or '<[numeric value]
                # if the concentration is less than the detection limit.
                # Values below the detection limit are recorded as negative
                # numbers in the database, e.g '<0.2' is recorded as '-0.2'
                result = str(worksheet.cell_value(row_index, col_index))
                if NUMERIC_RE.match(result):
                    row_data[parameter_name] = result
                else:
                    bdl = BELOW_DETECTION_LIMIT_RE.match(result)
                    if bdl:
                        row_data[parameter_name] = '-' + bdl.group(1)
                    else:
                        raise Exception(
                            'Unexpected result value "'+result+'" for sample '
                            + sample_number + ' in ' + file_name
                            + ' cell ['+row_index+','+col_index+']')


            if worksheet.cell_type(row_index, col_index) == xlrd.XL_CELL_TEXT:
                sample_match = SAMPLE_NUMBER_RE.match(worksheet.cell_value(row_index, col_index))
                if (sample_match):
                    sample_number = 'P1.' + sample_match.group(1)

        if sample_number != None and len(row_data) > 0:
            sample = get_sample(db_conn, sample_number)
            update_data = {
                'sample_number': sample_number,
                'sample_id': None,
                'chem_id': None,
                'row_data': row_data
            }

            if sample != None:
                update_data['sample_id'] = sample['id']
                if sample['chem_id'] != None:
                    update_data['chem_id'] = sample['chem_id']

            geochem_updates.append(update_data)

    row_count = 0
    with db_conn:
        cursor = db_conn.cursor()
        for update_data in geochem_updates:
            sql, sql_params = get_geochem_update_sql(update_data['chem_id'], update_data['row_data'])
            cursor.execute(sql, sql_params)
            if update_data['chem_id'] == None:
                if update_data['sample_id'] == None:
                    cursor.execute(
                        'insert into sample (sample_number, chem_id, date_gathered, sampler) values (%s, last_insert_id(), now(), %s)',
                        [update_data['sample_number'], 'Unknown']
                        )
                else:
                    cursor.execute('update sample set chem_id=last_insert_id() where id=%s', update_data['sample_id'])
            row_count += 1

    return row_count


# row: a dict in the form {column_name_1 => value_1, column_name_2 => value_2},
#      where column_names are those from the data spreadsheet.
#
# Returns an insert or an update SQL statement and a list of parameter values
# to be inserted into the statement.
def get_geochem_update_sql(geochem_id, row):

    column_names, values = get_column_names_and_values(row, GEOCHEMISTRY_COLUMN_MAP)
    if geochem_id == None:
         sql = 'insert into chemical_data (' + ','.join(column_names) + ') values ('+ ('%s,'*(len(values) - 1)) +'%s)'

    else:
        sql = 'update chemical_data set ' + '=%s,'.join(column_names) + '=%s where id=%s'
        values.append(geochem_id)

    return sql, values


def is_geochem(worksheet):
    return worksheet.cell_type(0, 0) == xlrd.XL_CELL_TEXT and worksheet.cell_value(0,0) == 'Geochemistry Results'


#-------------------------------------------------------------------------------
# UTILITY FUNCTIONS
#-------------------------------------------------------------------------------

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
        if value != None and value != '' and key in column_map:
            column_names.append(column_map[key])
            values.append(value)

    return column_names, values


# file_type: e.g 'Feature' or 'Sample'
# Adds file upload statistics to the email notification sent out for the upload.
def add_upload_summary(file_type, files_uploaded, files_error, files_skipped):
    indent = '  '
    add_to_notification('\n' + file_type + ' file upload summary')

    add_to_notification(indent + 'Files uploaded: '+ str(len(files_uploaded)))
    add_file_list(indent*2, files_uploaded)

    if len(files_error) > 0:
        add_to_notification(indent + 'Files not uploaded due to errors: '+ str(len(files_error)))
        add_file_list(indent*2, files_error)

    if len(files_skipped) > 0:
        add_to_notification(indent + 'Files skipped due to unrecognized format or unmatched identifier: '+ str(len(files_skipped)))
        add_file_list(indent*2, files_skipped)


# Adds a list of files and the number of records retrieved from each to the
# email notification sent out for the upload.
def add_file_list(indent, file_list):
    for file_data in file_list:
        # file_data should be either the full file path or a two element array
        # in the form [full file path, record count]
        if isinstance(file_data, basestring):
            add_to_notification(indent + get_relative_path(file_data))
        else:
            rec = ' records' if file_data[1] != 1 else ' record'
            add_to_notification(indent + get_relative_path(file_data[0]) + ': '+ str(file_data[1]) + rec)


# Adds the given line to the email notification sent out for the upload.
def add_to_notification(msg_line):
    global notification_msg;
    notification_msg = '\n'.join([notification_msg, msg_line])

# sample_number: e.g 'P1.0023'
# Returns the sample record with the given sample_number, or None if no such
# record exists. Returned value is a dict of columns from the DB, e.g:
#   {
#     id: 1583
#     sample_number: 'P1.0023'
#     location_id: 916
#     ...
#  }
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

def send_error_notification(log_file_name, config):
    try:
        fp = open(log_file_name, 'rb')
        msg = fp.read()
        fp.close()

        send_email(
            msg,
            "1000 Springs data upload error",
            'error_to_csv',
            config
            )

    except Exception as e:
        log.error("Failed to send error notification")
        log.exception(e)


def send_upload_notification(config):

    global notification_msg
    send_email(
        notification_msg,
        "1000 Springs data upload complete",
        'upload_stats_to_csv',
        config
        )


def send_email(message, subject, email_to_config_key, config):

    email_from =  config.get('Email', 'from')
    email_to = config.get('Email', email_to_config_key)
    msg = MIMEText(message)
    msg['Subject'] = subject
    msg['From'] = email_from
    msg['To'] = email_to

    s = smtplib.SMTP(config.get('Email', 'host'))
    s.sendmail(email_from, re.split('\s*,\s*', email_to), msg.as_string())
    s.quit()


# Returns the path of the given file relative to the file import directory.
# e.g if the file_name is:
#  'C:\tmp\data\Incoming\2013091303 1000 Project - Waiotapu.xls
# and the import directory is
#  'C:\tmp\data\Incoming',
# the value returned would be:
# '2013091303 1000 Project - Waiotapu.xls'
def get_relative_path(file_name):
    if (file_name.startswith(new_files_dir)):
        return file_name[len(new_files_dir) + 1:]
    else:
        return file_name


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
        os.system(mount_command + " " + config.get(MOUNT_SECTION, 'password'))
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

def move_files(file_list, output_dir):
    for file_data in file_list:
        if isinstance(file_data, basestring):
            source_file = file_data
        else:
            source_file = file_data[0]

        target_file = os.path.join(output_dir, get_relative_path(source_file))
        folder = os.path.dirname(target_file)
        if not os.path.isdir(folder):
            os.makedirs(folder)

        orig_target_file = target_file
        i = 1
        while os.path.isfile(target_file):
            target_file = orig_target_file + ' (' + str(i) +')'
            i += 1

        os.rename(source_file, target_file)

        source_dir = os.path.dirname(source_file)
        if source_dir != new_files_dir and len(os.listdir(source_dir)) == 0:
            os.rmdir(source_dir)


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