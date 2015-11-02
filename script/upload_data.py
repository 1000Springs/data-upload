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
import base64
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
import time

import MySQLdb
from PIL import Image
from PIL import ExifTags
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import xlrd
import httplib

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
        log.info('upload_data.py '+str(sys.argv))
        db_conn = db_connect(config)
        new_files_dir = get_new_files_dir(config)

        feature_files, sample_files, image_files, other_xls_files, thumbsdb_cruft_files, dna_sequence_files = find_files(new_files_dir)

        f_files_uploaded, f_files_error = process_feature_files(db_conn, feature_files)
        add_upload_summary('Feature',  f_files_uploaded, f_files_error, [])

        s_files_uploaded, s_files_error =  process_sample_files(db_conn, sample_files)
        add_upload_summary('Sample', s_files_uploaded, s_files_error, [])

        g_files_uploaded, g_files_error, g_files_skipped = process_geochem_files(db_conn, other_xls_files)
        t_files_uploaded, t_files_error, t_files_skipped = process_taxonomy_files(db_conn, other_xls_files)
        xls_files_skipped = [f for f in g_files_skipped if f in t_files_skipped]
        add_upload_summary('Geochemistry', g_files_uploaded, g_files_error, xls_files_skipped)
        add_upload_summary('Taxonomy', t_files_uploaded, t_files_error, [])

        d_files_uploaded, d_files_error = process_dna_sequence_files(db_conn, dna_sequence_files)
        add_upload_summary('DNA sequence', d_files_uploaded, d_files_error, [])

        i_files_uploaded, i_files_error, i_files_skipped, i_files_to_archive = process_image_files(config, db_conn, image_files)
        add_upload_summary('Image',  i_files_uploaded, i_files_error, i_files_skipped)

        move_files(f_files_uploaded + s_files_uploaded + i_files_uploaded + i_files_to_archive + g_files_uploaded + t_files_uploaded + d_files_uploaded, get_archive_dir(config))
        move_files(f_files_error + s_files_error + i_files_error + g_files_error + t_files_error + d_files_error, get_error_dir(config))

        if feature_files or sample_files or image_files or other_xls_files or dna_sequence_files:
            send_upload_notification(config)

        process_thumbsdb_cruft_files(thumbsdb_cruft_files)

        unmount_data_share(config)

        host = config.get('Website', 'host')
        clear_cache = len(t_files_uploaded) > 0 or (len(sys.argv) > 1 and sys.argv[1].lower() == 'reload')
        init_cache = clear_cache or len(sys.argv) > 1
        if clear_cache:
            clear_caches(host)

        if init_cache:
            init_caches(host, db_conn)
            msg = 'Cache reload complete' if clear_caches else 'Cache init complete'
            send_email(
                msg,
                "1000 Springs cache init complete",
                'cache_refreshed_to_csv',
                config
                )

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
    thumbsdb_cruft_file_re = re.compile('Thumbs\.db')
    image_file_re = re.compile('(P1\.\d{4})_([A-Z]*)_\d+\.jpg', re.IGNORECASE)
    dna_sequence_file_re = re.compile('^.*\.fasta$')
    feature_files = []
    sample_files = []
    other_xls_files = []
    thumbsdb_cruft_files = []
    image_files = {}
    dna_sequence_files = []
    for dirpath, dirnames, filenames in os.walk(new_files_dir):
        for filename in filenames:
            file_path = os.path.join(dirpath,filename)
            if (feature_file_re.match(filename)):
                feature_files.append(file_path)

            elif (sample_file_re.match(filename)):
                sample_files.append(file_path)

            elif (other_xls_file_re.match(filename)):
                other_xls_files.append(file_path)

            elif (dna_sequence_file_re.match(filename)):
                dna_sequence_files.append(file_path)

            elif (thumbsdb_cruft_file_re.match(filename)):
                thumbsdb_cruft_files.append(file_path)

            else:
                image = image_file_re.match(filename)
                if (image):
                    image_files[file_path] = {
                        IMAGE_SAMPLE_NUMBER: image.group(1),
                        IMAGE_TYPE: image.group(2)
                    }

    return feature_files, sample_files, image_files, other_xls_files, thumbsdb_cruft_files, dna_sequence_files


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
    'AccessType': 'access',
    'District': 'district',
    'Location': 'location',
    'FeatureType': 'feature_type'
}


# row: a dict in the form {column_name_1 => value_1, column_name_2 => value_2},
#      where column_names are those from the data spreadsheet.
#
# Returns an insert or an update SQL statement and a list of parameter values
# to be inserted into the statement.
def get_location_update_sql(db_conn, row):

    column_names, values = get_column_names_and_values(row, FEATURE_COLUMN_MAP)
    feature_name = row[FEATURE_NAME_COLUMN]
    observation_id = get_observation_id(feature_name)
    feature_id = get_feature_id(db_conn, observation_id)

    if feature_id != None:
        sql = 'update location set ' + '=%s,'.join(column_names) + '=%s, observation_id=%s where id=%s'
        values.append(observation_id)
        values.append(feature_id)
    else:
        sql = 'insert into location (' + ','.join(column_names) + ', feature_name, observation_id) values ('+ ('%s,'*(len(values) + 1)) +'%s)'
        values.append(feature_name)
        values.append(observation_id)

    return sql, values


# Returns the location.id of the geothermal feature with the given
# location.feature_name, or None if there is no such record in the database
def get_feature_id(db_conn, observation_id):
    cursor = db_conn.cursor()
    try:
        cursor.execute('select id from location where observation_id=%s', observation_id)
        rows = cursor.fetchall()
        if len(rows) == 0:
            return None

        return rows[0][0]

    finally:
        cursor.close()

# Returns a base-64 encoded version of the feature name. This is used as an ID
# to link samples to features (as the feature name may get changed for better
# presentation in the website).
def get_observation_id(feature_name):
    return base64.b64encode(feature_name)[:80]

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
    feature_id = get_feature_id(db_conn, get_observation_id(feature_name))

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
    'WaterColumnCollected': 'waterColumnCollected',
    'TotalDissolvedSolids': 'tds',
    'SettledAt4oC': 'settledAtFourDegC'
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
    set_settled_at_4_deg(row)

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

def set_settled_at_4_deg(row):
    column_name = 'SettledAt4oC'
    if column_name in row:
        row[column_name] = 1 if row[column_name].lower() == 'true' else 0

def set_boolean_column(row, column_name, comment_true_re, comment_false_re):
    if column_name in row:
        row[column_name] = 1 if row[column_name].lower() == 'true' else 0
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
    watermark_file = config.get(image_config, 'watermark_file')
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
    for raw_image_file, image_data in files_to_process.iteritems():
        if (image_data[IMAGE_TYPE] == 'BESTPHOTO'):
            sample_id = get_sample_id(db_conn, image_data[IMAGE_SAMPLE_NUMBER])
            if (sample_id != None):
                # reduce image size
                image_type = 'BESTPHOTO'
                new_image_file = os.path.join(working_dir, os.path.basename(raw_image_file))
                reduce_image(raw_image_file, new_image_file, 400, 300, None)
                small_uploaded = upload_image(db_conn, new_image_file, image_data,
                             sample_id, s3_bucket, s3_folder, s3_bucket_url)

                image_data[IMAGE_TYPE]='LARGE'
                new_image_file = os.path.join(working_dir, os.path.basename(raw_image_file).replace('BESTPHOTO', image_data[IMAGE_TYPE]))
                reduce_image(raw_image_file, new_image_file, 900, 676, watermark_file)
                large_uploaded = upload_image(db_conn, new_image_file, image_data,
                             sample_id, s3_bucket, s3_folder, s3_bucket_url)

                if small_uploaded and large_uploaded:
                    files_uploaded.append(raw_image_file)
                else:
                    files_error.append(raw_image_file)
            else:
                # sample not in the database...ignore
                files_skipped.append(raw_image_file)

        else:
            # Image not uploaded to S3, but put in files_to_archive so
            # the file is moved to the archive folder
            files_to_archive.append(raw_image_file)

    return files_uploaded, files_error, files_skipped, files_to_archive


# Returns the sample.id of the sample with the given
# sample.sample_number, or None if there is no such record in the database
def get_sample_id(db_conn, sample_number):
    sample = get_sample(db_conn, sample_number)
    if sample == None:
        return None
    else:
        return sample['id']


# raw_image_file: absolute path of an image file.
# new image_file: absolute path of file to save the reduced image to
# max_width: maximum width (in pixels) of the reduced image
# height: height (in pixels) of the reduced image
# watermark_file: path to file to use to watermark the reduced image, or None
#                 if no watermark is to be applied
def reduce_image(raw_image_file, new_image_file, max_width, height, watermark_file):
    image = Image.open(raw_image_file)
    exif_data = image._getexif()

    image.thumbnail((max_width, height), Image.ANTIALIAS)

    # Where the image is in portrait or upside down, we rotate it so it
    # is displayed properly on the website
    # See http://www.impulseadventure.com/photo/exif-orientation.html
    # 274 is the Exif tag for orientation data.
    if exif_data != None and 274 in exif_data:
        orientation = exif_data[274]
        if (orientation == 8):
            image = image.rotate(90)
        elif (orientation == 3):
            image = image.rotate(180)
        elif (orientation == 6):
            image = image.rotate(270)

    if watermark_file != None:
        image = image.convert('RGBA')
        im_width, im_height = image.size
        layer = Image.new('RGBA', image.size, (0,0,0,0))
        watermark = Image.open(watermark_file).convert('RGBA')
        wm_width, wm_height = watermark.size
        padding = 30
        layer.paste(watermark, (im_width - (wm_width + padding), im_height - (wm_height+padding)))
        image = Image.composite(layer, image, layer)

    image.save(new_image_file)


# Uploads the image_file to the given Amazon S3 bucket, and creates
# an image record in the database
#
# Returns True if both the S3 upload and DB update are successful, otherwise
# returns False.
def upload_image(db_conn, image_file, image_data, sample_id,
                 s3_bucket, s3_folder, s3_bucket_url):

    key = None
    image_uploaded = False
    try:
        log.info('Processing image file ' + image_file)
        # upload reduced image to Amazon S3 bucket
        key = Key(s3_bucket)
        key.key = '/'.join([s3_folder, os.path.basename(image_file)])
        # Encourage browser caching of up to 10 days
        key.metadata.update({
            'Content-Type': 'image/jpeg',
            'Cache-Control': 'max-age=864000'
        })
        key.set_contents_from_filename(image_file)
        key.make_public()
        image_url = '/'.join([s3_bucket_url, key.key])

        # insert image record into database
        with db_conn:
            cursor = db_conn.cursor()
            sql, sql_params = get_image_data_insert_sql(db_conn, sample_id, image_url, image_data)
            cursor.execute(sql, sql_params)

        image_uploaded = True

    except Exception as e:
        log.error('Error processing image file ' + image_file)
        log.exception(e)
        if key != None:
            key.delete()

    finally:
        if image_file != None:
            os.remove(image_file)

    return image_uploaded


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
# GEOCHEMISTRY FILE PROCESSING
#-------------------------------------------------------------------------------
def process_geochem_files(db_conn, files_to_process):
    files_uploaded = []
    files_error = []
    files_skipped = []
    for xls_file in files_to_process:
        # open excel spreadsheet - this loads the file into memory then closes it
        try:
            # xlrd can't handle formatting info from Excel 2007+ workbooks
            is_xlsx_file = xls_file.endswith('.xlsx')
            workbook = xlrd.open_workbook(xls_file, formatting_info=(not is_xlsx_file))
            worksheet = workbook.sheet_by_index(0)
            row_count = 0
            if is_nzgal_geochem(worksheet):
                log.info('Processing NZGAL geochem file ' + xls_file)
                row_count = process_nzgal_geochem_worksheet(db_conn, worksheet, get_relative_path(xls_file), workbook)
            # UoW worksheets use formatted values, so we can only process them if in .xls format
            elif not is_xlsx_file and is_uow_geochem(worksheet):
                log.info('Processing UoW geochem file ' + xls_file)
                row_count = process_uow_geochem_worksheet(db_conn, worksheet, get_relative_path(xls_file), workbook)

            if row_count == 0:
                files_skipped.append(xls_file)
            else:
                files_uploaded.append([xls_file, row_count])


        except Exception as e:
            log.error('Error processing geochemistry file ' + xls_file)
            log.exception(e)
            files_error.append(xls_file)

    return files_uploaded, files_error, files_skipped


# geochemistry spreadsheet row -> DB chemical_data table column
GEOCHEMISTRY_COLUMN_MAP = {
     # NZGAL data
    'Bicarbonate (Total)': 'bicarbonate',
    'Chloride': 'chloride',
    'Sulphate': 'sulfate',
    'Sulphide (total as H2S)': 'H2S',

    # UoW FIA data
    'NH4': 'ammonium',
    'PO4': 'phosphate',
    'NO3': 'nitrate',
    'NO2': 'nitrite',

    #UoW ICP-MS data
    'B 10': 'B',
    'Na 23': 'Na',
    'Mg 24': 'Mg',
    'Al 27': 'Al',
    'K 39': 'K',
    'Ca 43': 'Ca',
    'V 51': 'V',
    'Cr 52': 'Cr',
    'Fe 54': 'Fe',
    'Mn 55': 'Mn',
    'Co 59': 'cobalt',
    'Ni 60': 'Ni',
    'Cu 63': 'Cu',
    'Cu 65': 'Cu',
    'Zn 66': 'Zn',
    'As 75': '`As`', # Needs back-tick escaping, since As is an SQL keyword
    'Se 82': 'Se',
    'Sr 88': 'Sr',
    'Ag 109': 'Ag',
    'Cd 111': 'Cd',
    'Ba 137': 'Ba',
    'Li 7': 'Li',
    'Si 28': 'Si',
    'Fe 54': 'Fe',
    'Br 79': 'Br',
    'Mo 98': 'Mo',
    'La 139': 'La',
    'Tl 205': 'thallium',
    'Pb 207': 'Pb',
    'U 238': 'U',
    'S 32': 'S',
    'Rb 85': 'Rb',
    'Cs 133': 'Cs',
    'Hg 202': 'Hg',

    # Other
    'iron2': 'iron2',
    'H2': 'H2',
    'CO': 'CO',
    'CH4': 'CH4'
}

# Matches 'P1.0023', 'P1-0023', etc
SAMPLE_NUMBER_RE = re.compile('^\s*P1.(\d{4})\s*$', re.IGNORECASE)

# Matches '1234', '1.234', '.1234', etc
NUMERIC_RE= re.compile('^-{0,1}(?:[0-9]+|(?:[0-9]*\.[0-9]+))$')

# Matches '<1234', '<1.234', '<.1234', etc
BELOW_DETECTION_LIMIT_RE = re.compile('^\s*<\s*([0-9]+|(?:[0-9]*\.[0-9]+))$')


# Returns true if the given worksheet appears to contain data in the GNS NZGAL format
def is_nzgal_geochem(worksheet):
    return worksheet.cell_type(0, 0) == xlrd.XL_CELL_TEXT and worksheet.cell_value(0,0) == 'Geochemistry Results'


# Returns true if the given worksheet appears to contain data in the Waikato University format
def is_uow_geochem(worksheet):

    first_result_row = -1
    first_result_col = -1
    for i in range(0, worksheet.nrows):
        if  SAMPLE_NUMBER_RE.match(worksheet.cell_value(i, 0)):
            first_result_row = i
            break

    for i in range(0, worksheet.ncols):
        element_name = worksheet.cell_value(0, i)
        if  GEOCHEMISTRY_COLUMN_MAP.has_key(element_name):
            first_result_col = i
            break

    if first_result_row >= 0 and first_result_col >=0:
        result = str(worksheet.cell_value(first_result_row, first_result_col))
        return interpret_geochem_result(result) != None

    return False


# worksheet: xlrd worksheet instance created from an Excel workbook
# file_name: absolute path of the Excel workbook, just used for error logging.
#
# Parses the given NZGAL format worksheet, and inserts relevant results into the database.
# Returns the number of records inserted or updated.
def process_nzgal_geochem_worksheet(db_conn, worksheet, file_name, workbook):

    param_column = 0
    geochem_updates = []
    use_formatting = False
    for col_index in range (2, worksheet.ncols):
        sample_number = None
        row_data = {}
        for row_index in range (0, worksheet.nrows):
            parameter_name = worksheet.cell_value(row_index, param_column)
            add_geochem_result(row_data, sample_number, parameter_name, worksheet, row_index, col_index, file_name, workbook, use_formatting)
            new_sample_number = get_geochem_sample_number(worksheet, row_index, col_index)
            sample_number = new_sample_number if (new_sample_number!= None) else sample_number

        add_geochem_update_data(geochem_updates, sample_number, row_data, db_conn)

    row_count = perform_geochem_updates(db_conn, geochem_updates)

    return row_count


# worksheet: xlrd worksheet instance created from an Excel workbook
# file_name: absolute path of the Excel workbook, just used for error logging.
#
# Parses the given Waikato University format worksheet, and inserts relevant results into the database.
# Returns the number of records inserted or updated.
def process_uow_geochem_worksheet(db_conn, worksheet, file_name, workbook):

    param_row = 0
    sample_num_col = 0
    geochem_updates = []
    use_formatting = True
    for row_index in range (1, worksheet.nrows):
        sample_number = get_geochem_sample_number(worksheet, row_index, sample_num_col)
        row_data = {}
        for col_index in range (1, worksheet.ncols):
            parameter_name = worksheet.cell_value(param_row, col_index)
            add_geochem_result(row_data, sample_number, parameter_name, worksheet, row_index, col_index, file_name, workbook, use_formatting)

        add_geochem_update_data(geochem_updates, sample_number, row_data, db_conn)

    row_count = perform_geochem_updates(db_conn, geochem_updates)

    return row_count


# row_data: dictionary in the form {parameter_name: result}, e.g: {NH4: 3.69, PO4: 0.343}
#
# Reads a result from the given worksheet at the specified [row, column] and
# adds it to the given row_data.
def add_geochem_result(row_data, sample_number, parameter_name, worksheet, result_row, result_col, file_name, workbook, use_formatting):
    if (sample_number != None and parameter_name in GEOCHEMISTRY_COLUMN_MAP):
        # result values should be '[numeric value]' or '<[numeric value]
        # if the concentration is less than the detection limit.
        # Values below the detection limit are recorded as negative
        # numbers in the database, e.g '<0.2' is recorded as '-0.2'
        # Values less than 0 are treated as 0

        #result = str(worksheet.cell_value(result_row, result_col))
        result = read_value(worksheet, workbook, result_row, result_col, use_formatting)
        interpreted_result = interpret_geochem_result(result)
        if result is None:
            raise Exception(
                'Unexpected '+parameter_name+' result value "'+result+'" for sample '
                + sample_number + ' in ' + file_name
                + ' cell ['+str(result_row)+','+str(result_col)+']')
        else:
            row_data[parameter_name] = interpreted_result


# Matches '0.00', '0.000', etc
NUMBER_FORMAT_RE = re.compile('^0\.(0+)$')

def read_value(worksheet, workbook, row_index, col_index, use_formatting):

    if use_formatting:
        xf_index = worksheet.cell_xf_index(row_index, col_index)
        xf = workbook.xf_list[xf_index] # gets an XF object
        format_key = xf.format_key
        formatter = workbook.format_map[format_key] # gets a Format object
        value = str(worksheet.cell_value(row_index, col_index))
        is_formatted = NUMBER_FORMAT_RE.match(formatter.format_str)
        if is_formatted:
            try:
                value = str(Decimal(value).quantize(Decimal('1.'+is_formatted.group(1)), rounding=ROUND_HALF_UP))
            except InvalidOperation:
                pass

    else:
        value = worksheet.cell_value(row_index, col_index)

    return value


# Returns:
#  '-0.004' if result = '<0.004'
#  '0.0' if result = '-0.004'
#  '0.004' if result = '0.004'
#   Any other format, returns None
def interpret_geochem_result(result):
    interpreted_result = None
    bdl = BELOW_DETECTION_LIMIT_RE.match(result)
    if bdl:
        interpreted_result = '-' + bdl.group(1)
    else:
        try:
            if float(result) > 0:
                interpreted_result = result
            else:
                interpreted_result = '0.0'
        except ValueError:
            pass

    return interpreted_result


# Reads a sample number from the given worksheet at the specified [row, column].
# Returns the sample number in the form 'P1.0123' or None if no valid sample number
# is found.
def get_geochem_sample_number(worksheet, row_index, col_index):
    sample_number = None
    if worksheet.cell_type(row_index, col_index) == xlrd.XL_CELL_TEXT:
        sample_match = SAMPLE_NUMBER_RE.match(worksheet.cell_value(row_index, col_index))
        if (sample_match):
            sample_number = 'P1.' + sample_match.group(1)

    return sample_number


# geochem_updates: list of dictionaries in the form
#     {'sample_number': e.g 'P1.0123',
#      'sample_id': SAMPLE.ID value from the database,
#      'chem_id': CHEMICAL_DATA.ID value from the database,
#      'row_data': dictionary in the form of row_data parameter below
#     }
# sample_number: e.g 'P1.0123'
# row_data: dictionary in the form {parameter_name: result}, e.g: {NH4: 3.69, PO4: 0.343}
#
# Checks the database for existing geochemistry results associated with the
# given sample number. Adds details to given geochem_updates list.
def add_geochem_update_data(geochem_updates, sample_number, row_data, db_conn):
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


# geochem_updates: list of dictionaries in the form
#     {'sample_number': e.g 'P1.0123',
#      'sample_id': SAMPLE.ID value from the database, or None,
#      'chem_id': CHEMICAL_DATA.ID value from the database, or None,
#      'row_data': dictionary in the form {parameter_name: result}, e.g: {NH4: 3.69, PO4: 0.343}
#     }
#
# Adds the given geochemistry data into the database via inserts or updates.
def perform_geochem_updates(db_conn, geochem_updates):
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


#-------------------------------------------------------------------------------
# TAXONOMY FILE PROCESSING
#-------------------------------------------------------------------------------
def process_taxonomy_files(db_conn, files_to_process):
    files_uploaded = []
    files_error = []
    files_skipped = []
    for xls_file in files_to_process:
        # open excel spreadsheet - this loads the file into memory then closes it
        try:
            # xlrd can't handle formatting info from Excel 2007+ workbooks
            is_xlsx_file = xls_file.endswith('.xlsx')
            workbook = xlrd.open_workbook(xls_file, formatting_info=(not is_xlsx_file))
            worksheet = workbook.sheet_by_index(0)
            row_count = 0
            if is_taxonomy(worksheet):
                log.info('Processing taxonomy data file ' + xls_file)
                row_count = process_taxonomy_worksheet(db_conn, worksheet, get_relative_path(xls_file), workbook)

            if row_count == 0:
                files_skipped.append(xls_file)
            else:
                files_uploaded.append([xls_file, row_count])


        except Exception as e:
            log.error('Error processing taxonomy file ' + xls_file)
            log.exception(e)
            files_error.append(xls_file)

    return files_uploaded, files_error, files_skipped

# Returns true if the given worksheet appears to contain data in the taxonomy/DNA format
def is_taxonomy(worksheet):
    taxonomy_columns, sample_columns = get_taxonomy_columns(worksheet)
    found_all_taxonomy_columns = len(taxonomy_columns) == len(TAXONOMY_COLUMN_MAP)
    found_sample_column = len(sample_columns) > 0
    return found_all_taxonomy_columns and found_sample_column

# Matches 'OTU_25', 'OTU_789' etc
OTU_ID_RE = re.compile('OTU_\d+', re.IGNORECASE)

# Matches 'P1.0023_60', 'P1.0023_64', etc
TAXONOMY_SAMPLE_NUMBER_RE = re.compile('^\s*P1.(\d{4}).*$', re.IGNORECASE)

# worksheet: xlrd worksheet instance created from an Excel workbook
# file_name: absolute path of the Excel workbook
#
# Parses the given taxonomy worksheet, and inserts relevant results into the database.
# Returns the number of records inserted or updated.
def process_taxonomy_worksheet(db_conn, worksheet, file_name, workbook):

    taxonomy_updates = []
    taxonomy_columns, sample_columns = get_taxonomy_columns(worksheet)
    # Parse worksheet contents to extract data
    for row_index in range (1, worksheet.nrows):
        otu_id = worksheet.cell_value(row_index, taxonomy_columns['otu_id'])
        if OTU_ID_RE.match(otu_id):
            taxonomy_data = {
                'otu_id': otu_id,
                'data_file_name': remove_file_type(file_name)
            }
            for db_column_name, sheet_column_index in taxonomy_columns.iteritems():
                    value = str(worksheet.cell_value(row_index, sheet_column_index))
                    if len(value) == 0:
                        value = None
                    elif db_column_name.endswith('_confidence'):
                        value = float(value)
                    taxonomy_data[db_column_name] = value

            sample_taxonomy_data = []
            for sample_number, sample_column_index in sample_columns.iteritems():
                read_count = int(worksheet.cell_value(row_index, sample_column_index))
                if read_count > 0:
                    sample_taxonomy_data.append({
                        'sample_number': sample_number,
                        'read_count':read_count
                    })

            taxonomy_updates.append({
                'taxonomy_data': taxonomy_data,
                'sample_taxonomy_data': sample_taxonomy_data
                })

    # Perform database inserts
    log.info('Finished extracting data from ' + file_name)
    row_count = perform_taxonomy_updates(db_conn, taxonomy_updates)

    return row_count

# taxonomy spreadsheet column -> DB taxonomy table column
TAXONOMY_COLUMN_MAP = {
    'OTUId': 'otu_id',
    'Domain': 'domain',
    'DomainConf': 'domain_confidence',
    'Phylum': 'phylum',
    'PhylumConf': 'phylum_confidence',
    'Class': 'class',
    'ClassConf': 'class_confidence',
    'Order': 'order',
    'OrderConf': 'order_confidence',
    'Family': 'family',
    'FamilyConf': 'family_confidence',
    'Genus': 'genus',
    'GenusConf': 'genus_confidence',
    'Species': 'species',
    'SpeciesConf': 'species_confidence'
}


# worksheet: xlrd worksheet instance created from an Excel workbook
#
# Iterates over the given worksheet's header row looking for expected column
# names. Returns a map in the form {database_column_name: worksheet_column_index}
def get_taxonomy_columns(worksheet, header_row_index = 0):

    taxonomy_columns = {}
    sample_columns = {}
    for col_index in range (0, worksheet.ncols):
        col_name = str(worksheet.cell_value(header_row_index, col_index))
        sample_number = TAXONOMY_SAMPLE_NUMBER_RE.match(col_name)
        if (sample_number):
            sample_key = 'P1.'+sample_number.group(1)
            if sample_key in sample_columns:
                log.warn('Column ' + str(col_index) + ' is a duplicate of ' + sample_key + ' at ' + str(sample_columns[sample_key]))
            else:
                sample_columns['P1.'+sample_number.group(1)] = col_index

        elif col_name in TAXONOMY_COLUMN_MAP:
            taxonomy_columns[TAXONOMY_COLUMN_MAP[col_name]] = col_index

    return taxonomy_columns, sample_columns

# taxonomy_updates: list of dictionaries in the form
#     {
#        'taxonomy_data': {
#            'otu_id': 'OTU_670',
#            'data_file_name': 'R1R2_Production_OTUtable',
#            'domain': 'Bacteria',
#            'domain_confidence': 0.99,
#            ...+ phylum, class, order etc.
#         },
#        'sample_taxonomy_data': [
#            {
#                'sample_number': 'P1.0001',
#                'read_count': '23'
#            },
#            {
#                'sample_number': 'P1.0021',
#                'read_count': '517'
#            }
#        ]
#     }
#
# Adds the given taxonomy data into the database via inserts or updates.
def perform_taxonomy_updates(db_conn, taxonomy_updates):
    row_count = 0
    with db_conn:
        cursor = db_conn.cursor()
        # Each update contains the full set of taxonomy data,
        # so clear out the taxonomy tables before refilling them.
        # Performed on the same transaction so data not absent
        # from website for extended period.
        cursor.execute('delete from sample_taxonomy')
        cursor.execute('delete from taxonomy')
        sample_id_cache={}
        for update_data in taxonomy_updates:
            # insert or update taxonomy record
            taxonomy_data =  update_data['taxonomy_data']
            sql, sql_params = get_insert_sql('taxonomy', taxonomy_data)
            cursor.execute(sql, sql_params)
            taxonomy_id = db_conn.insert_id()

            # insert sample_taxonomy records
            if len(update_data['sample_taxonomy_data']) > 0:
                sql_params = []
                for sample_taxonomy_data in update_data['sample_taxonomy_data']:
                    sample_number = sample_taxonomy_data.pop('sample_number')
                    if sample_number in sample_id_cache:
                        sample_id = sample_id_cache[sample_number]
                    else:
                        sample = get_sample(db_conn, sample_number)
                        if sample is None:
                            sample_id = insert_dummy_sample(db_conn, cursor, sample_number)
                        else:
                            sample_id = sample['id']
                        sample_id_cache[sample_number] = sample_id

                    sql_params.extend([sample_id, taxonomy_id, sample_taxonomy_data['read_count']])


                sql = 'insert into sample_taxonomy (sample_id,taxonomy_id,read_count) values (%s,%s,%s) ' + (', (%s,%s,%s)' * (len(update_data['sample_taxonomy_data']) - 1))
                cursor.execute(sql, sql_params)
                log.info('Linked ' + str(len(update_data['sample_taxonomy_data'])) + ' samples to taxonomy data ' +
                    taxonomy_data['otu_id'] + ' from ' + taxonomy_data['data_file_name'])
            else:
                log.warn('No samples found with read counts for '+ taxonomy_data['otu_id'])

            # only count the taxonomy updates, as this will match the row count in the spreadsheet
            row_count += 1

    return row_count


#-------------------------------------------------------------------------------
# DNA SEQUENCE FILE PROCESSING
#-------------------------------------------------------------------------------

# Matches '>OTU_670', '>OTU_25', etc
OTU_ID_LINE_RE = re.compile('^>(OTU_\d+)$', re.IGNORECASE)

def process_dna_sequence_files(db_conn, files_to_process):
    files_uploaded = []
    files_error = []
    for dna_sequence_file in files_to_process:
        try:
            record_count = perform_dna_sequence_updates(db_conn, dna_sequence_file)
            files_uploaded.append([dna_sequence_file, record_count])
        except Exception as e:
            log.error('Error processing DNA sequence file ' + dna_sequence_file)
            log.exception(e)
            files_error.append(dna_sequence_file)

    return files_uploaded, files_error

def perform_dna_sequence_updates(db_conn, dna_sequence_file):

    file_name = remove_file_type(dna_sequence_file)
    record_count = 0

    with db_conn:
        cursor = db_conn.cursor()
        otu_id = None
        dna_sequence = None
        with open(dna_sequence_file) as f:
            log.info('Processing DNA sequence data file ' + dna_sequence_file)
            for line in f:
                otu_id_line = OTU_ID_LINE_RE.match(line)
                if otu_id_line:
                    if otu_id is not None:
                        update_dna_sequence(cursor, file_name, otu_id, dna_sequence)
                        record_count += 1

                    otu_id = otu_id_line.group(1)
                    dna_sequence = ''
                else:
                    dna_sequence += line.strip()

        update_dna_sequence(cursor, file_name, otu_id, dna_sequence)
        record_count += 1

    return record_count


def update_dna_sequence(cursor, file_name, otu_id, dna_sequence):
    sql = 'update taxonomy set sequence=%s where data_file_name like %s and otu_id=%s'
    cursor.execute(sql, [dna_sequence, file_name + '%', otu_id])

#-------------------------------------------------------------------------------
# CACHE OPERATIONS
#-------------------------------------------------------------------------------
def clear_caches(host):
    log.info('Clearing caches')
    http_get(host, '/clearTaxonomyCache')
    http_get(host, '/clearTaxonomyOverviewCache')

def init_caches(host, db_conn):
    init_taxonomy_overview_cache(host, db_conn)
    init_taxonomy_summary_cache(host, db_conn)

def init_taxonomy_overview_cache(host, db_conn):
    log.info('Initialising taxonomy overview cache')
    cursor = db_conn.cursor()
    domains = get_single_column(db_conn, 'select distinct domain from public_taxonomy order by domain')
    phylums = get_single_column(db_conn, 'select distinct phylum from public_taxonomy order by phylum')

    for domain in domains:
        http_get(host, '/overviewTaxonGraphJson/domain/'+domain)
        time.sleep(1) # sleep for 1 second so the website doesn't fall over

    for phylum in phylums:
        http_get(host, '/overviewTaxonGraphJson/phylum/'+phylum)
        time.sleep(1)

def init_taxonomy_summary_cache(host, db_conn):
    log.info('Initialising taxonomy summary cache')
    cursor = db_conn.cursor()
    sample_numbers = get_single_column(db_conn, 'select sample_number from public_sample s order by sample_number')
    for sample_number in sample_numbers:
        http_get(host, '/taxonomyJson/'+sample_number)
        time.sleep(5) # sleep for 5 seconds so the website doesn't fall over

def get_single_column(db_conn, sql):
    cursor = db_conn.cursor()
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
        result = [row[0] for row in rows]
    finally:
        cursor.close()

    return result


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
            trimmed_line = line.strip()
            if len(trimmed_line) > 0:
                row = dict(zip(column_names,line.strip().split('\t')))
                # Values from files edited in Excel end up with surrounding quotes
                remove_string_quotes(row)
                rows.append(row)

    return rows


# row: a map in the form {key => value,
#       key => values}
#
# Removes surrounding quotes from string values.
def remove_string_quotes(row):
    for key in row:
        value = row[key]
        if isinstance(value, basestring) and value.startswith('"') and value.endswith('"'):
            row[key] = value[1:-1]


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

# table_name: database table name
# value_map: dictionary in the form {database_column_name: value_to_insert}
#
# Returns a tuple in the form (the SQL statement to execute the insert, list of insert parameters)
def get_insert_sql(table_name, value_map):
    sql = 'insert into `'+table_name+'` (`' + '`,`'.join(value_map.keys()) + '`) values ('+ ','.join(['%s']*len(value_map.keys())) +')'
    return sql, value_map.values()

# id_col_name: primary key column of database table
# id_value: primary key of row to be updated
# table name: database table name
# value_map: dictionary in the form {database_column_name: update_value}
#
# Returns a tuple in the form (the SQL statement to execute the update, list of update parameters)
def get_update_sql(id_col_name, id_value, table_name, value_map):
    sql = 'update `'+table_name+'` set `' + '`=%s,`'.join(value_map.keys()) + '`=%s where `'+id_col_name+'`=%s'
    return sql, value_map.values() + [id_value]

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
    for file_data in sorted(file_list):
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
#     id: 1583,
#     sample_number: 'P1.0023',
#     location_id: 916
#     ...
#  }
def get_sample(db_conn, sample_number):
    return get_db_row(db_conn, 'select * from sample where sample_number=%s', sample_number)

# file_name: name of the file the taxonomy data originated from (without file type suffix), e.g 'R1R2_Production_OTU'
# otu_id: e.g 'OTU_670'
# Returns the taxonomy record with the data_file_name and otu_id attributes, or None if no such
# record exists. Returned value is a dict of columns from the DB, e.g:
#   {
#     id: 7,
#     domain: 'Bacteria',
#     domain_confidence: 0.99
#     ...
#  }
def get_taxonomy(db_conn, file_name, otu_id):
    sql = 'select * from taxonomy where data_file_name like %s and otu_id=%s'
    return get_db_row(db_conn, sql, [file_name + '%', otu_id])

# sample_id: sample.id DB column value
# taxonomy_id: taxonomy.id DB column value
# Returns the sample_taxonomy record with the given attributes, or None if no such
# record exists. Returned value is a dict of columns from the DB, e.g:
#   {
#     id: 7,
#     sample_id: 15,
#     taxonomy_id: 23,
#     read_count: 568
#  }
def get_sample_taxonomy(db_conn, sample_id, taxonomy_id):
    sql = 'select * from sample_taxonomy where sample_id=%s and taxonomy_id=%s'
    return get_db_row(db_conn, sql, [sample_id, taxonomy_id])

def get_db_row(db_conn, sql, sql_params):
    cursor = db_conn.cursor()
    try:
        cursor.execute(sql, sql_params)
        rows = cursor.fetchall()
        if len(rows) == 0:
            return None
        return dict(zip([i[0] for i in cursor.description], [i for i in rows[0]]))

    finally:
        cursor.close()


# sample_number: e.g 'P1.0025'
# Inserts a sample record with just the sample number into the database's sample table
def insert_dummy_sample(db_conn, cursor, sample_number):
    cursor.execute(
        'insert into sample (sample_number, date_gathered, sampler) values (%s, now(), %s)',
        [sample_number, 'Unknown']
        )
    return db_conn.insert_id()

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


# Folders containing images often contain a Thumbs.db
# file generated by Windows, delete these so folders are
# deleted properly
def process_thumbsdb_cruft_files(thumbsdb_cruft_files):

    try:
        for f in thumbsdb_cruft_files:
            os.remove(f)
            remove_dir(f)
    except Exception as e:
        log.error('Error removing USB drive temp file')
        log.exception(e)


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
        charset='utf8',
        sql_mode='STRICT_ALL_TABLES'
        )

def http_get(host, path):
    log.debug('HTTP get: http://' + host + path)
    conn = httplib.HTTPConnection(host)
    conn.request("GET", path)
    return conn.getresponse()

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

def remove_file_type(file_name):
    return os.path.splitext(os.path.basename(file_name))[0]

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

        remove_dir(source_file)


def remove_dir(source_file):
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