# -*- coding: utf8
#!/usr/bin/env python

import os
import sys
import shutil
import glob
import datetime
import codecs
import csv
import uuid
import zipfile
import ConfigParser

from pymongo import Connection

from icare.person import Person
from icare.address import Address
from icare.death import Death
from icare.card import Card
from icare.drugallergy import Drugallergy
from icare.home import Home
from icare.service import Service
from icare.appointment import Appointment
from icare.accident import Accident
from icare.diagnosis_opd import Diagnosis_opd
from icare.procedure_opd import Procedure_opd
from icare.drug_opd import Drug_opd
from icare.charge_opd import Charge_opd
from icare.admission import Admission
from icare.diagnosis_ipd import Diagnosis_ipd
from icare.procedure_ipd import Procedure_ipd
from icare.drug_ipd import Drug_ipd
from icare.charge_ipd import Charge_ipd
from icare.surveillance import Surveillance
from icare.women import Women
from icare.fp import Fp
from icare.epi import Epi
from icare.nutrition import Nutrition
from icare.prenatal import Prenatal
from icare.anc import Anc
from icare.labor import Labor
from icare.postnatal import Postnatal
from icare.newborn import NewBorn
from icare.newborncare import NewBornCare
from icare.dental import Dental
from icare.specialpp import SpecialPP
from icare.ncdscreen import NcdScreen
from icare.chronic import Chronic
from icare.chronicfu import ChronicFu
from icare.labfu import LabFu
from icare.community_service import CommunityService
from icare.disability import Disability
from icare.icf import Icf
from icare.rehabilitation import Rehabilitation
from icare.village import Village
from icare.community_activity import CommunityActivity
from icare.functional import Functional
from icare.provider import Provider

from icare.utils import Utils

"""
 Get file configuration
"""

parser = ConfigParser.ConfigParser()
parser.read('config.ini')

# Location of zip file for import.
ZIP_DIR = parser.get('import', 'dir')
# Zip files list
MONGO_HOST = parser.get('mongo', 'host')
MONGO_PORT = int(parser.get('mongo', 'port'))
MONGO_DB = parser.get('mongo', 'db')
MONGO_USER = parser.get('mongo', 'user')
MONGO_PASSWORD = parser.get('mongo', 'password')


"""
Get file list
"""


def get_zip_file_list():
    #Change to import dir
    #os.chdir(ZIP_DIR)
    #Get .zip file list
    zip_files = []

    for zip_file in glob.glob(os.path.join(ZIP_DIR, '*.zip')):
        #f = ZIP_DIR + '/' + zip_file
        zip_files.append(zip_file)

    return zip_files

""" Extract file """


def do_extract(zip_file):
    """
    Do extract zip file

    :param zip_file:
    """
    zf = zipfile.ZipFile(zip_file)
    #Create extract directory
    extract_dir = ZIP_DIR + '/' + str(uuid.uuid4())
    
    #Check dir exist
    if not os.path.exists(extract_dir):
        os.mkdir(extract_dir)
    else:
        shutil.rmtree(extract_dir)
        os.mkdir(extract_dir)
    
    for file_name in zf.namelist():
        f = extract_dir + '/' + file_name
        fd = open(f, 'wb')
        fd.write(zf.read(file_name))
        fd.close()

    ## Close zip file
    zf.close()
    ## Remove zip file
    try:
        os.remove(zip_file)
    except OSError as e:
        sys.stderr.write("Error: %s - %s \r" % (e.filename, e.strerror))

    ## Return extract directory
    return extract_dir


def get_file_contents(text_file):
    """
    Read csv
    """
    ##f = open(text_file, 'rt', encoding='utf-8-sig')
    f = codecs.open(text_file, 'r')
    try:
        reader = csv.DictReader(f, delimiter='|', quoting=csv.QUOTE_NONE)
        data = []
        for row in reader:
            data.append(row)

        f.close()
        return data

    except Exception as e:
        sys.stderr.write('[FILE_CONTENT] Error: %s \r' % e)


def get_connection():
    """
    Connection to MongoDB
    """
    conn = Connection(host=MONGO_HOST, port=MONGO_PORT)
    db = conn[MONGO_DB]
    db.authenticate(MONGO_USER, MONGO_PASSWORD)
    print('[MONGO] Connected. \r')
    return db


def main():

    """
    Main modules

    """
    try:
        db = get_connection()
        zip_count = len(glob.glob1(ZIP_DIR, '*.zip'))

        utils = Utils(db)

        if zip_count:

            print('Starting...\r')
            utils.log('Starting import.')

            #Get file list
            zip_files = get_zip_file_list()
            rec_id = str(uuid.uuid4())

            try:

                for f in zip_files:

                    zip_file_name = os.path.basename(f).upper()
                    print('[IMPORT] %s' % zip_file_name)
                    utils.log('[IMPORT] %s' % zip_file_name)

                    extracted_dir = do_extract(f)
                    #count file
                    file_count = len(glob.glob1(extracted_dir, '*.txt'))

                    if file_count == 43:
                        ## get files list
                        text_files = glob.glob(os.path.join(extracted_dir, '*.txt'))

                        for text_file in text_files:
                            ##file_content = get_file_contents(text_file)
                            text_file_name = os.path.basename(text_file).upper()
                            data = get_file_contents(text_file)

                            if text_file_name == 'PERSON.TXT':
                                col = Person(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'ADDRESS.TXT':
                                col = Address(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'DEATH.TXT':
                                col = Death(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'CARD.TXT':
                                col = Card(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'DRUGALLERGY.TXT':
                                col = Drugallergy(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'HOME.TXT':
                                col = Home(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'SERVICE.TXT':
                                col = Service(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'APPOINTMENT.TXT':
                                col = Appointment(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'ACCIDENT.TXT':
                                col = Accident(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'DIAGNOSIS_OPD.TXT':
                                col = Diagnosis_opd(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'PROCEDURE_OPD.TXT':
                                col = Procedure_opd(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'DRUG_OPD.TXT':
                                col = Drug_opd(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'CHARGE_OPD.TXT':
                                col = Charge_opd(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'ADMISSION.TXT':
                                col = Admission(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'DIAGNOSIS_IPD.TXT':
                                col = Diagnosis_ipd(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'PROCEDURE_IPD.TXT':
                                col = Procedure_ipd(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'DRUG_IPD.TXT':
                                col = Drug_ipd(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'CHARGE_IPD.TXT':
                                col = Charge_ipd(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'SURVEILLANCE.TXT':
                                col = Surveillance(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'WOMEN.TXT':
                                col = Women(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'FP.TXT':
                                col = Fp(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'EPI.TXT':
                                col = Epi(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'NUTRITION.TXT':
                                col = Nutrition(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'PRENATAL.TXT':
                                col = Prenatal(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'ANC.TXT':
                                col = Anc(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'LABOR.TXT':
                                col = Labor(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'POSTNATAL.TXT':
                                col = Postnatal(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'NEWBORN.TXT':
                                col = NewBorn(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'NEWBORNCARE.TXT':
                                col = NewBornCare(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'DENTAL.TXT':
                                col = Dental(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'SPECIALPP.TXT':
                                col = SpecialPP(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'NCDSCREEN.TXT':
                                col = NcdScreen(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'CHRONIC.TXT':
                                col = Chronic(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'CHRONICFU.TXT':
                                col = ChronicFu(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'LABFU.TXT':
                                col = LabFu(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'COMMUNITY_SERVICE.TXT':
                                col = CommunityService(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'DISABILITY.TXT':
                                col = Disability(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'ICF.TXT':
                                col = Icf(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'REHABILITATION.TXT':
                                col = Rehabilitation(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'VILLAGE.TXT':
                                col = Village(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'COMMUNITY_ACTIVITY.TXT':
                                col = CommunityActivity(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'FUNCTIONAL.TXT':
                                col = Functional(db, data, rec_id)
                                col.do_import()

                            if text_file_name == 'PROVIDER.TXT':
                                col = Provider(db, data, rec_id)
                                col.do_import()

                    else:
                        utils.log('[ERROR] File total invalid')
                        sys.stderr.write('[ERROR] File total invalid \r')
                    #Remove extracted directory
                    shutil.rmtree(extracted_dir)

                    ## process data
                utils.log('Processing...')
                print('Processing...')
                utils.do_process(rec_id)
                utils.log('End.')
                print("End.\r")

            except Exception as ex:
                sys.stderr.write('[ERROR] %s' % ex.message)
                utils.log('[ERROR] %s' % ex.message)

        else:
            utils.log('Zip file not found.')
            sys.stderr.write('Zip file not found.\r')

    except Exception as ex:

        sys.stderr.write('[MONGO] %s ' % ex.message)


if __name__ == '__main__':
    main()
