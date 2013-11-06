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

import random
import string

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

FILES = []

"""
Get file list
"""


def strRandom():
    char_set = string.ascii_uppercase + string.digits
    return ''.join(random.sample(char_set*24, 24))


def get_zip_file_list():
    #Change to import dir
    #os.chdir(ZIP_DIR)
    #Get .zip file list
    zip_files = []

    for zip_file in glob.glob(os.path.join(ZIP_DIR, '*')):
        #f = ZIP_DIR + '/' + zip_file
        zip_files.append(zip_file)

    return zip_files


def do_extract(zip_file):
    """
    Do extract zip file

    :param zip_file:
    """
    zf = zipfile.ZipFile(zip_file)
    #Create extract directory
    str_random = strRandom()
    extract_dir = ZIP_DIR + '/' + str(str_random)
    """
        #Check dir exist
        if not os.path.exists(extract_dir):
            os.mkdir(extract_dir)
        else:
            shutil.rmtree(extract_dir)
            os.mkdir(extract_dir)
    """

    for file_name in zf.namelist():
        f = os.path.join(extract_dir, file_name)
        dir = os.path.dirname(f)

        if not os.path.exists(dir):
            os.makedirs(dir)

        fd = open(f, 'wb')
        fd.write(zf.read(file_name))
        fd.close()
        #print(f)
    ## Close zip file
    zf.close()
    ## Remove zip file
    try:
        os.remove(zip_file)
    except OSError as e:
        sys.stderr.write("Error: %s - %s \r" % (zip_file, e.strerror))

    ## Return extract directory
    return extract_dir


def get_file_contents(text_file):
    """
    Read csv
    """
    ##f = open(text_file, 'rt', encoding='utf-8-sig')
    f = codecs.open(text_file, 'rU')
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


def get_file_list(currentDir):

    myFiles = []

    for root, dirs, files in os.walk(currentDir):
        for f in files:
            myFiles.append(os.path.join(root, f))

    return myFiles
    

def main():

    """
    Main modules

    """
    try:
        db = get_connection()
        zip_count = len(glob.glob1(ZIP_DIR, '*'))

        #utils = Utils(db)

        if zip_count:

            print('Starting...\r')
            #utils.log('Starting import.')

            #Get file list
            zip_files = get_zip_file_list()
            rec_id = str(uuid.uuid4())

            try:

                for f in zip_files:

                    zip_file_name = os.path.basename(f).upper()
                    print('[IMPORT] %s' % zip_file_name)
                    #utils.log('[IMPORT] %s' % zip_file_name)

                    extracted_dir = do_extract(f)
                    #print(extracted_dir)
                    ## get files list
                    #text_files = glob.glob(os.path.join(extracted_dir, '*.txt'))
                    text_files = get_file_list(extracted_dir)

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

                        #if text_file_name == 'DEATH.TXT':
                        #    col = Death(db, data, rec_id)
                        #    col.do_import()

                        #if text_file_name == 'CARD.TXT':
                        #    col = Card(db, data, rec_id)
                        #    col.do_import()

                        #if text_file_name == 'DRUGALLERGY.TXT':
                        #    col = Drugallergy(db, data, rec_id)
                        #    col.do_import()

                        if text_file_name == 'HOME.TXT':
                            col = Home(db, data, rec_id)
                            col.do_import()

                        if text_file_name == 'SERVICE.TXT':
                            col = Service(db, data, rec_id)
                            col.do_import()

                        if text_file_name == 'APPOINTMENT.TXT':
                            col = Appointment(db, data, rec_id)
                            col.do_import()

                        #if text_file_name == 'ACCIDENT.TXT':
                        #    col = Accident(db, data, rec_id)
                        #    col.do_import()

                        if text_file_name == 'DIAGNOSIS_OPD.TXT':
                            col = Diagnosis_opd(db, data, rec_id)
                            col.do_import()

                        if text_file_name == 'PROCEDURE_OPD.TXT':
                            col = Procedure_opd(db, data, rec_id)
                            col.do_import()

                        if text_file_name == 'DRUG_OPD.TXT':
                            col = Drug_opd(db, data, rec_id)
                            col.do_import()

                        #if text_file_name == 'CHARGE_OPD.TXT':
                        #    col = Charge_opd(db, data, rec_id)
                        #    col.do_import()

                        #if text_file_name == 'ADMISSION.TXT':
                        #    col = Admission(db, data, rec_id)
                        #    col.do_import()

                        if text_file_name == 'DIAGNOSIS_IPD.TXT':
                            col = Diagnosis_ipd(db, data, rec_id)
                            col.do_import()

                        if text_file_name == 'PROCEDURE_IPD.TXT':
                            col = Procedure_ipd(db, data, rec_id)
                            col.do_import()

                        if text_file_name == 'DRUG_IPD.TXT':
                            col = Drug_ipd(db, data, rec_id)
                            col.do_import()

                        #if text_file_name == 'CHARGE_IPD.TXT':
                        #    col = Charge_ipd(db, data, rec_id)
                        #    col.do_import()

                        #if text_file_name == 'SURVEILLANCE.TXT':
                        #    col = Surveillance(db, data, rec_id)
                        #    col.do_import()

                        #if text_file_name == 'WOMEN.TXT':
                        #    col = Women(db, data, rec_id)
                        #    col.do_import()

                        #if text_file_name == 'FP.TXT':
                        #    col = Fp(db, data, rec_id)
                        #    col.do_import()

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

                        #if text_file_name == 'DENTAL.TXT':
                        #    col = Dental(db, data, rec_id)
                        #    col.do_import()

                        #if text_file_name == 'SPECIALPP.TXT':
                        #    col = SpecialPP(db, data, rec_id)
                        #    col.do_import()

                        #if text_file_name == 'NCDSCREEN.TXT':
                        #    col = NcdScreen(db, data, rec_id)
                        #    col.do_import()

                        #if text_file_name == 'CHRONIC.TXT':
                        #    col = Chronic(db, data, rec_id)
                        #    col.do_import()

                        #if text_file_name == 'CHRONICFU.TXT':
                        #    col = ChronicFu(db, data, rec_id)
                        #    col.do_import()

                        #if text_file_name == 'LABFU.TXT':
                        #    col = LabFu(db, data, rec_id)
                        #    col.do_import()

                        #if text_file_name == 'COMMUNITY_SERVICE.TXT':
                        #    col = CommunityService(db, data, rec_id)
                        #    col.do_import()

                        #if text_file_name == 'DISABILITY.TXT':
                        #    col = Disability(db, data, rec_id)
                        #    col.do_import()

                        #if text_file_name == 'ICF.TXT':
                        #    col = Icf(db, data, rec_id)
                        #    col.do_import()

                        #if text_file_name == 'REHABILITATION.TXT':
                        #    col = Rehabilitation(db, data, rec_id)
                        #    col.do_import()

                        if text_file_name == 'VILLAGE.TXT':
                            col = Village(db, data, rec_id)
                            col.do_import()

                        #if text_file_name == 'COMMUNITY_ACTIVITY.TXT':
                        #    col = CommunityActivity(db, data, rec_id)
                        #    col.do_import()

                        #if text_file_name == 'FUNCTIONAL.TXT':
                        #    col = Functional(db, data, rec_id)
                        #    col.do_import()

                        if text_file_name == 'PROVIDER.TXT':
                            col = Provider(db, data, rec_id)
                            col.do_import()
                    #Remove extracted directory
                    shutil.rmtree(extracted_dir)

            except Exception as ex:
                sys.stderr.write('Error: %s' % ex.message)

            #utils.log('Processing...')
            print('CID Mapping...')
            #utils.do_process(rec_id)
            #utils.log('End.')
            try:
                threads = []

                #thread_diagnosis_opd = Utils(db, 'DIAGNOSIS_OPD', rec_id)
                #thread_diagnosis_ipd = Utils(db, 'DIAGNOSIS_IPD', rec_id)
                #thread_procedure_opd = Utils(db, 'PROCEDURE_OPD', rec_id)
                #thread_procedure_ipd = Utils(db, 'PROCEDURE_IPD', rec_id)
                #thread_drug_opd = Utils(db, 'DRUG_OPD', rec_id)
                #thread_drug_ipd = Utils(db, 'DRUG_IPD', rec_id)
                #thread_death = Utils(db, 'DEATH', rec_id)
                #thread_drug_allergy = Utils(db, 'DRUG_ALLERGY', rec_id)
                #thread_disability = Utils(db, 'DISABILITY', rec_id)
                thread_appointment = Utils(db, 'APPOINTMENT', rec_id)
                #thread_accident = Utils(db, 'ACCIDENT', rec_id)
                #thread_charge_ipd = Utils(db, 'CHARGE_IPD', rec_id)
                #thread_charge_opd = Utils(db, 'CHARGE_OPD', rec_id)
                #thread_admission = Utils(db, 'ADMISSION', rec_id)
                #thread_surveillance = Utils(db, 'SURVEILLANCE', rec_id)
                #thread_women = Utils(db, 'WOMEN', rec_id)
                #thread_fp = Utils(db, 'FP', rec_id)
                thread_epi = Utils(db, 'EPI', rec_id)
                thread_nutrition = Utils(db, 'NUTRITION', rec_id)
                thread_prenatal = Utils(db, 'PRENATAL', rec_id)
                thread_labfu = Utils(db, 'LABFU', rec_id)
                thread_labor = Utils(db, 'LABOR', rec_id)
                thread_postnatal = Utils(db, 'POSTNATAL', rec_id)
                thread_newborn = Utils(db, 'NEWBORN', rec_id)
                thread_newborn_care = Utils(db, 'NEWBORN_CARE', rec_id)
                #thread_dental = Utils(db, 'DENTAL', rec_id)
                #thread_specialpp = Utils(db, 'SPECIALPP', rec_id)
                #thread_ncdscreen = Utils(db, 'NCDSCREEN', rec_id)
                #thread_chronic = Utils(db, 'CHRONIC', rec_id)
                #thread_chronicfu = Utils(db, 'CHRONICFU', rec_id)
                #thread_community_service = Utils(db, 'COMMUNITY_SERVICE', rec_id)
                #thread_icf = Utils(db, 'ICF', rec_id)
                #thread_rehabilitation = Utils(db, 'REHABILITATION', rec_id)
                #thread_functional = Utils(db, 'FUNCTIONAL', rec_id)
                #thread_community_activity = Utils(db, 'COMMUNITY_ACTIVITY', rec_id)
                thread_anc = Utils(db, 'ANC', rec_id)
                thread_service = Utils(db, 'SERVICE', rec_id)
                #thread_card = Utils(db, 'CARD', rec_id)

                thread_anc.start()
                threads.append(thread_anc)

                #thread_card.start()
                #threads.append(thread_card)

                thread_service.start()
                threads.append(thread_service)

                #thread_diagnosis_opd.start()
                #threads.append(thread_diagnosis_opd)

                #thread_diagnosis_ipd.start()
                #threads.append(thread_diagnosis_ipd)

                #thread_procedure_opd.start()
                #threads.append(thread_procedure_opd)

                #thread_procedure_ipd.start()
                #threads.append(thread_procedure_ipd)

                #thread_drug_opd.start()
                #threads.append(thread_drug_opd)

                #thread_drug_ipd.start()
                #threads.append(thread_drug_ipd)

                #thread_death.start()
                #threads.append(thread_death)

                #thread_drug_allergy.start()
                #threads.append(thread_drug_allergy)

                #thread_disability.start()
                #threads.append(thread_disability)

                thread_appointment.start()
                threads.append(thread_appointment)

                #thread_charge_ipd.start()
                #threads.append(thread_charge_ipd)

                #thread_accident.start()
                #threads.append(thread_accident)

                #thread_charge_opd.start()
                #threads.append(thread_charge_opd)

                #thread_admission.start()
                #threads.append(thread_admission)

                #thread_surveillance.start()
                #threads.append(thread_surveillance)

                #thread_women.start()
                #threads.append(thread_women)

                #thread_fp.start()
                #threads.append(thread_fp)

                thread_epi.start()
                threads.append(thread_epi)

                thread_nutrition.start()
                threads.append(thread_nutrition)

                thread_prenatal.start()
                threads.append(thread_prenatal)

                thread_labfu.start()
                threads.append(thread_labfu)

                thread_labor.start()
                threads.append(thread_labor)

                thread_postnatal.start()
                threads.append(thread_postnatal)

                thread_newborn.start()
                threads.append(thread_newborn)

                thread_newborn_care.start()
                threads.append(thread_newborn_care)

                #thread_dental.start()
                #threads.append(thread_dental)

                #thread_specialpp.start()
                #threads.append(thread_specialpp)

                #thread_ncdscreen.start()
                #threads.append(thread_ncdscreen)

                #thread_chronic.start()
                #threads.append(thread_chronic)

                #thread_chronicfu.start()
                #threads.append(thread_chronicfu)

                #thread_community_service.start()
                #threads.append(thread_community_service)

                #thread_icf.start()
                #threads.append(thread_icf)

                #thread_rehabilitation.start()
                #threads.append(thread_rehabilitation)

                #thread_functional.start()
                #threads.append(thread_functional)

                #thread_community_activity.start()
                #threads.append(thread_community_activity)

                for t in threads:
                    t.join()

                # process map

                print("End.\r")

            except Exception as ex:
                sys.stderr.write('[PROCESS CID ERROR] %s' % ex.message)
                #utils.log('[ERROR] %s' % ex.message)

        else:
            #utils.log('Zip file not found.')
            sys.stderr.write('Zip file not found.\r')

    except Exception as ex:
        sys.stderr.write('[MONGO] %s ' % ex.message)


if __name__ == '__main__':
    main()
