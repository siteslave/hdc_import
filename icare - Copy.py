# -*- coding: utf8
#!/usr/bin/env python

import os, sys, shutil
import glob, datetime
import codecs, csv, json
import zipfile
import ConfigParser
import random, string

from pymongo import Connection
from pymongo.errors import ConnectionFailure

""" Get file configuration """

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

""" Random string """


def get_random(n):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(n))

""" Get file list """


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
    zf = zipfile.ZipFile(zip_file)
    #Create extract directory
    extract_dir = ZIP_DIR + '/' + get_random(15)
    
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
        print ("Error: %s - %s \r\n" % (e.filename, e.strerror))

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
        sys.stderr.write('[FILE_CONTENT] Error: %s \r\n' % e)


def get_connection():
    """
    Connection to MongoDB
    """
    conn = Connection(host=MONGO_HOST, port=MONGO_PORT)
    db = conn[MONGO_DB]
    db.authenticate(MONGO_USER, MONGO_PASSWORD)
    sys.stderr.write('[MONGO] Connected \r\n')
    return db


def main():

    try:
        db = get_connection()
                
        zip_count = len(glob.glob1(ZIP_DIR, '*.zip'))

        if zip_count:
            sys.stderr.write('Starting...\r\n')
            #Get file list
            zip_files = get_zip_file_list()
            #print(ZIP_DIR)
            for f in zip_files:

                extracted_dir = do_extract(f)

                try:
                    #count file
                    file_count = len(glob.glob1(extracted_dir, '*.txt'))

                    if file_count == 43:
                        ## get files list
                        text_files = glob.glob(os.path.join(extracted_dir, '*.txt'))
                        for text_file in text_files:
                            ##file_content = get_file_contents(text_file)
                            text_file_name = os.path.basename(text_file).upper()

                            if text_file_name == 'PERSON.TXT':
                                from icare.person import Person

                                data = get_file_contents(text_file)
                                col = Person(db, data)
                                col.do_import()

                            if text_file_name == 'ADDRESS.TXT':
                                from icare.address import Address

                                data = get_file_contents(text_file)
                                col = Address(db, data)
                                col.do_import()

                            if text_file_name == 'DEATH.TXT':
                                from icare.death import Death

                                data = get_file_contents(text_file)
                                col = Death(db, data)
                                col.do_import()

                            if text_file_name == 'CARD.TXT':
                                from icare.card import Card

                                data = get_file_contents(text_file)
                                col = Card(db, data)
                                col.do_import()

                            if text_file_name == 'DRUGALLERGY.TXT':
                                from icare.drugallergy import Drugallergy

                                data = get_file_contents(text_file)
                                col = Drugallergy(db, data)
                                col.do_import()

                            if text_file_name == 'HOME.TXT':
                                from icare.home import Home

                                data = get_file_contents(text_file)
                                col = Home(db, data)
                                col.do_import()

                            if text_file_name == 'SERVICE.TXT':
                                from icare.service import Service

                                data = get_file_contents(text_file)
                                col = Service(db, data)
                                col.do_import()

                            if text_file_name == 'APPOINTMENT.TXT':
                                from icare.appointment import Appointment

                                data = get_file_contents(text_file)
                                col = Appointment(db, data)
                                col.do_import()

                            if text_file_name == 'ACCIDENT.TXT':
                                from icare.accident import Accident

                                data = get_file_contents(text_file)
                                col = Accident(db, data)
                                col.do_import()

                            if text_file_name == 'DIAGNOSIS_OPD.TXT':
                                from icare.diagnosis_opd import Diagnosis_opd

                                data = get_file_contents(text_file)
                                col = Diagnosis_opd(db, data)
                                col.do_import()

                            if text_file_name == 'PROCEDURE_OPD.TXT':
                                from icare.procedure_opd import Procedure_opd

                                data = get_file_contents(text_file)
                                col = Procedure_opd(db, data)
                                col.do_import()

                            if text_file_name == 'DRUG_OPD.TXT':
                                from icare.drug_opd import Drug_opd

                                data = get_file_contents(text_file)
                                col = Drug_opd(db, data)
                                col.do_import()

                            if text_file_name == 'CHARGE_OPD.TXT':
                                from icare.charge_opd import Charge_opd

                                data = get_file_contents(text_file)
                                col = Charge_opd(db, data)
                                col.do_import()

                            if text_file_name == 'ADMISSION.TXT':
                                from icare.admission import Admission

                                data = get_file_contents(text_file)
                                col = Admission(db, data)
                                col.do_import()

                            if text_file_name == 'DIAGNOSIS_IPD.TXT':
                                from icare.diagnosis_ipd import Diagnosis_ipd

                                data = get_file_contents(text_file)
                                col = Diagnosis_ipd(db, data)
                                col.do_import()

                            if text_file_name == 'PROCEDURE_IPD.TXT':
                                from icare.procedure_ipd import Procedure_ipd

                                data = get_file_contents(text_file)
                                col = Procedure_ipd(db, data)
                                col.do_import()

                            if text_file_name == 'DRUG_IPD.TXT':
                                from icare.drug_ipd import Drug_ipd

                                data = get_file_contents(text_file)
                                col = Drug_ipd(db, data)
                                col.do_import()

                            if text_file_name == 'CHARGE_IPD.TXT':
                                from icare.charge_ipd import Charge_ipd

                                data = get_file_contents(text_file)
                                col = Charge_ipd(db, data)
                                col.do_import()

                            if text_file_name == 'SURVEILLANCE.TXT':
                                from icare.surveillance import Surveillance

                                data = get_file_contents(text_file)
                                col = Surveillance(db, data)
                                col.do_import()

                            if text_file_name == 'WOMEN.TXT':
                                from icare.women import Women

                                data = get_file_contents(text_file)
                                col = Women(db, data)
                                col.do_import()

                            if text_file_name == 'FP.TXT':
                                from icare.fp import Fp

                                data = get_file_contents(text_file)
                                col = Fp(db, data)
                                col.do_import()

                            if text_file_name == 'EPI.TXT':
                                from icare.epi import Epi

                                data = get_file_contents(text_file)
                                col = Epi(db, data)
                                col.do_import()

                            if text_file_name == 'NUTRITION.TXT':
                                from icare.nutrition import Nutrition

                                data = get_file_contents(text_file)
                                col = Nutrition(db, data)
                                col.do_import()

                            if text_file_name == 'PRENATAL.TXT':
                                from icare.prenatal import Prenatal

                                data = get_file_contents(text_file)
                                col = Prenatal(db, data)
                                col.do_import()

                            if text_file_name == 'ANC.TXT':
                                from icare.anc import Anc

                                data = get_file_contents(text_file)
                                col = Anc(db, data)
                                col.do_import()

                            if text_file_name == 'LABOR.TXT':
                                from icare.labor import Labor

                                data = get_file_contents(text_file)
                                col = Labor(db, data)
                                col.do_import()

                            if text_file_name == 'POSTNATAL.TXT':
                                from icare.postnatal import Postnatal

                                data = get_file_contents(text_file)
                                col = Postnatal(db, data)
                                col.do_import()

                            if text_file_name == 'NEWBORN.TXT':
                                from icare.newborn import NewBorn

                                data = get_file_contents(text_file)
                                col = NewBorn(db, data)
                                col.do_import()

                            if text_file_name == 'NEWBORNCARE.TXT':
                                from icare.newborncare import NewBornCare

                                data = get_file_contents(text_file)
                                col = NewBornCare(db, data)
                                col.do_import()

                            if text_file_name == 'DENTAL.TXT':
                                from icare.dental import Dental

                                data = get_file_contents(text_file)
                                col = Dental(db, data)
                                col.do_import()

                            if text_file_name == 'SPECIALPP.TXT':
                                from icare.specialpp import SpecialPP

                                data = get_file_contents(text_file)
                                col = SpecialPP(db, data)
                                col.do_import()

                            if text_file_name == 'NCDSCREEN.TXT':
                                from icare.ncdscreen import NcdScreen

                                data = get_file_contents(text_file)
                                col = NcdScreen(db, data)
                                col.do_import()

                            if text_file_name == 'CHRONIC.TXT':
                                from icare.chronic import Chronic

                                data = get_file_contents(text_file)
                                col = Chronic(db, data)
                                col.do_import()

                            if text_file_name == 'CHRONICFU.TXT':
                                from icare.chronicfu import ChronicFu

                                data = get_file_contents(text_file)
                                col = ChronicFu(db, data)
                                col.do_import()

                            if text_file_name == 'LABFU.TXT':
                                from icare.labfu import LabFu

                                data = get_file_contents(text_file)
                                col = LabFu(db, data)
                                col.do_import()

                            if text_file_name == 'COMMUNITY_SERVICE.TXT':
                                from icare.community_service import CommunityService

                                data = get_file_contents(text_file)
                                col = CommunityService(db, data)
                                col.do_import()

                            if text_file_name == 'DISABILITY.TXT':
                                from icare.disability import Disability

                                data = get_file_contents(text_file)
                                col = Disability(db, data)
                                col.do_import()

                            if text_file_name == 'ICF.TXT':
                                from icare.icf import Icf

                                data = get_file_contents(text_file)
                                col = Icf(db, data)
                                col.do_import()

                            if text_file_name == 'REHABILITATION.TXT':
                                from icare.rehabilitation import Rehabilitation

                                data = get_file_contents(text_file)
                                col = Rehabilitation(db, data)
                                col.do_import()

                            if text_file_name == 'VILLAGE.TXT':
                                from icare.village import Village

                                data = get_file_contents(text_file)
                                col = Village(db, data)
                                col.do_import()

                    else:
                        sys.stderr.write('[ERROR] File total invalid \r\n')
                    #Remove extracted directory
                    shutil.rmtree(extracted_dir)

                except:
                    shutil.rmtree(extracted_dir)
                    sys.stderr.write('[ERROR]: file %s \r\n' % f)
                    continue

            print("End.\r\n")

        else:
            sys.stderr.write('[ERROR] file not found.\r\n')
    except:
         sys.stderr.write('[MONGO] Failed.\r\n') 


if __name__ == '__main__':
    main()
