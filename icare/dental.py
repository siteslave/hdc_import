# -*- coding: utf8

import pymongo


class Dental():

    def __init__(self, db, rows, rec_id):
        self.db = db
        self.rows = rows
        self.rec_id = rec_id

    def do_import(self):

        for data in self.rows:
            doc = {
                'hospcode': data['HOSPCODE'],
                'pid': data['PID'],
                'seq': data['SEQ'],
                'date_serv': data['DATE_SERV'],
                'denttype': data['DENTTYPE'],
                'servplace': data['SERVPLACE'],
                'pteeth': data['PTEETH'],
                'pfilling': data['PFILLING'],
                'pextract': data['PEXTRACT'],
                'dteeth': data['DTEETH'],
                'dcaries': data['DCARIES'],
                'dfilling': data['DFILLING'],
                'dextract': data['DEXTRACT'],
                'need_fluoride': data['NEED_FLUORIDE'],
                'need_scaling': data['NEED_SCALING'],
                'need_sealant': data['NEED_SEALANT'],
                'need_pfilling': data['NEED_PFILLING'],
                'need_dfilling': data['NEED_DFILLING'],
                'need_pextract': data['NEED_PEXTRACT'],
                'need_dextract': data['NEED_DEXTRACT'],
                'nprosthesis': data['NPROSTHESIS'],
                'permanent_permanent': data['PERMANENT_PERMANENT'],
                'permanent_prosthesis': data['PERMANENT_PROSTHESIS'],
                'prosthesis_prosthesis': data['PROSTHESIS_PROSTHESIS'],
                'gum': data['GUM'],
                'schooltype': data['SCHOOLTYPE'],
                'class': data['CLASS'],
                'provider': data['PROVIDER'],
                'd_update': data['D_UPDATE'],
                'rec_id': self.rec_id
            }
            #print(doc)
            self.do_update(doc)

    def do_update(self, doc):
        self.db.dental.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.dental.ensure_index('pid', pymongo.ASCENDING)
        self.db.dental.ensure_index('seq', pymongo.ASCENDING)

        self.db.dental.update(
            {
                'hospcode': doc['hospcode'],
                'pid': doc['pid'],
                'seq': doc['seq']
            },
            {'$set': doc},
            True
        )

