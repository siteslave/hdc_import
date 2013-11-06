# -*- coding: utf8

import pymongo


class Diagnosis_opd():

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
                'diagtype': data['DIAGTYPE'],
                'diagcode': data['DIAGCODE'],
                'clinic': data['CLINIC'],
                'provider': data['PROVIDER'],
                'd_update': data['D_UPDATE'],
                'rec_id': self.rec_id
            }
            #print(doc)
            self.do_update(doc)

    def do_update(self, doc):
        self.db.diagnosis_opd.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.diagnosis_opd.ensure_index('pid', pymongo.ASCENDING)
        self.db.diagnosis_opd.ensure_index('seq', pymongo.ASCENDING)
        self.db.diagnosis_opd.ensure_index('diagcode', pymongo.ASCENDING)

        self.db.diagnosis_opd.update(
            {
                'hospcode': doc['hospcode'],
                'pid': doc['pid'],
                'seq': doc['seq'],
                'diagcode': doc['diagcode']
            },
            {'$set': doc},
            True
        )

