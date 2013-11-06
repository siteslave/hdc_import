# -*- coding: utf8

import pymongo


class ChronicFu():

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
                'weight': data['WEIGHT'],
                'height': data['HEIGHT'],
                'waist_cm': data['WAIST_CM'],
                'sbp': data['SBP'],
                'dbp': data['DBP'],
                'foot': data['FOOT'],
                'retina': data['RETINA'],
                'provider': data['PROVIDER'],
                'd_update': data['D_UPDATE'],
                'rec_id': self.rec_id
            }
            #print(doc)
            self.do_update(doc)

    def do_update(self, doc):
        self.db.chronicfu.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.chronicfu.ensure_index('pid', pymongo.ASCENDING)
        self.db.chronicfu.ensure_index('seq', pymongo.ASCENDING)

        self.db.chronicfu.update(
            {
                'hospcode': doc['hospcode'],
                'pid': doc['pid'],
                'seq': doc['seq']
            },
            {'$set': doc},
            True
        )

