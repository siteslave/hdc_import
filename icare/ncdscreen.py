# -*- coding: utf8

import pymongo


class NcdScreen():

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
                'servplace': data['SERVPLACE'],
                'smoke': data['SMOKE'],
                'alcohol': data['ALCOHOL'],
                'dmfamily': data['DMFAMILY'],
                'htfamily': data['HTFAMILY'],
                'weight': data['WEIGHT'],
                'height': data['HEIGHT'],
                'waist_cm': data['WAIST_CM'],
                'sbp_1': data['SBP_1'],
                'dbp_1': data['DBP_2'],
                'sbp_2': data['SBP_2'],
                'dbp_2': data['DBP_2'],
                'bslevel': data['BSLEVEL'],
                'bstest': data['BSTEST'],
                'screenplace': data['SCREENPLACE'],
                'provider': data['PROVIDER'],
                'd_update': data['D_UPDATE'],
                'rec_id': self.rec_id
            }
            #print(doc)
            self.do_update(doc)

    def do_update(self, doc):
        self.db.ncdscreen.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.ncdscreen.ensure_index('pid', pymongo.ASCENDING)
        self.db.ncdscreen.ensure_index('date_serv', pymongo.ASCENDING)

        self.db.ncdscreen.update(
            {
                'hospcode': doc['hospcode'],
                'pid': doc['pid'],
                'date_serv': doc['date_serv']
            },
            {'$set': doc},
            True
        )

