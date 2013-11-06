# -*- coding: utf8

import pymongo


class Chronic():

    def __init__(self, db, rows, rec_id):
        self.db = db
        self.rows = rows
        self.rec_id = rec_id

    def do_import(self):

        for data in self.rows:
            doc = {
                'hospcode': data['HOSPCODE'],
                'pid': data['PID'],
                'date_diag': data['DATE_DIAG'],
                'chronic': data['CHRONIC'],
                'hosp_dx': data['HOSP_DX'],
                'hosp_rx': data['HOSP_RX'],
                'date_disch': data['DATE_DISCH'],
                'typedisch': data['TYPEDISCH'],
                'd_update': data['D_UPDATE'],
                'rec_id': self.rec_id
            }
            #print(doc)
            self.do_update(doc)

    def do_update(self, doc):
        self.db.chronic.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.chronic.ensure_index('pid', pymongo.ASCENDING)
        self.db.chronic.ensure_index('chronic', pymongo.ASCENDING)

        self.db.chronic.update(
            {
                'hospcode': doc['hospcode'],
                'pid': doc['pid'],
                'chronic': doc['chronic'],
            },
            {'$set': doc},
            True
        )

