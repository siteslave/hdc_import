# -*- coding: utf8

import pymongo


class Women():

    def __init__(self, db, rows, rec_id):
        self.db = db
        self.rows = rows
        self.rec_id = rec_id

    def do_import(self):

        for data in self.rows:
            doc = {
                'hospcode': data['HOSPCODE'],
                'pid': data['PID'],
                'fptype': data['FPTYPE'],
                'nofpcause': data['NOFPCAUSE'],
                'totalson': data['TOTALSON'],
                'numberson': data['NUMBERSON'],
                'abortion': data['ABORTION'],
                'stillbirth': data['STILLBIRTH'],
                'd_update': data['D_UPDATE'],
                'rec_id': self.rec_id
            }
            #print(doc)
            self.do_update(doc)

    def do_update(self, doc):
        self.db.women.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.women.ensure_index('pid', pymongo.ASCENDING)

        self.db.women.update(
            {
                'hospcode': doc['hospcode'],
                'pid': doc['pid']
            },
            {'$set': doc},
            True
        )

