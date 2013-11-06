# -*- coding: utf8

import pymongo


class NewBornCare():

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
                'bdate': data['BDATE'],
                'bcare': data['BCARE'],
                'bcplace': data['BCPLACE'],
                'bcareresult': data['BCARERESULT'],
                'food': data['FOOD'],
                'provider': data['PROVIDER'],
                'd_update': data['D_UPDATE'],
                'rec_id': self.rec_id
            }
            #print(doc)
            self.do_update(doc)

    def do_update(self, doc):
        self.db.newborncare.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.newborncare.ensure_index('pid', pymongo.ASCENDING)
        self.db.newborncare.ensure_index('bcare', pymongo.ASCENDING)

        self.db.newborncare.update(
            {
                'hospcode': doc['hospcode'],
                'pid': doc['pid'],
                'bcare': doc['bcare']
            },
            {'$set': doc},
            True
        )

