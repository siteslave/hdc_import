# -*- coding: utf8

import pymongo


class Postnatal():

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
                'gravida': data['GRAVIDA'],
                'bdate': data['BDATE'],
                'ppcare': data['PPCARE'],
                'ppplace': data['PPPLACE'],
                'ppresult': data['PPRESULT'],
                'provider': data['PROVIDER'],
                'd_update': data['D_UPDATE'],
                'rec_id': self.rec_id
            }
            #print(doc)
            self.do_update(doc)

    def do_update(self, doc):
        self.db.postnatal.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.postnatal.ensure_index('pid', pymongo.ASCENDING)
        self.db.postnatal.ensure_index('gravida', pymongo.ASCENDING)
        self.db.postnatal.ensure_index('ppcare', pymongo.ASCENDING)

        self.db.postnatal.update(
            {
                'hospcode': doc['hospcode'],
                'pid': doc['pid'],
                'gravida': doc['gravida'],
                'ppcare': doc['ppcare']
            },
            {'$set': doc},
            True
        )

