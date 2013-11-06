# -*- coding: utf8

import pymongo


class Disability():

    def __init__(self, db, rows, rec_id):
        self.db = db
        self.rows = rows
        self.rec_id = rec_id

    def do_import(self):

        for data in self.rows:
            doc = {
                'hospcode': data['HOSPCODE'],
                'pid': data['PID'],
                'disabid': data['DISABID'],
                'disabtype': data['DISABTYPE'],
                'disabcause': data['DISABCAUSE'],
                'diagcode': data['DIAGCODE'],
                'date_detect': data['DATE_DETECT'],
                'date_disab': data['DATE_DISAB'],
                'd_update': data['D_UPDATE'],
                'rec_id': self.rec_id
            }
            #print(doc)
            self.do_update(doc)

    def do_update(self, doc):
        self.db.disability.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.disability.ensure_index('pid', pymongo.ASCENDING)
        self.db.disability.ensure_index('disabtype', pymongo.ASCENDING)

        self.db.disability.update(
            {
                'hospcode': doc['hospcode'],
                'pid': doc['pid'],
                'disabtype': doc['disabtype']
            },
            {'$set': doc},
            True
        )

