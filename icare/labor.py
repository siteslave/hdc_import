# -*- coding: utf8

import pymongo


class Labor():

    def __init__(self, db, rows, rec_id):
        self.db = db
        self.rows = rows
        self.rec_id = rec_id

    def do_import(self):

        for data in self.rows:
            doc = {
                'hospcode': data['HOSPCODE'],
                'pid': data['PID'],
                'gravida': data['GRAVIDA'],
                'lmp': data['LMP'],
                'edc': data['EDC'],
                'bdate': data['BDATE'],
                'bresult': data['BRESULT'],
                'bplace': data['BPLACE'],
                'bhosp': data['BHOSP'],
                'btype': data['BTYPE'],
                'bdoctor': data['BDOCTOR'],
                'lborn': data['LBORN'],
                'sborn': data['SBORN'],
                'd_update': data['D_UPDATE'],
                'rec_id': self.rec_id
            }
            #print(doc)
            self.do_update(doc)

    def do_update(self, doc):
        self.db.labor.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.labor.ensure_index('pid', pymongo.ASCENDING)
        self.db.labor.ensure_index('gravida', pymongo.ASCENDING)

        self.db.labor.update(
            {
                'hospcode': doc['hospcode'],
                'pid': doc['pid'],
                'gravida': doc['gravida']
            },
            {'$set': doc},
            True
        )

