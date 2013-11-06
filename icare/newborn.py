# -*- coding: utf8

import pymongo


class NewBorn():

    def __init__(self, db, rows, rec_id):
        self.db = db
        self.rows = rows
        self.rec_id = rec_id

    def do_import(self):

        for data in self.rows:
            doc = {
                'hospcode': data['HOSPCODE'],
                'pid': data['PID'],
                'mpid': data['MPID'],
                'gravida': data['GRAVIDA'],
                'ga': data['GA'],
                'bdate': data['BDATE'],
                'btime': data['BTIME'],
                'bplace': data['BPLACE'],
                'bhosp': data['BHOSP'],
                'birthno': data['BIRTHNO'],
                'btype': data['BTYPE'],
                'bdoctor': data['BDOCTOR'],
                'bweight': data['BWEIGHT'],
                'asphyxia': data['ASPHYXIA'],
                'vitk': data['VITK'],
                'tsh': data['TSH'],
                'tshresult': data['TSHRESULT'],
                'd_update': data['D_UPDATE'],
                'rec_id': self.rec_id
            }
            #print(doc)
            self.do_update(doc)

    def do_update(self, doc):
        self.db.newborn.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.newborn.ensure_index('pid', pymongo.ASCENDING)

        self.db.newborn.update(
            {
                'hospcode': doc['hospcode'],
                'pid': doc['pid']
            },
            {'$set': doc},
            True
        )

