# -*- coding: utf8

import pymongo


class Prenatal():

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
                'vdrl_result': data['VDRL_RESULT'],
                'hb_result': data['HB_RESULT'],
                'date_hct': data['DATE_HCT'],
                'hiv_result': data['HIV_RESULT'],
                'hct_result': data['HCT_RESULT'],
                'thalassemia': data['THALASSEMIA'],
                'd_update': data['D_UPDATE'],
                'rec_id': self.rec_id
            }
            #print(doc)
            self.do_update(doc)

    def do_update(self, doc):
        self.db.prenatal.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.prenatal.ensure_index('pid', pymongo.ASCENDING)
        self.db.prenatal.ensure_index('gravida', pymongo.ASCENDING)

        self.db.prenatal.update(
            {
                'hospcode': doc['hospcode'],
                'pid': doc['pid'],
                'gravida': doc['gravida']
            },
            {'$set': doc},
            True
        )

