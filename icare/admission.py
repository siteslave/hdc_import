# -*- coding: utf8

import pymongo


class Admission():

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
                'an': data['AN'],
                'datetime_admit': data['DATETIME_ADMIT'],
                'wardadmit': data['WARDADMIT'],
                'instype': data['INSTYPE'],
                'typein': data['TYPEIN'],
                'referinhosp': data['REFERINHOSP'],
                'causein': data['CAUSEIN'],
                'admitweight': data['ADMITWEIGHT'],
                'admitheight': data['ADMITHEIGHT'],
                'datetime_disch': data['DATETIME_DISCH'],
                'wardisch': data['WARDDISCH'],
                'dischstatus': data['DISCHSTATUS'],
                'dischtype': data['DISCHTYPE'],
                'referouthosp': data['REFEROUTHOSP'],
                'causeout': data['CAUSEOUT'],
                'cost': data['COST'],
                'price': data['PRICE'],
                'payprice': data['PAYPRICE'],
                'actualpay': data['ACTUALPAY'],
                'provider': data['PROVIDER'],
                'd_update': data['D_UPDATE'],
                'rec_id': self.rec_id
            }
            #print(doc)
            self.do_update(doc)

    def do_update(self, doc):
        self.db.admission.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.admission.ensure_index('pid', pymongo.ASCENDING)
        self.db.admission.ensure_index('an', pymongo.ASCENDING)

        self.db.admission.update(
            {
                'hospcode': doc['hospcode'],
                'pid': doc['pid'],
                'an': doc['an']
            },
            {'$set': doc},
            True
        )

