# -*- coding: utf8

import pymongo


class Procedure_ipd():

    def __init__(self, db, rows, rec_id):
        self.db = db
        self.rows = rows
        self.rec_id = rec_id

    def do_import(self):

        for data in self.rows:
            doc = {
                'hospcode': data['HOSPCODE'],
                'pid': data['PID'],
                'an': data['AN'],
                'datetime_admit': data['DATETIME_ADMIT'],
                'wardstay': data['WARDSTAY'],
                'procedcode': data['PROCEDCODE'],
                'timestart': data['TIMESTART'],
                'serviceprice': data['SERVICEPRICE'],
                'provider': data['PROVIDER'],
                'd_update': data['D_UPDATE'],
                'rec_id': self.rec_id
            }
            #print(doc)
            self.do_update(doc)

    def do_update(self, doc):
        self.db.procedure_ipd.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.procedure_ipd.ensure_index('pid', pymongo.ASCENDING)
        self.db.procedure_ipd.ensure_index('an', pymongo.ASCENDING)
        self.db.procedure_ipd.ensure_index('procedcode', pymongo.ASCENDING)

        self.db.procedure_ipd.update(
            {
                'hospcode': doc['hospcode'],
                'pid': doc['pid'],
                'an': doc['an'],
                'procedcode': doc['procedcode']
            },
            {'$set': doc},
            True
        )

