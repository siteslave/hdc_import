# -*- coding: utf8

import pymongo


class Drugallergy():

    def __init__(self, db, rows, rec_id):
        self.db = db
        self.rows = rows
        self.rec_id = rec_id

    def do_import(self):

        for data in self.rows:
            doc = {
                'hospcode': data['HOSPCODE'],
                'pid': data['PID'],
                'daterecord': data['DATERECORD'],
                'drugallergy': data['DRUGALLERGY'],
                'dname': data['DNAME'],
                'typedx': data['TYPEDX'],
                'alevel': data['ALEVEL'],
                'symptom': data['SYMPTOM'],
                'informant': data['INFORMANT'],
                'informhosp': data['INFORMHOSP'],
                'd_update': data['D_UPDATE'],
                'rec_id': self.rec_id
            }
            #print(doc)
            self.do_update(doc)

    def do_update(self, doc):
        self.db.drugallergy.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.drugallergy.ensure_index('pid', pymongo.ASCENDING)
        self.db.drugallergy.ensure_index('drugallergy', pymongo.ASCENDING)

        self.db.drugallergy.update(
            {
                'hospcode': doc['hospcode'],
                'pid': doc['pid'],
                'drugallergy': doc['drugallergy']
            },
            {'$set': doc},
            True
        )

