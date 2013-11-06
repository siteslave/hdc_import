# -*- coding: utf8

import pymongo


class Provider():

    def __init__(self, db, rows, rec_id):
        self.db = db
        self.rows = rows
        self.rec_id = rec_id

    def do_import(self):

        for data in self.rows:
            doc = {
                'hospcode': data['HOSPCODE'],
                'provider': data['PROVIDER'],
                'registerno': data['REGISTERNO'],
                'council': data['COUNCIL'],
                'cid': data['CID'],
                'prename': data['PRENAME'],
                'name': data['NAME'],
                'lname': data['LNAME'],
                'sex': data['SEX'],
                'birth': data['BIRTH'],
                'providertype': data['PROVIDERTYPE'],
                'startdate': data['STARTDATE'],
                'outdate': data['OUTDATE'],
                'movefrom': data['MOVEFROM'],
                'moveto': data['MOVETO'],
                'd_update': data['D_UPDATE'],
                'rec_id': self.rec_id
            }
            #print(doc)
            self.do_update(doc)

    def do_update(self, doc):
        self.db.provider.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.provider.ensure_index('provider', pymongo.ASCENDING)

        self.db.provider.update(
            {
                'hospcode': doc['hospcode'],
                'provider': doc['provider'],
            },
            {'$set': doc},
            True
        )

