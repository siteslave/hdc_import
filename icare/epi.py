# -*- coding: utf8

import pymongo


class Epi():

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
                'date_serv': data['DATE_SERV'],
                'vaccinetype': data['VACCINETYPE'],
                'vaccineplace': data['VACCINEPLACE'],
                'provider': data['PROVIDER'],
                'd_update': data['D_UPDATE'],
                'rec_id': self.rec_id
            }
            #print(doc)
            self.do_update(doc)

    def do_update(self, doc):
        self.db.epi.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.epi.ensure_index('pid', pymongo.ASCENDING)
        self.db.epi.ensure_index('date_serv', pymongo.ASCENDING)
        self.db.epi.ensure_index('vaccinetype', pymongo.ASCENDING)

        self.db.epi.update(
            {
                'hospcode': doc['hospcode'],
                'pid': doc['pid'],
                'date_serv': doc['date_serv'],
                'vaccinetype': doc['vaccinetype']
            },
            {'$set': doc},
            True
        )

