# -*- coding: utf8

import pymongo


class SpecialPP():

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
                'servplace': data['SERVPLACE'],
                'ppspecial': data['PPSPECIAL'],
                'ppsplace': data['PPSPLACE'],
                'provider': data['PROVIDER'],
                'd_update': data['D_UPDATE'],
                'rec_id': self.rec_id
            }
            #print(doc)
            self.do_update(doc)

    def do_update(self, doc):
        self.db.specialpp.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.specialpp.ensure_index('pid', pymongo.ASCENDING)
        self.db.specialpp.ensure_index('date_serv', pymongo.ASCENDING)
        self.db.specialpp.ensure_index('ppspecial', pymongo.ASCENDING)

        self.db.specialpp.update(
            {
                'hospcode': doc['hospcode'],
                'pid': doc['pid'],
                'date_serv': doc['date_serv'],
                'ppspecial': doc['ppspecial']
            },
            {'$set': doc},
            True
        )

