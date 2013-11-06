# -*- coding: utf8

import pymongo


class Rehabilitation():

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
                'date_admit': data['DATE_ADMIT'],
                'date_serv': data['DATE_SERV'],
                'date_start': data['DATE_START'],
                'date_finish': data['DATE_FINISH'],
                'rehabcode': data['REHABCODE'],
                'at_device': data['AT_DEVICE'],
                'at_no': data['AT_NO'],
                'rehabplace': data['REHABPLACE'],
                'provider': data['PROVIDER'],
                'd_update': data['D_UPDATE'],
                'rec_id': self.rec_id
            }
            #print(doc)
            self.do_update(doc)

    def do_update(self, doc):

        self.db.rehabilitation.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.rehabilitation.ensure_index('pid', pymongo.ASCENDING)
        self.db.rehabilitation.ensure_index('date_serv', pymongo.ASCENDING)
        self.db.rehabilitation.ensure_index('rehabcode', pymongo.ASCENDING)

        self.db.rehabilitation.update(
            {
                'hospcode': doc['hospcode'],
                'pid': doc['pid'],
                'date_serv': doc['date_serv'],
                'rehabcode': doc['rehabcode']
            },
            {'$set': doc},
            True
        )

