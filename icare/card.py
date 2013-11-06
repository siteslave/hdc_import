# -*- coding: utf8

import pymongo


class Card():

    def __init__(self, db, rows, rec_id):
        self.db = db
        self.rows = rows
        self.rec_id = rec_id

    def do_import(self):

        for data in self.rows:
            doc = {
                'hospcode': data['HOSPCODE'],
                'pid': data['PID'],
                'instype_old': data['INSTYPE_OLD'],
                'instype_new': data['INSTYPE_NEW'],
                'insid': data['INSID'],
                'startdate': data['STARTDATE'],
                'expiredate': data['EXPIREDATE'],
                'main': data['MAIN'],
                'sub': data['SUB'],
                'd_update': data['D_UPDATE'],
                'rec_id': self.rec_id
            }
            #print(doc)
            self.do_update(doc)

    def do_update(self, doc):
        self.db.card.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.card.ensure_index('pid', pymongo.ASCENDING)
        self.db.card.ensure_index('instype_new', pymongo.ASCENDING)

        self.db.card.update(
            {
                'hospcode': doc['hospcode'],
                'pid': doc['pid'],
                'instype_new': doc['instype_new']
            },
            {'$set': doc},
            True
        )

