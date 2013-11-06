# -*- coding: utf8

import pymongo


class Death():

    def __init__(self, db, rows, rec_id):
        self.db = db
        self.rows = rows
        self.rec_id = rec_id

    def do_import(self):
        try:
            for data in self.rows:
                doc = {
                    'hospcode': data['HOSPCODE'],
                    'pid': data['PID'],
                    'hospdeath': data['HOSPDEATH'],
                    'an': data['AN'],
                    'seq': data['SEQ'],
                    'ddeath': data['DDEATH'],
                    'cdeath_a': data['CDEATH_A'],
                    'cdeath_b': data['CDEATH_B'],
                    'cdeath_c': data['CDEATH_C'],
                    'cdeath_d': data['CDEATH_D'],
                    'odisease': data['ODISEASE'],
                    'cdeath': data['CDEATH'],
                    'pregdeath': data['PREGDEATH'],
                    'pdeath': data['PDEATH'],
                    'provider': data['PROVIDER'],
                    'd_update': data['D_UPDATE'],
                    'rec_id': self.rec_id
                }
                #print(doc)
                self.do_update(doc)

        except (RuntimeError, TypeError, KeyError):
            pass

    def do_update(self, doc):
        self.db.death.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.death.ensure_index('pid', pymongo.ASCENDING)

        self.db.death.update(
            {
                'hospcode': doc['hospcode'],
                'pid': doc['pid']
            },
            {'$set': doc},
            True
        )

