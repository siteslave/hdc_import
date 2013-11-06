# -*- coding: utf8

import pymongo


class LabFu():

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
                'labtest': data['LABTEST'],
                'labresult': data['LABRESULT'],
                'd_update': data['D_UPDATE'],
                'rec_id': self.rec_id
            }
            #print(doc)
            self.do_update(doc)

    def do_update(self, doc):
        self.db.labfu.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.labfu.ensure_index('pid', pymongo.ASCENDING)
        self.db.labfu.ensure_index('seq', pymongo.ASCENDING)
        self.db.labfu.ensure_index('labtest', pymongo.ASCENDING)

        self.db.labfu.update(
            {
                'hospcode': doc['hospcode'],
                'pid': doc['pid'],
                'seq': doc['seq'],
                'labtest': doc['labtest']
            },
            {'$set': doc},
            True
        )

