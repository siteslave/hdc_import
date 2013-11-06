# -*- coding: utf8

import pymongo


class Drug_opd():

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
                'clinic': data['CLINIC'],
                'didstd': data['DIDSTD'],
                'dname': data['DNAME'],
                'amount': data['AMOUNT'],
                'unit': data['UNIT'],
                'unit_packing': data['UNIT_PACKING'],
                'drugprice': data['DRUGPRICE'],
                'drugcost': data['DRUGCOST'],
                'provider': data['PROVIDER'],
                'd_update': data['D_UPDATE'],
                'rec_id': self.rec_id
            }
            #print(doc)
            self.do_update(doc)

    def do_update(self, doc):
        self.db.drug_opd.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.drug_opd.ensure_index('pid', pymongo.ASCENDING)
        self.db.drug_opd.ensure_index('seq', pymongo.ASCENDING)
        self.db.drug_opd.ensure_index('didstd', pymongo.ASCENDING)

        self.db.drug_opd.update(
            {
                'hospcode': doc['hospcode'],
                'pid': doc['pid'],
                'seq': doc['seq'],
                'didstd': doc['didstd']
            },
            {'$set': doc},
            True
        )

