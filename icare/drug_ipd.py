# -*- coding: utf8

import pymongo


class Drug_ipd():

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
                'typedrug': data['TYPEDRUG'],
                'didstd': data['DIDSTD'],
                'dname': data['DNAME'],
                'datestart': data['DATESTART'],
                'datefinish': data['DATEFINISH'],
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
        self.db.drug_ipd.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.drug_ipd.ensure_index('pid', pymongo.ASCENDING)
        self.db.drug_ipd.ensure_index('an', pymongo.ASCENDING)
        self.db.drug_ipd.ensure_index('didstd', pymongo.ASCENDING)
        self.db.drug_ipd.ensure_index('typedrug', pymongo.ASCENDING)

        self.db.drug_ipd.update(
            {
                'hospcode': doc['hospcode'],
                'pid': doc['pid'],
                'an': doc['an'],
                'typedrug': doc['typedrug'],
                'didstd': doc['didstd']
            },
            {'$set': doc},
            True
        )

