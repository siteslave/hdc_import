# -*- coding: utf8

import pymongo


class Charge_ipd():

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
                'chargeitem': data['CHARGEITEM'],
                'chargelist': data['CHARGELIST'],
                'quantity': data['QUANTITY'],
                'instype': data['INSTYPE'],
                'cost': data['COST'],
                'price': data['PRICE'],
                'payprice': data['PAYPRICE'],
                'd_update': data['D_UPDATE'],
                'rec_id': self.rec_id
            }
            #print(doc)
            self.do_update(doc)

    def do_update(self, doc):
        self.db.charge_ipd.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.charge_ipd.ensure_index('pid', pymongo.ASCENDING)
        self.db.charge_ipd.ensure_index('an', pymongo.ASCENDING)
        self.db.charge_ipd.ensure_index('chargeitem', pymongo.ASCENDING)
        self.db.charge_ipd.ensure_index('chargelist', pymongo.ASCENDING)
        self.db.charge_ipd.ensure_index('instype', pymongo.ASCENDING)

        self.db.charge_ipd.update(
            {
                'hospcode': doc['hospcode'],
                'pid': doc['pid'],
                'an': doc['an'],
                'chargeitem': doc['chargeitem'],
                'chargelist': doc['chargelist'],
                'instype': doc['instype']
            },
            {'$set': doc},
            True
        )

