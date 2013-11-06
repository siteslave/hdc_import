# -*- coding: utf8

import pymongo


class Charge_opd():

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
        self.db.charge_opd.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.charge_opd.ensure_index('pid', pymongo.ASCENDING)
        self.db.charge_opd.ensure_index('seq', pymongo.ASCENDING)
        self.db.charge_opd.ensure_index('chargeitem', pymongo.ASCENDING)
        self.db.charge_opd.ensure_index('chargelist', pymongo.ASCENDING)
        self.db.charge_opd.ensure_index('instype', pymongo.ASCENDING)

        self.db.charge_opd.update(
            {
                'hospcode': doc['hospcode'],
                'pid': doc['pid'],
                'seq': doc['seq'],
                'chargeitem': doc['chargeitem'],
                'chargelist': doc['chargelist'],
                'instype': doc['instype']
            },
            {'$set': doc},
            True
        )

