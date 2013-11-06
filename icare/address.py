# -*- coding: utf8

import pymongo


class Address():

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
                    'addresstype': data['ADDRESSTYPE'],
                    'house_id': data['HOUSE_ID'],
                    'housetype': data['HOUSETYPE'],
                    'roomno': data['ROOMNO'],
                    'condo': data['CONDO'],
                    'houseno': data['HOUSENO'],
                    'soisub': data['SOISUB'],
                    'soimain': data['SOIMAIN'],
                    'road': data['ROAD'],
                    'villaname': data['VILLANAME'],
                    'village': data['VILLAGE'],
                    'tambon': data['TAMBON'],
                    'ampur': data['AMPUR'],
                    'changwat': data['CHANGWAT'],
                    'telephone': data['TELEPHONE'],
                    'mobile': data['MOBILE'],
                    'd_update': data['D_UPDATE'],
                    'rec_id': self.rec_id
                }
                #print(doc)
                self.do_update(doc)

        except (RuntimeError, TypeError, KeyError):
            pass

    def do_update(self, doc):
        self.db.address.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.address.ensure_index('pid', pymongo.ASCENDING)
        self.db.address.ensure_index('addresstype', pymongo.ASCENDING)

        self.db.address.update(
            {
                'hospcode': doc['hospcode'],
                'pid': doc['pid'],
                'addresstype': doc['addresstype']
            },
            {'$set': doc},
            True
        )

