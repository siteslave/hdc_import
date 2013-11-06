# -*- coding: utf8

import pymongo


class Nutrition():

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
                'nutritionplace': data['NUTRITIONPLACE'],
                'weight': data['WEIGHT'],
                'height': data['HEIGHT'],
                'headcircum': data['HEADCIRCUM'],
                'childdevelop': data['CHILDDEVELOP'],
                'food': data['FOOD'],
                'bottle': data['BOTTLE'],
                'provider': data['PROVIDER'],
                'd_update': data['D_UPDATE'],
                'rec_id': self.rec_id
            }
            #print(doc)
            self.do_update(doc)

    def do_update(self, doc):
        self.db.nutrition.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.nutrition.ensure_index('pid', pymongo.ASCENDING)
        self.db.nutrition.ensure_index('date_serv', pymongo.ASCENDING)

        self.db.nutrition.update(
            {
                'hospcode': doc['hospcode'],
                'pid': doc['pid'],
                'date_serv': doc['date_serv']
            },
            {'$set': doc},
            True
        )

