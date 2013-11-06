# -*- coding: utf8

import pymongo


class Surveillance():

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
                'an': data['AN'],
                'datetime_admit': data['DATETIME_ADMIT'],
                'syndrome': data['SYNDROME'],
                'diagcode': data['DIAGCODE'],
                'code506': data['CODE506'],
                'diagcodelast': data['DIAGCODELAST'],
                'code506last': data['CODE506LAST'],
                'illdate': data['ILLDATE'],
                'illhouse': data['ILLHOUSE'],
                'illvillage': data['ILLVILLAGE'],
                'illtambon': data['ILLTAMBON'],
                'illampur': data['ILLAMPUR'],
                'illchanwat': data['ILLCHANGWAT'],
                'latitude': data['LATITUDE'],
                'longitude': data['LONGITUDE'],
                'ptstatus': data['PTSTATUS'],
                'date_death': data['DATE_DEATH'],
                'complication': data['COMPLICATION'],
                'organism': data['ORGANISM'],
                'provider': data['PROVIDER'],
                'd_update': data['D_UPDATE'],
                'rec_id': self.rec_id
            }
            #print(doc)
            self.do_update(doc)

    def do_update(self, doc):
        self.db.surveillance.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.surveillance.ensure_index('pid', pymongo.ASCENDING)
        self.db.surveillance.ensure_index('seq', pymongo.ASCENDING)
        self.db.surveillance.ensure_index('diagcode', pymongo.ASCENDING)

        self.db.surveillance.update(
            {
                'hospcode': doc['hospcode'],
                'pid': doc['pid'],
                'seq': doc['seq'],
                'diagcode': doc['diagcode']
            },
            {'$set': doc},
            True
        )

