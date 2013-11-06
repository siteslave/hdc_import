# -*- coding: utf8

import pymongo


class Accident():

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
                'datetime_serv': data['DATETIME_SERV'],
                'datetime_ae': data['DATETIME_AE'],
                'aetype': data['AETYPE'],
                'aeplace': data['AEPLACE'],
                'typein_ae': data['TYPEIN_AE'],
                'traffic': data['TRAFFIC'],
                'vehicle': data['VEHICLE'],
                'alcohol': data['ALCOHOL'],
                'nacrotic_drug': data['NACROTIC_DRUG'],
                'belt': data['BELT'],
                'helmet': data['HELMET'],
                'airway': data['AIRWAY'],
                'stopbleed': data['STOPBLEED'],
                'splint': data['SPLINT'],
                'fluid': data['FLUID'],
                'urgency': data['URGENCY'],
                'coma_eye': data['COMA_EYE'],
                'coma_speak': data['COMA_SPEAK'],
                'coma_movement': data['COMA_MOVEMENT'],
                'd_update': data['D_UPDATE'],
                'rec_id': self.rec_id
            }
            #print(doc)
            self.do_update(doc)

    def do_update(self, doc):
        self.db.accident.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.accident.ensure_index('pid', pymongo.ASCENDING)
        self.db.accident.ensure_index('seq', pymongo.ASCENDING)

        self.db.accident.update(
            {
                'hospcode': doc['hospcode'],
                'pid': doc['pid'],
                'seq': doc['seq']
            },
            {'$set': doc},
            True
        )

