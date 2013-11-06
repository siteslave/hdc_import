# -*- coding: utf8

import pymongo


class Appointment():

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
                    'an': data['AN'],
                    'seq': data['SEQ'],
                    'date_serv': data['DATE_SERV'],
                    'clinic': data['CLINIC'],
                    'apdate': data['APDATE'],
                    'aptype': data['APTYPE'],
                    'apdiag': data['APDIAG'],
                    'provider': data['PROVIDER'],
                    'd_update': data['D_UPDATE'],
                    'rec_id': self.rec_id
                }
                #print(doc)
                self.do_update(doc)

        except (RuntimeError, TypeError, KeyError):
            pass

    def do_update(self, doc):
        self.db.appointment.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.appointment.ensure_index('pid', pymongo.ASCENDING)
        self.db.appointment.ensure_index('aptype', pymongo.ASCENDING)
        self.db.appointment.ensure_index('aptype', pymongo.ASCENDING)

        self.db.appointment.update(
            {
                'hospcode': doc['hospcode'],
                'pid': doc['pid'],
                'seq': doc['seq'],
                'aptype': doc['aptype']
            },
            {'$set': doc},
            True
        )

