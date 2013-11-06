# -*- coding: utf8

import pymongo


class Service():

    def __init__(self, db, rows, rec_id):
        self.db = db
        self.rows = rows
        self.rec_id = rec_id

    def do_import(self):

        for data in self.rows:
            doc = {
                'hospcode': data['HOSPCODE'],
                'pid': data['PID'],
                'hn': data['HN'],
                'seq': data['SEQ'],
                'date_serv': data['DATE_SERV'],
                'time_serv': data['TIME_SERV'],
                'location': data['LOCATION'],
                'intime': data['INTIME'],
                'instype': data['INSTYPE'],
                'insid': data['INSID'],
                'main': data['MAIN'],
                'typein': data['TYPEIN'],
                'referinhosp': data['REFERINHOSP'],
                'causein': data['CAUSEIN'],
                'chiefcomp': data['CHIEFCOMP'],
                'servplace': data['SERVPLACE'],
                'btemp': data['BTEMP'],
                'sbp': data['SBP'],
                'dbp': data['DBP'],
                'pr': data['PR'],
                'rr': data['RR'],
                'typeout': data['TYPEOUT'],
                'referouthosp': data['REFEROUTHOSP'],
                'causeout': data['CAUSEOUT'],
                'cost': data['COST'],
                'price': data['PRICE'],
                'payprice': data['PAYPRICE'],
                'actualpay': data['ACTUALPAY'],
                'd_update': data['D_UPDATE'],
                'rec_id': self.rec_id
            }
            #print(doc)
            self.do_update(doc)

    def do_update(self, doc):
        self.db.service.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.service.ensure_index('pid', pymongo.ASCENDING)
        self.db.service.ensure_index('seq', pymongo.ASCENDING)

        self.db.service.update(
            {
                'hospcode': doc['hospcode'],
                'pid': doc['pid'],
                'seq': doc['seq']
            },
            {'$set': doc},
            True
        )

