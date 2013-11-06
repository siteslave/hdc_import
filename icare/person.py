# -*- coding: utf8

import pymongo


class Person():

    def __init__(self, db, rows, rec_id):
        self.db = db
        self.rows = rows
        self.rec_id = rec_id

    def do_import(self):

        for data in self.rows:
            doc = {
                'hospcode': data['HOSPCODE'],
                'pid': data['PID'],
                'cid': data['CID'],
                'hid': data['HID'],
                'prename': data['PRENAME'],
                'name': data['NAME'],
                'lname': data['LNAME'],
                'hn': data['HN'],
                'sex': data['SEX'],
                'birth': data['BIRTH'],
                'mstatus': data['MSTATUS'],
                'occupation_old': data['OCCUPATION_OLD'],
                'occupation_new': data['OCCUPATION_NEW'],
                'race': data['RACE'],
                'nation': data['NATION'],
                'religion': data['RELIGION'],
                'education': data['EDUCATION'],
                'fstatus': data['FSTATUS'],
                'father': data['FATHER'],
                'mother': data['MOTHER'],
                'couple': data['COUPLE'],
                'vstatus': data['VSTATUS'],
                'movein': data['MOVEIN'],
                'discharge': data['DISCHARGE'],
                'ddischarge': data['DDISCHARGE'],
                'abogroup': data['ABOGROUP'],
                'rhgroup': data['RHGROUP'],
                'labor': data['LABOR'],
                'passport': data['PASSPORT'],
                'typearea': data['TYPEAREA'],
                'd_update': data['D_UPDATE'],
                'rec_id': self.rec_id
            }
            #print(doc)
            self.do_update(doc)

    def do_update(self, doc):
        self.db.person.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.person.ensure_index('cid', pymongo.ASCENDING)
        self.db.person.ensure_index('pid', pymongo.ASCENDING)

        self.db.person.update(
            {
                'hospcode': doc['hospcode'],
                'cid': doc['cid'],
                'pid': doc['pid']
            },
            {'$set': doc},
            True
        )