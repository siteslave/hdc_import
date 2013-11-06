# -*- coding: utf8

import pymongo


class CommunityActivity():

    def __init__(self, db, rows, rec_id):
        self.db = db
        self.rows = rows
        self.rec_id = rec_id

    def do_import(self):

        for data in self.rows:
            doc = {
                'hospcode': data['HOSPCODE'],
                'vid': data['VID'],
                'date_start': data['DATE_START'],
                'date_finish': data['DATE_FINISH'],
                'comactivity': data['COMACTIVITY'],
                'provider': data['PROVIDER'],
                'd_update': data['D_UPDATE'],
                'rec_id': self.rec_id
            }
            #print(doc)
            self.do_update(doc)

    def do_update(self, doc):
        self.db.community_activity.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.community_activity.ensure_index('vid', pymongo.ASCENDING)
        self.db.community_activity.ensure_index('date_start', pymongo.ASCENDING)
        self.db.community_activity.ensure_index('comactivity', pymongo.ASCENDING)

        self.db.community_activity.update(
            {
                'hospcode': doc['hospcode'],
                'vid': doc['vid'],
                'comactivity': doc['comactivity'],
                'date_start': doc['date_start']
            },
            {'$set': doc},
            True
        )

