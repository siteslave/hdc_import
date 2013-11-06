# -*- coding: utf8

import pymongo


class Village():

    def __init__(self, db, rows, rec_id):
        self.db = db
        self.rows = rows
        self.rec_id = rec_id

    def do_import(self):

        for data in self.rows:
            doc = {
                'hospcode': data['HOSPCODE'],
                'vid': data['VID'],
                'ntraditional': data['NTRADITIONAL'],
                'nmonk': data['NMONK'],
                'nreligionleader': data['NRELIGIONLEADER'],
                'nbroadcast': data['NBROADCAST'],
                'nradio': data['NRADIO'],
                'npchc': data['NPCHC'],
                'nclinic': data['NCLINIC'],
                'ndrugstore': data['NDRUGSTORE'],
                'nchildcenter': data['NCHILDCENTER'],
                'npschool': data['NPSCHOOL'],
                'nsschool': data['NSSCHOOL'],
                'ntemple': data['NTEMPLE'],
                'nreligiousplace': data['NRELIGIOUSPLACE'],
                'nmarket': data['NMARKET'],
                'nshop': data['NSHOP'],
                'nfoodshop': data['NFOODSHOP'],
                'nstall': data['NSTALL'],
                'nraintank': data['NRAINTANK'],
                'nchickenfarm': data['NCHICKENFARM'],
                'npigfarm': data['NPIGFARM'],
                'wastewater': data['WASTEWATER'],
                'garbage': data['GARBAGE'],
                'nfactory': data['NFACTORY'],
                'latitude': data['LATITUDE'],
                'longitude': data['LONGITUDE'],
                'outdate': data['OUTDATE'],
                'numactually': data['NUMACTUALLY'],
                'risktype': data['RISKTYPE'],
                'numstateless': data['NUMSTATELESS'],
                'nexerciseclub': data['NEXERCISECLUB'],
                'nolderlyclub': data['NOLDERLYCLUB'],
                'ndisableclub': data['NDISABLECLUB'],
                'nnumberoneclub': data['NNUMBERONECLUB'],
                'd_update': data['D_UPDATE'],
                'rec_id': self.rec_id
            }
            #print(doc)
            self.do_update(doc)

    def do_update(self, doc):
        self.db.village.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.village.ensure_index('vid', pymongo.ASCENDING)

        self.db.village.update(
            {
                'hospcode': doc['hospcode'],
                'vid': doc['vid']
            },
            {'$set': doc},
            True
        )

