# -*- coding: utf8

import pymongo


class Home():

    def __init__(self, db, rows, rec_id):
        self.db = db
        self.rows = rows
        self.rec_id = rec_id

    def do_import(self):

        for data in self.rows:
            lat = float(data['LATITUDE']) if data['LATITUDE'] else None
            lng = float(data['LONGITUDE']) if data['LONGITUDE'] else None
            doc = {
                'hospcode': str(data['HOSPCODE']),
                'hid': data['HID'],
                'house_id': data['HOUSE_ID'],
                'housetype': data['HOUSETYPE'],
                'roomno': data['ROOMNO'],
                'condo': data['CONDO'],
                'house': data['HOUSE'],
                'soisub': data['SOISUB'],
                'soimain': data['SOIMAIN'],
                'road': data['ROAD'],
                'villaname': data['VILLANAME'],
                'village': data['VILLAGE'],
                'tambon': data['TAMBON'],
                'ampur': data['AMPUR'],
                'changwat': data['CHANGWAT'],
                'telephone': data['TELEPHONE'],
                #'latitude': data['LATITUDE'],
                #'longitude': data['LONGITUDE'],
                'loc': [lat, lng],
                'nfamily': data['NFAMILY'],
                'locatype': data['LOCATYPE'],
                'vhvid': data['VHVID'],
                'headid': data['HEADID'],
                'toilet': data['TOILET'],
                'water': data['WATER'],
                'watertype': data['WATERTYPE'],
                'garbage': data['GARBAGE'],
                'housing': data['HOUSING'],
                'durability': data['DURABILITY'],
                'cleanliness': data['CLEANLINESS'],
                'ventilation': data['VENTILATION'],
                'light': data['LIGHT'],
                'watertm': data['WATERTM'],
                'mfood': data['MFOOD'],
                'bcontrol': data['BCONTROL'],
                'acontrol': data['ACONTROL'],
                'chemical': data['CHEMICAL'],
                'outdate': data['OUTDATE'],
                'd_update': data['D_UPDATE'],
                'rec_id': self.rec_id
            }
            #print(doc)
            self.do_update(doc)
            #self.save_latlng(doc)

    def do_update(self, data):
        self.db.home.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.home.ensure_index('hid', pymongo.ASCENDING)

        # Check loc[,] 
        # if not null do update loc else don't update loc
        if data['loc'][0] is not None:
            self.db.home.update(
                {
                    'hospcode': data['hospcode'],
                    'hid': data['hid']
                },
                {'$set': data},
                True
            )
        else:
            self.db.home.update(
                {
                    'hospcode': data['hospcode'],
                    'hid': data['hid']
                }, {
                    '$set': {
                        'hospcode': data['hospcode'],
                        'hid': data['hid'],
                        'house_id': data['house_id'],
                        'housetype': data['housetype'],
                        'roomno': data['roomno'],
                        'condo': data['condo'],
                        'house': data['house'],
                        'soisub': data['soisub'],
                        'soimain': data['soimain'],
                        'road': data['road'],
                        'villaname': data['villaname'],
                        'village': data['village'],
                        'tambon': data['tambon'],
                        'ampur': data['ampur'],
                        'changwat': data['changwat'],
                        'telephone': data['telephone'],
                        #'latitude': data['latitude'],
                        #'longitude': data['longitude'],
                        'loc': [None, None],
                        'nfamily': data['nfamily'],
                        'locatype': data['locatype'],
                        'vhvid': data['vhvid'],
                        'headid': data['headid'],
                        'toilet': data['toilet'],
                        'water': data['water'],
                        'watertype': data['watertype'],
                        'garbage': data['garbage'],
                        'housing': data['housing'],
                        'durability': data['durability'],
                        'cleanliness': data['cleanliness'],
                        'ventilation': data['ventilation'],
                        'light': data['light'],
                        'watertm': data['watertm'],
                        'mfood': data['mfood'],
                        'bcontrol': data['bcontrol'],
                        'acontrol': data['acontrol'],
                        'chemical': data['chemical'],
                        'outdate': data['outdate'],
                        'd_update': data['d_update'],
                        'rec_id': data['rec_id']
                    }
                },
                True
            )

