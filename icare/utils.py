# -*- coding: utf8

import pymongo
import sys
import threading


class Utils(threading.Thread):
    def __init__(self, db, t, rec_id):

        threading.Thread.__init__(self)
        self.db = db
        self.rec_id = rec_id
        self.t = t

    def get_cid_from_pid(self, pid='', hospcode=''):

        self.db.person.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.person.ensure_index('pid', pymongo.ASCENDING)

        rs = self.db.person.find_one({
            'hospcode': hospcode,
            'pid': pid,
        })

        if rs:
            return rs['cid']
        else:
            return None

    # def log(self, msg=''):
    #     doc = {
    #         'msg': msg,
    #         'last_update': datetime.today().ctime()
    #     }
    #
    #     self.db.logs.insert(doc)

    def run(self):
        if self.t == 'ANC':
            self.process_anc()
        elif self.t == 'DIAGNOSIS_OPD':
            self.process_diagnosis_opd()
        elif self.t == 'DIAGNOSIS_IPD':
            self.process_diagnosis_ipd()
        #elif self.t == 'PROCEDURE_OPD':
        #    self.process_procedure_opd()
        #elif self.t == 'PROCEDURE_IPD':
        #    self.process_procedure_ipd()
        elif self.t == 'SERVICE':
            self.process_service()
        #elif self.t == 'DRUG_OPD':
        #    self.process_drug_opd()
        #elif self.t == 'DRUG_IPD':
        #    self.process_drug_ipd()
        elif self.t == 'DEATH':
            self.process_death()
        #elif self.t == 'CARD':
        #    self.process_card()
        elif self.t == 'DRUG_ALLERGY':
            self.process_drug_allergy()
        elif self.t == 'DISABILITY':
            self.process_disability()
        elif self.t == 'APPOINTMENT':
            self.process_appointment()
        elif self.t == 'ACCIDENT':
            self.process_accident()
        #elif self.t == 'CHARGE_IPD':
        #    self.process_charge_ipd()
        #elif self.t == 'CHARGE_OPD':
        #    self.process_charge_opd()
        elif self.t == 'ADMISSION':
            self.process_admission()
        elif self.t == 'SURVEILLANCE':
            self.process_surveillance()
        elif self.t == 'WOMEN':
            self.process_women()
        elif self.t == 'FP':
            self.process_fp()
        elif self.t == 'EPI':
            self.process_epi()
        elif self.t == 'NUTRITION':
            self.process_nutrition()
        elif self.t == 'PRENATAL':
            self.process_prenatal()
        elif self.t == 'LABFU':
            self.process_labfu()
        elif self.t == 'LABOR':
            self.process_labor()
        elif self.t == 'POSTNATAL':
            self.process_postnatal()
        elif self.t == 'NEWBORN':
            self.process_newborn()
        elif self.t == 'NEWBORN_CARE':
            self.process_newborn_care()
        elif self.t == 'DENTAL':
            self.process_dental()
        elif self.t == 'SPECIALPP':
            self.process_specialpp()
        elif self.t == 'NCDSCREEN':
            self.process_ncdscreen()
        elif self.t == 'CHRONIC':
            self.process_chronic()
        elif self.t == 'CHRONICFU':
            self.process_chronicfu()
        elif self.t == 'COMMUNITY_SERVICE':
            self.process_community_service()
        elif self.t == 'ICF':
            self.process_icf()
        elif self.t == 'REHABILITATION':
            self.process_rehabilitation()
        elif self.t == 'FUNCTIONAL':
            self.process_functional()
        elif self.t == 'COMMUNITY_ACTIVITY':
            self.process_community_activity()
        else:
            pass

    def process_anc(self):

        self.db.anc.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.anc.ensure_index('pid', pymongo.ASCENDING)
        self.db.anc.ensure_index('rec_id', pymongo.ASCENDING)

        data = self.db.anc.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.anc.update({
                    'hospcode': i['hospcode'],
                    'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_card(self):

        self.db.card.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.card.ensure_index('pid', pymongo.ASCENDING)
        self.db.card.ensure_index('rec_id', pymongo.ASCENDING)

        data = self.db.card.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.card.update({
                    'hospcode': i['hospcode'],
                    'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_death(self):

        self.db.death.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.death.ensure_index('pid', pymongo.ASCENDING)
        self.db.death.ensure_index('rec_id', pymongo.ASCENDING)

        data = self.db.death.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.death.update({
                     'hospcode': i['hospcode'],
                     'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_drug_allergy(self):

        self.db.drugallergy.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.drugallergy.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.drugallergy.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.drugallergy.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.drugallergy.update({
                    'hospcode': i['hospcode'],
                    'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_service(self):

        self.db.service.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.service.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.service.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.service.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.service.update({
                    'hospcode': i['hospcode'],
                    'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_appointment(self):

        self.db.appointment.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.appointment.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.appointment.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.appointment.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.appointment.update({
                    'hospcode': i['hospcode'],
                    'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_accident(self):

        self.db.accident.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.accident.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.accident.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.accident.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.accident.update({
                    'hospcode': i['hospcode'],
                    'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_diagnosis_opd(self):

        self.db.diagnosis_opd.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.diagnosis_opd.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.diagnosis_opd.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.diagnosis_opd.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.diagnosis_opd.update({
                    'hospcode': i['hospcode'],
                    'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_procedure_opd(self):

        self.db.procedure_opd.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.procedure_opd.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.procedure_opd.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.procedure_opd.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.procedure_opd.update({
                    'hospcode': i['hospcode'],
                    'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_drug_opd(self):

        self.db.drug_opd.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.drug_opd.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.drug_opd.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.drug_opd.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.drug_opd.update({
                    'hospcode': i['hospcode'],
                    'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_charge_opd(self):

        self.db.charge_opd.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.charge_opd.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.charge_opd.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.charge_opd.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.charge_opd.update({
                    'hospcode': i['hospcode'],
                    'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_admission(self):

        self.db.admission.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.admission.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.admission.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.admission.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.admission.update({
                    'hospcode': i['hospcode'],
                    'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_diagnosis_ipd(self):

        self.db.diagnosis_ipd.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.diagnosis_ipd.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.diagnosis_ipd.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.diagnosis_ipd.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.diagnosis_ipd.update({
                    'hospcode': i['hospcode'],
                    'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_procedure_ipd(self):

        self.db.procedure_ipd.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.procedure_ipd.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.procedure_ipd.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.procedure_ipd.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.procedure_ipd.update({
                    'hospcode': i['hospcode'],
                    'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_drug_ipd(self):

        self.db.drug_ipd.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.drug_ipd.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.drug_ipd.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.drug_ipd.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.drug_ipd.update({
                    'hospcode': i['hospcode'],
                    'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_charge_ipd(self):

        self.db.charge_ipd.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.charge_ipd.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.charge_ipd.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.charge_ipd.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.charge_ipd.update({
                     'hospcode': i['hospcode'],
                     'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_surveillance(self):

        self.db.surveillance.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.surveillance.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.surveillance.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.surveillance.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.surveillance.update({
                    'hospcode': i['hospcode'],
                    'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_women(self):

        self.db.women.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.women.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.women.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.women.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.women.update({
                    'hospcode': i['hospcode'],
                    'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_fp(self):

        self.db.fp.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.fp.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.fp.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.fp.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.fp.update({
                    'hospcode': i['hospcode'],
                    'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_epi(self):

        self.db.epi.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.epi.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.epi.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.epi.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.epi.update({
                    'hospcode': i['hospcode'],
                    'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_nutrition(self):

        self.db.nutrition.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.nutrition.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.nutrition.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.nutrition.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.nutrition.update({
                    'hospcode': i['hospcode'],
                    'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_prenatal(self):

        self.db.prenatal.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.prenatal.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.prenatal.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.prenatal.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.prenatal.update({
                    'hospcode': i['hospcode'],
                    'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_labor(self):

        self.db.labor.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.labor.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.labor.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.labor.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.labor.update({
                    'hospcode': i['hospcode'],
                    'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_postnatal(self):

        self.db.postnatal.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.postnatal.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.postnatal.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.postnatal.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.postnatal.update({
                    'hospcode': i['hospcode'],
                    'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_newborn(self):

        self.db.newborn.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.newborn.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.newborn.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.newborn.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.newborn.update({
                    'hospcode': i['hospcode'],
                    'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_newborn_care(self):

        self.db.newborncare.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.newborncare.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.newborncare.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.newborncare.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.newborncare.update({
                    'hospcode': i['hospcode'],
                    'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_dental(self):

        self.db.dental.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.dental.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.dental.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.dental.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.dental.update({
                    'hospcode': i['hospcode'],
                    'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_specialpp(self):

        self.db.specialpp.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.specialpp.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.specialpp.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.specialpp.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.specialpp.update({
                    'hospcode': i['hospcode'],
                    'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_ncdscreen(self):

        self.db.ncdscreen.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.ncdscreen.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.ncdscreen.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.ncdscreen.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.ncdscreen.update({
                    'hospcode': i['hospcode'],
                    'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_chronic(self):

        self.db.chronic.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.chronic.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.chronic.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.chronic.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.chronic.update({
                    'hospcode': i['hospcode'],
                    'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_chronicfu(self):

        self.db.chronicfu.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.chronicfu.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.chronicfu.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.chronicfu.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.chronicfu.update({
                    'hospcode': i['hospcode'],
                    'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_labfu(self):

        self.db.labfu.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.labfu.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.labfu.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.labfu.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.labfu.update({
                    'hospcode': i['hospcode'],
                    'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_community_service(self):

        self.db.community_service.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.community_service.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.community_service.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.community_service.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.community_service.update({
                    'hospcode': i['hospcode'],
                    'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_disability(self):

        self.db.disability.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.disability.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.disability.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.disability.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.disability.update({
                    'hospcode': i['hospcode'],
                    'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_icf(self):

        self.db.icf.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.icf.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.icf.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.icf.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.icf.update({
                    'hospcode': i['hospcode'],
                    'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_functional(self):

        self.db.functional.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.functional.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.functional.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.functional.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.functional.update({
                    'hospcode': i['hospcode'],
                    'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_rehabilitation(self):

        self.db.rehabilitation.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.rehabilitation.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.rehabilitation.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.rehabilitation.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.rehabilitation.update({
                    'hospcode': i['hospcode'],
                    'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def process_community_activity(self):

        self.db.community_activity.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.community_activity.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.community_activity.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.community_activity.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                self.db.community_activity.update({
                    'hospcode': i['hospcode'],
                    'pid': i['pid']
                }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}}, multi=True)

    def save_latlng(self, doc):

        self.db.maps.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.maps.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.person.find({'rec_id': self.rec_id})

        if data:
            for i in data:
                latLng = self.get_latlng(i['hospcode'], i['hid'])
                self.db.maps.update({'pid': data['pid'], 'hospcode': data['hospcode']}, {'$set': {
                    'hospcode': doc['hospcode'],
                    'pid': doc['pid'],
                    'loc': latLng
                }}, True)

    def get_latlng(self, hospcode, hid):
        self.db.home.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.home.ensure_index('hid', pymongo.ASCENDING)

        rs = self.db.home.find_one({
            'hospcode': hospcode,
            'hid': hid
        })

        if rs:
            return [float(rs['latitude']), float(rs['longitude'])]
        else:
            return [None, None]