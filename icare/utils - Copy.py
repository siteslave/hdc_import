# -*- coding: utf8

import pymongo
import sys
import threading


class Utils(threading.Thread):

    def __init__(self, db, rec_id):

        threading.Thread.__init__(self)
        self.db = db
        self.rec_id = rec_id

    def get_cid_from_pid(self, pid='', hospcode=''):

        self.db.person.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.person.ensure_index('pid', pymongo.ASCENDING)

        try:
            rs = self.db.person.find_one({
                'hospcode': hospcode,
                'pid': pid,
            })

            return rs['cid']

        except Exception as ex:
            sys.stderr.write('[ERROR] %s\n. [hospcode=%s, pid=%s]' % (ex.message, hospcode, pid))
            return '-'

    # def log(self, msg=''):
    #     doc = {
    #         'msg': msg,
    #         'last_update': datetime.today().ctime()
    #     }
    #
    #     self.db.logs.insert(doc)

    def run(self):

        self.process_anc()
        self.process_diagnosis_opd()
        self.process_diagnosis_ipd()
        self.process_procedure_opd()
        self.process_procedure_ipd()
        self.process_service()
        self.process_drug_opd()
        self.process_drug_ipd()
        self.process_death()
        self.process_card()
        self.process_drug_allergy()
        self.process_disability()
        self.process_appointment()
        self.process_accident()
        self.process_charge_ipd()
        self.process_charge_opd()
        self.process_admission()
        self.process_surveillance()
        self.process_women()
        self.process_fp()
        self.process_epi()
        self.process_nutrition()
        self.process_prenatal()
        self.process_labfu()
        self.process_labor()
        self.process_postnatal()
        self.process_newborn()
        self.process_newborn_care()
        self.process_dental()
        self.process_specialpp()
        self.process_ncdscreen()
        self.process_chronic()
        self.process_chronicfu()
        self.process_community_service()
        self.process_icf()
        self.process_rehabilitation()
        self.process_functional()
        self.process_community_activity()

    def process_anc(self):

        self.db.anc.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.anc.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.anc.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.anc.ensure_index('rec_id', pymongo.ASCENDING)

        data = self.db.anc.find({'rec_id': self.rec_id})

        for i in data:
            self.db.anc.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})
    #
    # def process_anc_coverages(self):
    #
    #     reducer = Code("""
    #             function(curr, result) {}
    #     """)
    #
    #     self.db.anc.ensure_index('hospcode', pymongo.ASCENDING)
    #     self.db.anc.ensure_index('pid', pymongo.ASCENDING)
    #     self.db.anc.ensure_index('gravida', pymongo.ASCENDING)
    #     self.db.anc.ensure_index('rec_id', pymongo.ASCENDING)
    #
    #     data = self.db.anc.group(
    #         key={'hospcode': 1, 'pid': 1, 'gravida': 1},
    #         condition={'rec_id': self.rec_id},
    #         initial={},
    #         reduce=reducer)
    #
    #     #self.db['anc_coverages'].remove({'hospcode': self.request.session['hospcode']})
    #
    #     try:
    #         self.db.anc_coverages.ensure_index('hospcode', pymongo.ASCENDING)
    #         self.db.anc_coverages.ensure_index('pid', pymongo.ASCENDING)
    #         self.db.anc_coverages.ensure_index('gravida', pymongo.ASCENDING)
    #
    #         for i in data:
    #             doc = {
    #                 'hospcode': i['hospcode'],
    #                 'pid': i['pid'],
    #                 'cid': self.get_cid_from_pid(i['pid'], i['hospcode']),
    #                 'gravida': i['gravida']
    #             }
    #
    #             self.db.anc_coverages.update({
    #                 'hospcode': i['hospcode'],
    #                 'pid': i['pid'],
    #                 'gravida': i['gravida']
    #             }, {'$set': doc}, True)
    #
    #     except Exception as e:
    #         sys.stderr.write('[PROCESS] ANC_COVERAGES: %s' % e)

    def process_card(self):

        self.db.card.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.card.ensure_index('pid', pymongo.ASCENDING)
        self.db.card.ensure_index('rec_id', pymongo.ASCENDING)

        data = self.db.card.find({'rec_id': self.rec_id})

        for i in data:
            self.db.card.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})

    def process_death(self):

        self.db.death.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.death.ensure_index('pid', pymongo.ASCENDING)
        self.db.death.ensure_index('rec_id', pymongo.ASCENDING)

        data = self.db.death.find({'rec_id': self.rec_id})

        for i in data:
            self.db.death.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})

    def process_drug_allergy(self):

        self.db.drugallergy.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.drugallergy.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.drugallergy.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.drugallergy.find({'rec_id': self.rec_id})

        for i in data:
            self.db.drugallergy.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})

    def process_service(self):

        self.db.service.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.service.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.service.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.service.find({'rec_id': self.rec_id})

        for i in data:
            self.db.service.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})

    def process_appointment(self):

        self.db.appointment.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.appointment.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.appointment.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.appointment.find({'rec_id': self.rec_id})

        for i in data:
            self.db.appointment.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})

    def process_accident(self):

        self.db.accident.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.accident.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.accident.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.accident.find({'rec_id': self.rec_id})

        for i in data:
            self.db.accident.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})

    def process_diagnosis_opd(self):

        self.db.diagnosis_opd.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.diagnosis_opd.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.diagnosis_opd.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.diagnosis_opd.find({'rec_id': self.rec_id})

        for i in data:
            self.db.diagnosis_opd.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})

    def process_procedure_opd(self):

        self.db.procedure_opd.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.procedure_opd.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.procedure_opd.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.procedure_opd.find({'rec_id': self.rec_id})

        for i in data:
            self.db.procedure_opd.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})

    def process_drug_opd(self):

        self.db.drug_opd.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.drug_opd.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.drug_opd.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.drug_opd.find({'rec_id': self.rec_id})

        for i in data:
            self.db.drug_opd.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})

    def process_charge_opd(self):

        self.db.charge_opd.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.charge_opd.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.charge_opd.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.charge_opd.find({'rec_id': self.rec_id})

        for i in data:
            self.db.charge_opd.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})

    def process_admission(self):

        self.db.admission.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.admission.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.admission.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.admission.find({'rec_id': self.rec_id})

        for i in data:
            self.db.admission.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})

    def process_diagnosis_ipd(self):

        self.db.diagnosis_ipd.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.diagnosis_ipd.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.diagnosis_ipd.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.diagnosis_ipd.find({'rec_id': self.rec_id})

        for i in data:
            self.db.diagnosis_ipd.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})

    def process_procedure_ipd(self):

        self.db.procedure_ipd.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.procedure_ipd.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.procedure_ipd.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.procedure_ipd.find({'rec_id': self.rec_id})

        for i in data:
            self.db.procedure_ipd.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})

    def process_drug_ipd(self):

        self.db.drug_ipd.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.drug_ipd.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.drug_ipd.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.drug_ipd.find({'rec_id': self.rec_id})

        for i in data:
            self.db.drug_ipd.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})

    def process_charge_ipd(self):

        self.db.charge_ipd.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.charge_ipd.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.charge_ipd.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.charge_ipd.find({'rec_id': self.rec_id})

        for i in data:
            self.db.charge_ipd.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})

    def process_surveillance(self):

        self.db.surveillance.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.surveillance.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.surveillance.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.surveillance.find({'rec_id': self.rec_id})

        for i in data:
            self.db.surveillance.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})

    def process_women(self):

        self.db.women.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.women.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.women.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.women.find({'rec_id': self.rec_id})

        for i in data:
            self.db.women.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})

    def process_fp(self):

        self.db.fp.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.fp.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.fp.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.fp.find({'rec_id': self.rec_id})

        for i in data:
            self.db.fp.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})

    def process_epi(self):

        self.db.epi.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.epi.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.epi.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.epi.find({'rec_id': self.rec_id})

        for i in data:
            self.db.epi.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})

    def process_nutrition(self):

        self.db.nutrition.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.nutrition.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.nutrition.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.nutrition.find({'rec_id': self.rec_id})

        for i in data:
            self.db.nutrition.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})

    def process_prenatal(self):

        self.db.prenatal.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.prenatal.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.prenatal.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.prenatal.find({'rec_id': self.rec_id})

        for i in data:
            self.db.prenatal.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})

    def process_labor(self):

        self.db.labor.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.labor.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.labor.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.labor.find({'rec_id': self.rec_id})

        for i in data:
            self.db.labor.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})

    def process_postnatal(self):

        self.db.postnatal.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.postnatal.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.postnatal.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.postnatal.find({'rec_id': self.rec_id})

        for i in data:
            self.db.postnatal.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})

    def process_newborn(self):

        self.db.newborn.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.newborn.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.newborn.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.newborn.find({'rec_id': self.rec_id})

        for i in data:
            self.db.newborn.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})

    def process_newborn_care(self):

        self.db.newborncare.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.newborncare.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.newborncare.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.newborncare.find({'rec_id': self.rec_id})

        for i in data:
            self.db.newborncare.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})

    def process_dental(self):

        self.db.dental.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.dental.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.dental.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.dental.find({'rec_id': self.rec_id})

        for i in data:
            self.db.dental.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})

    def process_specialpp(self):

        self.db.specialpp.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.specialpp.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.specialpp.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.specialpp.find({'rec_id': self.rec_id})

        for i in data:
            self.db.specialpp.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})

    def process_ncdscreen(self):

        self.db.ncdscreen.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.ncdscreen.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.ncdscreen.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.ncdscreen.find({'rec_id': self.rec_id})

        for i in data:
            self.db.ncdscreen.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})

    def process_chronic(self):

        self.db.chronic.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.chronic.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.chronic.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.chronic.find({'rec_id': self.rec_id})

        for i in data:
            self.db.chronic.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})

    def process_chronicfu(self):

        self.db.chronicfu.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.chronicfu.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.chronicfu.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.chronicfu.find({'rec_id': self.rec_id})

        for i in data:
            self.db.chronicfu.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})

    def process_labfu(self):

        self.db.labfu.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.labfu.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.labfu.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.labfu.find({'rec_id': self.rec_id})

        for i in data:
            self.db.labfu.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})

    def process_community_service(self):

        self.db.community_service.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.community_service.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.community_service.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.community_service.find({'rec_id': self.rec_id})

        for i in data:
            self.db.community_service.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})

    def process_disability(self):

        self.db.disability.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.disability.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.disability.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.disability.find({'rec_id': self.rec_id})

        for i in data:
            self.db.disability.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})

    def process_icf(self):

        self.db.icf.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.icf.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.icf.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.icf.find({'rec_id': self.rec_id})

        for i in data:
            self.db.icf.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})

    def process_functional(self):

        self.db.functional.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.functional.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.functional.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.functional.find({'rec_id': self.rec_id})

        for i in data:
            self.db.functional.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})

    def process_rehabilitation(self):

        self.db.rehabilitation.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.rehabilitation.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.rehabilitation.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.rehabilitation.find({'rec_id': self.rec_id})

        for i in data:
            self.db.rehabilitation.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})

    def process_community_activity(self):

        self.db.community_activity.ensure_index('rec_id', pymongo.ASCENDING)
        self.db.community_activity.ensure_index('hospcode', pymongo.ASCENDING)
        self.db.community_activity.ensure_index('pid', pymongo.ASCENDING)

        data = self.db.community_activity.find({'rec_id': self.rec_id})

        for i in data:
            self.db.community_activity.update({
                'hospcode': i['hospcode'],
                'pid': i['pid']
            }, {'$set': {'cid': self.get_cid_from_pid(i['pid'], i['hospcode'])}})