#!/usr/bin/python
import pickle
import csv
import math
import copy

from collections import defaultdict

#todo filters:
# defunct agencies
# things that have been checked out

#----------

#todo Make sure we have all EFROs for a given account
#todo More elegant unicode decoding?

# -----------------------------------------------------------------------------

# efro-map duplicate-efros
# Multiple entries in EFRO map for same EFRO
#
# efro-map duplicate-entries
# Multiple entries in EFRO map with same CH account, alias, and agency type
#
# active-agency never-active
# Entry in EFRO map for given agency but the agency isn't in the active agency file
#
# active-agency no-efro-entry
# No EFRO mapping in efro-map file for the given agency (CH account)
#
# feednyc-data no-data
# No data for an active agency
#

# -----------------------------------------------------------------------------

# Inputs (eventually these become command line/GUI options)
ANALYSIS_MAX = 201404
ANALYSIS_MIN = 201301
# output has to be a subset of analysis for this to DTRT
OUTPUT_MAX = 201404
OUTPUT_MIN = 201401

SIMILAR_SENSITIVITY = 3
SIMILAR_THRESH_ABS = 0
SIMILAR_THRESH_REL = 0

OUTLIER_MAX_STDDEV = 2.0

# -------

FEEDNYC_PICKLE_DIR = '/Users/patrickmauro/code/ch/pickles/'
ACTIVE_AGENCY_CSV = "/Users/patrickmauro/code/ch/active-agencies.csv"
EFRO_MAP_CSV = "/Users/patrickmauro/code/ch/efro-map.csv"

# -----------------------------------------------------------------------------
# Classes

# meal factor should be 9 or 1

class FeedNYCDatum:
    def __init__(self, pickle_tuple=None):
        if pickle_tuple != None:
            self.efro = pickle_tuple[0]
            self.name = pickle_tuple[1]
            self.type = pickle_tuple[2]
            self.address = pickle_tuple[3]
            self.district = pickle_tuple[4]
            self.boro = pickle_tuple[5]
            self.sampleMonth = int(pickle_tuple[6])
            self.elderlyServed = pickle_tuple[7]
            self.adultsServed = pickle_tuple[8]
            self.childrenServed = pickle_tuple[9]
            self.mealFactor = pickle_tuple[10]
            self.updateDate = pickle_tuple[11]
            self.updateUser = pickle_tuple[12]

        self.agencyType = ""
        self.flag = ""

    def print_header(self):
        return "efro-month_id,name,efro,month,elderly_served,adults_served,children_served,meal_factor," \
               "total_meals_served,agency_type,flag,"

    def __str__(self):
        total = (self.elderlyServed + self.adultsServed + self.childrenServed) * self.mealFactor

        return "%(id)s,\"%(name)s\",%(efro)d,%(month)02d-%(year)d,%(elderly)d," \
               "%(adult)d,%(children)d,%(factor)d,%(total)d,%(agencyType)s,%(flag)s," % \
               {'id': str(self.efro) + "-" + str(self.sampleMonth),
                'name': self.name,
                'efro': self.efro,
                'year': int(self.sampleMonth / 100),
                'month': self.sampleMonth % 100,
                'elderly': self.elderlyServed,
                'adult': self.adultsServed,
                'children': self.childrenServed,
                'factor': self.mealFactor,
                'total': total,
                'agencyType': self.agencyType,
                'flag': self.flag}


class BaseFilter():
    def __init__(self):
        # stores all bad data for this filter
        self.badData = defaultdict(list) # efro -> datum

    def print_header(self):
        datum = FeedNYCDatum()
        print "error_type,%s" % datum.print_header()

    def filter_by_date(self, datum):
        return datum.sampleMonth < OUTPUT_MIN or datum.sampleMonth > OUTPUT_MAX

    # flag should be a string that can uniquely identify the filter
    def print_bad_data(self, flag, filterFn = None):
        for efroBD in self.badData.values():
            for badDatum in efroBD:
                if filterFn == None:
                    filterFn = self.filter_by_date
                if filterFn(badDatum):
                    continue

                print "%s,%s" % (flag, badDatum)


class MealFactorFilter(BaseFilter):
    NORMAL_MFS = {1, 9}

    def filter(self, data):
        for efro, efroSet in data.items():
            for month, datum in efroSet.items():
                if datum.mealFactor not in self.NORMAL_MFS:
                    self.badData[efro].append(datum)

    def print_bad_data(self):
        BaseFilter.print_bad_data(self, "meal-factor")


class SimilarFilter(BaseFilter):
    def __init__(self, sensitivity=1, absThresh=0, relThresh=0):
        BaseFilter.__init__(self)

        self.SENSITIVITY = sensitivity
        self.ABS_THRESH = absThresh
        self.REL_THRESH = relThresh

    def filter(self, data):
        for efro, efroSet in data.items():
            histo = defaultdict(int)
            totServed2data = defaultdict(list)
            for month, datum in efroSet.items():
                totServed = datum.childrenServed + datum.adultsServed + datum.elderlyServed
                # we print out zeros elsewhere (ZeroFilter)
                if totServed == 0:
                    continue
                histo[totServed] += 1
                totServed2data[totServed].append(datum)

            totServedSet = set(histo.keys())
            badTotServed = set()
            for totServed, count in histo.items():
                thresh = max(self.ABS_THRESH, self.REL_THRESH * totServed)

                totCount = 0
                curBadTotServed = set()
                # figure out which counts are within the threshold
                for n in filter(lambda x: abs(x - totServed) <= thresh, totServedSet):
                    totCount += histo[n]
                    curBadTotServed.add(n)

                # if we've gotten too many data points within our window, they're all suspect
                if count >= self.SENSITIVITY:
                    # we only add this to the output if at least one of the currently bad entries is
                    # in the output window
                    got_recent_entry = False
                    for tot in curBadTotServed:
                        for datum in totServed2data[tot]:
                            if not self.filter_by_date(datum):
                                got_recent_entry = True
                                break
                        if got_recent_entry:
                            badTotServed.update(curBadTotServed)
                            break

            for n in badTotServed:
                for d in totServed2data[n]:
                    d = copy.copy(d)
                    d.flag = "c" if self.filter_by_date(d) else "r"
                    self.badData[efro].append(d)

    # We don't filter by date for this because entries that aren't in the output date range give context
    def filter_none(self, datum):
        return False

    def print_bad_data(self):
        BaseFilter.print_bad_data(self, "too-similar", filterFn=self.filter_none)


class SkippedDataFilter(BaseFilter):
    def adjust_month(self, month, amount):
        if amount == 0:
            return month

        year = math.floor(month / 100)
        month %= 100
        sign = 1 if amount > 0 else -1
        amount = abs(amount)

        yDiff = int(amount / 12)
        mDiff = amount % 12

        year += (yDiff * sign)
        month += (mDiff * sign)
        if month > 12:
            year += 1
            month -= 12
        elif month < 1:
            year -= 1
            month += 12

        return int(year * 100 + month)

    def months_between(self, begin, end):
        """
        Returns a list of months in [begin, end]

        N.B. This breaks if begin or end aren't a valid YYYYMM month.
        """
        if end < begin:
            return []

        month_list = []
        cur_month = begin
        while cur_month != end:
            month_list.append(cur_month)
            cur_month = self.adjust_month(cur_month, 1)
        month_list.append(end)

        return month_list

    def filter(self, data):
        req_months = set(self.months_between(OUTPUT_MIN, OUTPUT_MAX))

        for efro, efroSet in data.items():
            missing_months = req_months - set(efroSet.keys())

            for month in missing_months:
                sample_datum = copy.copy(efroSet.values()[0])
                sample_datum.elderlyServed = 0
                sample_datum.adultsServed = 0
                sample_datum.childrenServed = 0
                sample_datum.updateDate = ""
                sample_datum.updateUser = ""
                sample_datum.sampleMonth = month
                self.badData[efro].append(sample_datum)

    def print_bad_data(self):
        BaseFilter.print_bad_data(self, 'skipped-entries')


class ZeroFilter(BaseFilter):
    def filter(self, data):
        for efro, efroSet in data.items():
            for month, datum in efroSet.items():
                totServed = datum.childrenServed + datum.adultsServed + datum.elderlyServed
                if totServed == 0:
                    self.badData[efro].append(datum)

    def print_bad_data(self):
        BaseFilter.print_bad_data(self, 'zeros')


class OutlierFilter(BaseFilter):
    def __init__(self, stdDev):
            BaseFilter.__init__(self)
            self.sdThresh = stdDev

    def filter(self, data):
        for efro, efroSet in data.items():
            sum = 0
            sum2 = 0
            n = len(efroSet)
            for month, datum in efroSet.items():
                totServed = datum.childrenServed + datum.adultsServed + datum.elderlyServed
                sum += totServed
                sum2 += (totServed * totServed)

            # don't divide by 0
            if n <= 2:
                continue

            #todo Is it proper to subtract out the element we're testing?
            for month, datum in efroSet.items():
                totServed = datum.childrenServed + datum.adultsServed + datum.elderlyServed
                if totServed == 0:
                    continue

                nSum = sum - totServed
                nSum2 = sum2 - (totServed * totServed)

                mean = nSum / (n - 1)
                stdDev = math.sqrt((nSum2 / (n - 2)) - (mean * mean))

                if abs(totServed - mean) > stdDev * self.sdThresh:
                    self.badData[efro].append(datum)

    def print_bad_data(self):
        BaseFilter.print_bad_data(self, 'outlier')


def get_years_in_range(min_month, max_month):
    # inputs are YYYYMM
    # Figure out our date range
    minY = int(math.floor(min_month / 100))
    minM = int(min_month % 100)
    maxY = int(math.floor(max_month / 100))
    maxM = int(max_month  % 100)
    cal_year = range(minY, maxY)
    # we need to do this to split on fiscal years correctly
    # FYxx is from July 1, 20xx-1 to June 30, 20xx, i.e., FYxx ends in 20xx
    if minM > 6:
        minY += 1
    if maxM > 6:
        maxY += 1
    fiscal_year = range(minY, maxY + 1)

    return cal_year, fiscal_year

#-----------------------------------------------------------------------------

if __name__ == "__main__":
    # Figure out our date range
    analysis_range_cy, analysis_range_fy = get_years_in_range(ANALYSIS_MIN, ANALYSIS_MAX)
    output_range_cy, output_range_fy = get_years_in_range(OUTPUT_MIN, OUTPUT_MAX)

    # STEP 1
    # Read in the EFRO Map
    chAcct2efro = defaultdict(set)  # acct -> set(efros)
    efro2data = dict()              # efro -> data (should be unique)
    class EFROData():
        pass

    with open(EFRO_MAP_CSV, 'rbU') as csvfile:
        reader = csv.reader(csvfile)

        # skip the header rows
        next(reader, None)
        next(reader, None)

        for row in reader:
            # get rid of the unicode nonsense (thanks MSFT!)
            row = map(lambda x: str(x.decode("ascii", "ignore")), row)

            efro_data = EFROData()
            efro_data.efro = int(row[0])
            efro_data.chAcct = row[1]
            efro_data.idAlias = row[2]
            efro_data.name = row[3]
            efro_data.agencyType = row[4]

            # Make sure there aren't duplicate efros
            if efro_data.efro in efro2data:
                print "efro-map duplicate-efros\t%s\t%s" % (efro_data.efro, efro_data.name)
                continue

            # Also, make sure that if the account and alias are the same, then the agency types differ.
            if efro_data.chAcct in chAcct2efro:
                for old_efro in chAcct2efro[efro_data.chAcct]:
                    old_data = efro2data[old_efro]
                    if efro_data.idAlias == old_data.idAlias and efro_data.agencyType == old_data.agencyType:
                        print "efro-map duplicate-entries\t%s\t%-40s%s\t%s\tefros %d %d" % \
                              (efro_data.chAcct, efro_data.name, efro_data.idAlias, efro_data.agencyType,
                               efro_data.efro, old_efro)

            chAcct2efro[efro_data.chAcct].add(efro_data.efro)
            efro2data[efro_data.efro] = efro_data

    # STEP 2
    # Process the active agencies list (from CH)

    # names for which we don't have an EFRO mapping
    entries_wo_efromap = set()
    # efros that are active in our ranges AND have a efro mapping
    goodEFROs = set()
    # all the ch accts
    all_ch_accts = set()
    
    with open(ACTIVE_AGENCY_CSV, 'rbU') as csvfile:
        reader = csv.reader(csvfile)

        # skip the header row
        next(reader, None)

        fyFilterSet = set(map(lambda x: "FY%s" % (x % 100), output_range_fy))

        for row in reader:
            # get rid of the unicode nonsense (thanks MSFT!)
            row = map(lambda x: str(x.decode("ascii", "ignore")), row)

            alias = row[0]
            chAcct = row[1].replace("(", "").replace(")", "")
            name = row[2]
            year = row[3]

            all_ch_accts.add(chAcct)

            # filter by fiscal year if so specified
            if year not in fyFilterSet:
                continue

            if chAcct not in chAcct2efro:
                entries_wo_efromap.add((name, chAcct))
            else:
                goodEFROs.update(chAcct2efro[chAcct])

    # Print out agencies for which we have an entry in the EFRO map but no entry in the active agencies list
    for account in set(chAcct2efro.keys()) - all_ch_accts:
        name = efro2data[list(chAcct2efro[account])[0]].name
        print("active-agency never-active\t%s\t%s" % (account, name))

    # Print out agencies taht appear in the active agencies list but that don't have an EFRO mapping
    for name, account in entries_wo_efromap:
        print("active-agency no-efro-entry\t%s\t%s" % (account, name))

    # STEP 3
    # Read in the pickles
    # The data is stored by fiscal year.
    fnData = defaultdict(dict) # efro->month->datum
    for year in analysis_range_fy:
        with open(FEEDNYC_PICKLE_DIR + "FeedNYC-All-%d.pickle" % year, 'r') as pickleFile:
            for curTup in pickle.load(pickleFile):
                datum = FeedNYCDatum(curTup)
                if datum.sampleMonth < ANALYSIS_MIN or datum.sampleMonth > ANALYSIS_MAX:
                    continue

                # We dump all agency data from FeedNYC, so we need to filter by the EFROs that are for active agencies
                # for the target years.
                if datum.efro in goodEFROs:
                    datum.agencyType = efro2data[datum.efro].agencyType
                    fnData[datum.efro][datum.sampleMonth] = datum

    # print out an error if we didn't see all the efros we expected
    for efro in goodEFROs - set(fnData.keys()):
        print("feednyc-data no-data\t\t%d\t%s" % (efro, efro2data[efro].name))

    # STEP 4
    # Now do some filtering
    print ""

    f = MealFactorFilter()
    f.filter(fnData)
    f.print_header()
    f.print_bad_data()

    f = SimilarFilter(SIMILAR_SENSITIVITY, SIMILAR_THRESH_ABS, SIMILAR_THRESH_REL)
    f.filter(fnData)
    f.print_bad_data()

    f = SkippedDataFilter()
    f.filter(fnData)
    f.print_bad_data()

    f = ZeroFilter()
    f.filter(fnData)
    f.print_bad_data()

    f = OutlierFilter(OUTLIER_MAX_STDDEV)
    f.filter(fnData)
    f.print_bad_data()
