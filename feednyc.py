#!/usr/bin/python
import pickle
import csv
import math
import copy

from collections import defaultdict

#todo are pickles weird?
#todo fill in agency type in EFRO Map CSV
#todo exclude agencies from filters

# -----------------------

#todo More elegant unicode decoding?

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
    def __init__(self, pickle_tuple):
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

    def __str__(self):
        return "%(id)s,\"%(name)s\",%(efro)d,%(month)d,%(elderly)d,%(adult)d,%(children)d,%(factor)d" % \
               {'id': str(self.efro) + "-" + str(self.sampleMonth),
                'name': self.name,
                'efro': self.efro,
                'month': self.sampleMonth,
                'elderly': self.elderlyServed,
                'adult': self.adultsServed,
                'children': self.childrenServed,
                'factor': self.mealFactor}


class BaseFilter():

    def __init__(self):
        # stores all bad data for this filter
        self.badData = defaultdict(list) # efro -> datum

    # flag should be a string that can uniquely identify the filter
    def print_bad_data(self, flag, datumExtraStr = None, filterFn = None):
        for efroBD in self.badData.values():
            for badDatum in efroBD:
                if badDatum.sampleMonth < OUTPUT_MIN or badDatum.sampleMonth > OUTPUT_MAX:
                    continue

                if filterFn is not None and filterFn(badDatum):
                    continue

                extraStr = "" if datumExtraStr is None else "%s," % datumExtraStr(badDatum)
                print ("%s,%s,%s" % (flag, badDatum, extraStr))


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
            totServed2datum = defaultdict(list)
            for month, datum in efroSet.items():
                totServed = datum.childrenServed + datum.adultsServed + datum.elderlyServed
                # we print out zeros elsewhere (ZeroFilter)
                if totServed == 0:
                    continue
                histo[totServed] += 1
                totServed2datum[totServed].append(datum)

            totServedSet = set(histo.keys())
            badTotServed = set()
            for totServed, count in histo.items():
                thresh = max(self.ABS_THRESH, self.REL_THRESH * totServed)

                totCount = 0
                curBadTotServed = set()
                for n in filter(lambda x: abs(x - totServed) <= thresh, totServedSet):
                    totCount += histo[n]
                    curBadTotServed.add(n)

                if count >= self.SENSITIVITY:
                    badTotServed.update(curBadTotServed)

            for n in badTotServed:
                self.badData[efro].extend(totServed2datum[n])

    def print_bad_data(self):
        BaseFilter.print_bad_data(self, "too-similar")


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


#-----------------------------------------------------------------------------

if __name__ == "__main__":
    # Figure out our date range
    minY = int(math.floor(ANALYSIS_MIN / 100))
    minM = int(ANALYSIS_MIN % 100)
    maxY = int(math.floor(ANALYSIS_MAX / 100))
    maxM = int(ANALYSIS_MAX % 100)
    # we need to do this to split on fiscal years correctly
    # FYxx is from July 1, 20xx-1 to June 30, 20xx, i.e., FYxx ends in 20xx
    if minM > 6:
        minY += 1
    if maxM > 6:
        maxY += 1
    analysisYears = range(minY, maxY + 1)

    # STEP 1
    # Read in the EFRO Map
    chAcct2efro = dict()
    name2efro = defaultdict(set)
    efro2data = dict()

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

            chAcct2efro[efro_data.chAcct] = efro_data.efro
            efro2data[efro_data.efro] = efro_data

            if efro_data.name in name2efro:
                # If a name-efro pair is in the CSV more than once, this is an error.
                if efro_data.efro in name2efro[efro_data.name]:
                    print "Duplicate entries for name '%s' and efro '%s'." % (efro_data.name, efro_data.efro)

                # Make sure that two agencies with the same name have different agency types
                for existing_efro in name2efro[efro_data.name]:
                    existingAT = efro2data[existing_efro].agencyType
                    if efro_data.agencyType == existingAT:
                        print "Two agencies with the same name and agency type: %s, %s (%d, %d)" % \
                              (efro_data.name, efro_data.agencyType, efro_data.efro, existing_efro)

            name2efro[efro_data.name].add(efro_data.efro)

    # STEP 2
    # Process the active agencies list (from CH)

    # all the names of active agencies
    ch_names = set()
    # names for which we don't have an EFRO mapping
    names_wo_efromap = set()
    # efros that are active in our ranges AND have a efro mapping
    goodEFROs = set()
    
    with open(ACTIVE_AGENCY_CSV, 'rbU') as csvfile:
        reader = csv.reader(csvfile)

        # skip the header row
        next(reader, None)

        fyFilterSet = set(map(lambda x: "FY%s" % (x % 100), analysisYears))

        for row in reader:
            # get rid of the unicode nonsense (thanks MSFT!)
            row = map(lambda x: str(x.decode("ascii", "ignore")), row)

            alias = row[0]
            chAcct = row[1].replace("(", "").replace(")", "")
            name = row[2]
            year = row[3]

            ch_names.add(name)

            # filter by fiscal year if so specified
            if year not in fyFilterSet:
                continue

            if chAcct not in chAcct2efro:
                names_wo_efromap.add(name)
            else:
                goodEFROs.add(int(chAcct2efro[chAcct]))

            # The name of the agency might change in CH's database, so don't check
            # for consistency among names for the same chAcct

    # Do some error checking
    for name in set(name2efro.keys()) - ch_names:
        print("Mapping specified for name not in CH active agency file: %s" % name)

    print("%d CH name(s) missing from mapping file:" % len(names_wo_efromap))
    for name in names_wo_efromap:
        print("\t%s" % name)

    # STEP 3
    # Read in the pickles
    fnData = defaultdict(dict) # efro->month->datum
    for year in analysisYears:
        with open(FEEDNYC_PICKLE_DIR + "FeedNYC-All-%d.pickle" % year, 'r') as pickleFile:
            for curTup in pickle.load(pickleFile):
                datum = FeedNYCDatum(curTup)
                if datum.sampleMonth < ANALYSIS_MIN or datum.sampleMonth > ANALYSIS_MAX:
                    continue

                # We dump all agency data from FeedNYC, so we need to filter by the EFROs that are for active agencies
                # for the target years.
                if datum.efro in goodEFROs:
                    fnData[datum.efro][datum.sampleMonth] = datum

    # print out an error if we didn't see all the efros we expected
    for efro in goodEFROs - set(fnData.keys()):
        print("Did not see data for efro %d (%s)" % (efro, efro2data[efro].name))

    # STEP 4
    # Now do some filtering
    f = MealFactorFilter()
    f.filter(fnData)
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
