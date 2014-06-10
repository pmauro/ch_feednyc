#!/usr/bin/python
import csv
import re

from collections import defaultdict

#todo regional demand
#todo regional mapping

INPUT_FILE = "/Users/patrickmauro/Documents/20140517 - CH Targeting Model.csv"

# We might not want to fill an agency to its capacity if we're worried about other emergency food programs filling up
# an agency's capacity.  CAPACITY_FACTOR is the precentage of real capacity that we use as our capacity threshold for
# each agency.  That is, a CAPACITY_FACTOR of 0.9 means we'll fill each agency up to 90% capacity.
CAPACITY_FACTOR = 1.0

# -------------------------------------------------------------

class Region():
    def __init__(self, region_id):
        self.id = region_id

        self.agencies = []
        self.excess = 0

    def add_agency(self, agency):
        self.agencies.append(agency)

    # returns unallocated portion of distrib_amt
    # allocates by filling agencies to capacity, from agency with most bandwidth to agency with least bandwidth
    def distrib_overage(self, distrib_amt):  # get a set of agencies that have capacity
        # Get the agencies to which we can distribute food
        available_agencies = filter(lambda x: x.get_overage() < 0, self.agencies)

        # Allocate food from agency with most bandwidth to agency with least bandwidth
        for agency in sorted(available_agencies, key=lambda x: x.bandwidth, reverse=True):
            amt = min(distrib_amt, agency.adj_capacity - agency.cur_tgt)
            agency.cur_tgt += amt
            distrib_amt -= amt

            if distrib_amt <= 0:
                break

        return distrib_amt

class Agency():
    def __init__(self, ch_id, tgt_pounds, capacity, bandwidth):
        self.ch_id = ch_id
        self.init_tgt = tgt_pounds
        self.cur_tgt = tgt_pounds
        self.true_capacity = capacity
        self.adj_capacity = capacity * CAPACITY_FACTOR
        self.bandwidth = bandwidth

    def get_overage(self):
        return self.cur_tgt - self.adj_capacity

    def get_capacity(self):
        return self.adj_capacity

# -------------------------------------------------------------

regions = dict() # region_id -> Region

# Read input file
with open(INPUT_FILE, 'rbU') as csvfile:
    reader = csv.reader(csvfile)

    # skip the header rows
    next(reader, None)

    for row in reader:
        # get rid of the unicode nonsense (thanks MSFT!)
        row = map(lambda x: str(x.decode("ascii", "ignore")), row)

        if row[0] == "":
            continue

        agency_name = row[0]
        ch_id = row[1]
        region_name = row[2]
        region_id = row[3]
        meals_served = int(row[4])
        pctg = row[5] # todo FIX THIS OUTPUT FORMAT

        tgt_pounds = re.sub("[, ]", "", row[6].rstrip())
        tgt_pounds = int(tgt_pounds) if tgt_pounds.isdigit() else 0

        capacity = re.sub("[, ]", "", row[7].rstrip())
        capacity = int(capacity) if capacity.isdigit() else 0

        if region_id not in regions:
            regions[region_id] = Region(region_id)

        regions[region_id].add_agency(Agency(ch_id, tgt_pounds, capacity, meals_served))

# Redistribute within a region, from agency with most bandwidth to least bandwidth
for region in regions.values():
    #todo Move this to Region method?
    overage = 0
    for agency in region.agencies:
        if agency.get_overage() > 0:
            overage += agency.get_overage()
            agency.cur_tgt = agency.get_capacity()

    if overage > 0:
        region.overage = region.distrib_overage(overage)

# Redistribute among neighboring regions, from region with most need to region with least need

# Skip regions with overage
# Keep map of reverse checks

# Deal with remainder: Redistribute among regions, from region with most need to region with least need


for region in regions.values():
    for agency in region.agencies:
        print("%s,%s,%d,%d,%d" % (region.id, agency.ch_id, agency.init_tgt, agency.cur_tgt, agency.true_capacity))

# Print unallocated supply
