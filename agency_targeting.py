#!/usr/bin/python
import csv
import re

from collections import defaultdict

#todo don't require Excel cleanup

INPUT_FILE = "/Users/patrickmauro/Documents/20140517 - CH Targeting Model.csv"
PUMA_MAP_FILE = "/Users/patrickmauro/code/ch/puma-mapping.csv"

# We might not want to fill an agency to its capacity if we're worried about other emergency food programs filling up
# an agency's capacity.  CAPACITY_FACTOR is the percentage of real capacity that we use as our capacity threshold for
# each agency.  That is, a CAPACITY_FACTOR of 0.9 means we'll fill each agency up to 90% capacity.
CAPACITY_FACTOR = 1.0

VERBOSE = True

# -------------------------------------------------------------

class Region():
    def __init__(self, region_id, ef_demand, neighbors):
        self.id = region_id
        self.ef_demand = ef_demand
        self.neighboring_regions = neighbors

        self.agencies = []
        self.overage = 0

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


# Step 1 - Read input files
region_map = dict()
#todo put error checking from check_puma_mapping.py in here
with open(PUMA_MAP_FILE, 'rbU') as csvfile:
    reader = csv.reader(csvfile)

    # skip the header rows
    next(reader, None)

    for row in reader:
        region_id = int(row[0])
        neighbors = map(lambda x: int(x), row[1].split(","))

        if region_id in region_map:
            print("Region id %d appears twice in region mapping file" % region_id)
        else:
            region_map[region_id] = set(neighbors)

regions = dict() # region_id -> Region
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
        # region_name = row[2]
        region_id = int(row[2])
        region_demand = int(row[3])
        meals_served = int(row[4])
        agency_region_count = int(row[5])
        pctg = row[6] # todo fix output format from excel

        tgt_pounds = re.sub("[, ]", "", row[7].rstrip())
        tgt_pounds = int(tgt_pounds) if tgt_pounds.isdigit() else 0

        capacity = re.sub("[, ]", "", row[8].rstrip())
        capacity = int(capacity) if capacity.isdigit() else 0

        if region_id not in regions:
            if region_id not in region_map:
                print("Could not find region->neighbor mapping for region %d" % region_id)
                continue
            regions[region_id] = Region(region_id, region_demand, region_map[region_id])
        else:
            if regions[region_id].ef_demand != region_demand:
                print("Differing region demands ( %d v. %d ) for region %d" %
                    (regions[region_id].ef_demand, region_demand, region_id))

        regions[region_id].add_agency(Agency(ch_id, tgt_pounds, capacity, meals_served))


# Step 2 - Redistribute within a region, from agency with most bandwidth to least bandwidth
for region in regions.values():
    #todo Move this to Region method?
    overage = 0
    for agency in region.agencies:
        if agency.get_overage() > 0:
            overage += agency.get_overage()
            agency.cur_tgt = agency.get_capacity()

    if overage > 0:
        region.overage = region.distrib_overage(overage)

# Step 3 - Redistribute among neighboring regions, from region with most need to region with least need
def distrib_btwn_regions(restrict_to_neighbors):
    # a) only distribute to regions w/o overage b/c such regions are already full
    # b) sort from most need to least need
    # c) make this a list of region ids
    available_regions = filter(lambda x: x.overage == 0, regions.values())
    available_regions = sorted(available_regions, key=lambda x: x.ef_demand, reverse=True)
    available_regions = map(lambda x: x.id, available_regions)

    # now start distributing the overage
    regions_w_overage = filter(lambda x: x.overage > 0, regions.values())
    # start sorting from the region with the most overage to make this algorithm more stable
    for source_region in sorted(regions_w_overage, key=lambda x: x.overage, reverse=True):
        if restrict_to_neighbors:
            # Get neighboring regions in the order in which we'll check them
            target_region_set = filter(lambda x: x in source_region.neighboring_regions, available_regions)
        else:
            target_region_set = available_regions

        for target_region_id in target_region_set:
            target_region = regions[target_region_id]
            new_overage = target_region.distrib_overage(source_region.overage)

            if VERBOSE and new_overage != source_region.overage:
                print("Transferring %d lbs from region %d to %d -- neighbors? %s" %
                      (source_region.overage - new_overage, source_region.id, target_region.id,
                       str(restrict_to_neighbors)))

            source_region.overage = new_overage

            if source_region.overage == 0:
                break

distrib_btwn_regions(True)

# Step 4 - Deal with remainder: Redistribute among regions, from region with most need to region with least need
distrib_btwn_regions(False)

# Step 5 - Output
for region in regions.values():
    for agency in region.agencies:
        print("%s,%s,%d,%d,%d" % (region.id, agency.ch_id, agency.init_tgt, agency.cur_tgt, agency.true_capacity))

for region in regions.values():
    if VERBOSE and region.overage > 0:
        print("Region %d has unallocated overage of %d" % (region.id, region.overage))
