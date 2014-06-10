#!/usr/bin/python
import csv
import re

AGENCY_INPUT_FILE = "/Users/patrickmauro/Documents/Agency Targets.csv"
REGION_INPUT_FILE = "/Users/patrickmauro/Documents/Regional Targets.csv"
PUMA_MAP_FILE = "/Users/patrickmauro/code/ch/puma-mapping.csv"

VERBOSE = True

# -------------------------------------------------------------


def str_to_int(input_str):
    val = re.sub("[, ]", "", input_str.rstrip())
    val = int(val) if val.isdigit() else None
    return val


class Region():
    def __init__(self, _id, _ef_demand, _neighbors):
        self.id = _id
        self.ef_demand = _ef_demand
        self.neighboring_regions = _neighbors

        self.agencies = []
        self.overage = 0

    def add_agency(self, _agency):
        self.agencies.append(_agency)

    # returns unallocated portion of distrib_amt
    # allocates by filling agencies to capacity, from agency with most bandwidth to agency with least bandwidth
    def distrib_overage(self, distrib_amt):  # get a set of agencies that have capacity
        # Get the agencies to which we can distribute food
        available_agencies = filter(lambda x: x.get_overage() < 0, self.agencies)

        # Allocate food from agency with most bandwidth to agency with least bandwidth
        for tgt_agency in sorted(available_agencies, key=lambda x: x.bandwidth, reverse=True):
            amt = min(distrib_amt, tgt_agency.get_capacity())
            tgt_agency.cur_tgt += amt
            distrib_amt -= amt

            # should never be < 0, but let's be a bit silly here to be super-safe
            if distrib_amt <= 0:
                break

        return distrib_amt


class Agency():
    def __init__(self, _ch_id, _tgt_pounds, _capacity, _bandwidth):
        self.ch_id = _ch_id
        self.init_tgt = _tgt_pounds
        self.cur_tgt = _tgt_pounds
        self.capacity = _capacity
        self.bandwidth = _bandwidth

    def get_overage(self):
        return self.cur_tgt - self.capacity

    def get_capacity(self):
        return -1.0 * self.get_overage()


def distrib_btwn_regions(restrict_to_neighbors):
    # a) only distribute to regions w/o overage b/c such regions are already full
    # b) sort from most need to least need
    # c) make this a list of region ids, not Region objects
    available_regions = filter(lambda x: x.overage == 0, regions.values())
    available_regions = sorted(available_regions, key=lambda x: x.ef_demand, reverse=True)
    available_regions = map(lambda x: x.id, available_regions)

    # now start distributing the overage
    regions_w_overage = filter(lambda x: x.overage > 0, regions.values())
    # start sorting from the region with the most overage to make this algorithm more stable
    #todo Is this claim of greater stability true?
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

# -------------------------------------------------------------

# Step 1 - Read input files
neighbor_map = dict()
#todo put error checking from check_puma_mapping.py in here
with open(PUMA_MAP_FILE, 'rbU') as csvfile:
    reader = csv.reader(csvfile)

    # skip the header rows
    next(reader, None)

    for row in reader:
        region_id = int(row[0])
        neighbors = map(lambda x: int(x), row[1].split(","))

        if region_id in neighbor_map:
            print("Region id %d appears twice in region mapping file" % region_id)
        else:
            neighbor_map[region_id] = set(neighbors)

region_to_demand = dict()
region_to_target = dict()
with open(REGION_INPUT_FILE, 'rbU') as csvfile:
    reader = csv.reader(csvfile)

    # skip the header rows
    next(reader, None)

    for row in reader:
        if not row[0].isdigit():
            continue

        region_id = int(row[0])
        name = row[1]
        ppl_in_need = str_to_int(row[2])
        ef_demand_total = str_to_int(row[3])
        ef_demand_satisfied = str_to_int(row[4])
        ef_demand_residual = str_to_int(row[5])
        region_target = str_to_int(row[6])

        region_to_demand[region_id] = ef_demand_residual
        region_to_target[region_id] = region_target

regions = dict()  # region_id -> Region
with open(AGENCY_INPUT_FILE, 'rbU') as csvfile:
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
        efro = int(row[2])
        region_id = int(row[3])
        meals_served = int(row[4])
        agency_region_count = int(row[5])
        pctg = row[6]  # todo fix output format from excel
        tgt_pounds = str_to_int(row[7])
        capacity = str_to_int(row[8])

        if region_id not in regions:
            if region_id not in neighbor_map:
                print("Could not find region->neighbor mapping for region %d" % region_id)
                continue

            if region_id not in region_to_demand:
                print("Could not find region->demadn mapping for region %d" % region_id)
                continue

            regions[region_id] = Region(region_id, region_to_demand[region_id], neighbor_map[region_id])

        regions[region_id].add_agency(Agency(ch_id, tgt_pounds, capacity, meals_served))

# Some regions don't have agencies.  Set the overage for those regions appropriately.
regions_wo_agencies = set(region_to_target.keys()) - set(regions.keys())
for region_id in regions_wo_agencies:
    if region_id not in neighbor_map:
        print("Could not find region->neighbor mapping for region %d" % region_id)
        continue

    regions[region_id] = Region(region_id, region_to_demand[region_id], neighbor_map[region_id])
    regions[region_id].overage = region_to_target[region_id]

# Step 2 - Redistribute within a region, from agency with most bandwidth to least bandwidth
for region in regions.values():
    #todo Move this to Region method?
    overage = 0
    for agency in region.agencies:
        if agency.get_overage() > 0:
            overage += agency.get_overage()
            agency.cur_tgt = agency.capacity

    if overage > 0:
        region.overage = region.distrib_overage(overage)

# Step 3 - Redistribute among neighboring regions, from region with most need to region with least need
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
