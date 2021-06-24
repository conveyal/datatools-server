package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.gtfs.loader.Table;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.ZipFile;

import static com.conveyal.datatools.manager.utils.MergeFeedUtils.getIdsForTable;

/**
 * Helper class that collects the feed version and its zip file. Note: this class helps with sorting versions to
 * merge in a list collection.
 */
public class FeedToMerge {
    public FeedVersion version;
    public ZipFile zipFile;
    public SetMultimap<Table, String> idsForTable = HashMultimap.create();
    public Set<String> serviceIds = new HashSet<>();
    private static final Set<Table> tablesToCheck = Sets.newHashSet(Table.TRIPS, Table.CALENDAR, Table.CALENDAR_DATES);

    public FeedToMerge(FeedVersion version) throws IOException {
        this.version = version;
        this.zipFile = new ZipFile(version.retrieveGtfsFile());
    }

    /** Collects all trip/service IDs (tables noted in {@link #tablesToCheck}) for comparing feeds during merge. */
    public void collectTripAndServiceIds() throws IOException {
        for (Table table : tablesToCheck) {
            idsForTable.get(table).addAll(getIdsForTable(zipFile, table));
        }
        serviceIds.addAll(idsForTable.get(Table.CALENDAR));
        serviceIds.addAll(idsForTable.get(Table.CALENDAR_DATES));
    }
}