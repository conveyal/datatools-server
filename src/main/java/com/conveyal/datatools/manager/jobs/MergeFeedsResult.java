package com.conveyal.datatools.manager.jobs;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Contains the result of {@link MergeFeedsJob}.
 */
public class MergeFeedsResult implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Number of feeds merged */
    public int feedCount;
    /** Type of merge operation performed */
    public MergeFeedsType type;
    public MergeStrategy mergeStrategy = MergeStrategy.DEFAULT;
    /** Contains a set of strings for which there were error-causing duplicate values */
    public Set<String> idConflicts = new HashSet<>();
    /** Contains the set of IDs for records that were excluded in the merged feed */
    public Set<String> skippedIds = new HashSet<>();
    /**
     * Track the set of service IDs to end up in the merged feed in order to determine which calendar_dates and trips
     * records should be retained in the merged result.
     */
    public Set<String> serviceIds  = new HashSet<>();
    /** Contains the set of IDs that had their values remapped during the merge */
    public Map<String, String> remappedIds = new HashMap<>();
    /** Mapping of table name to line count in merged file */
    public Map<String, Integer> linesPerTable = new HashMap<>();
    public int remappedReferences;
    public int recordsSkipCount;
    public Date startTime;
    public boolean failed;
    public int errorCount;
    /** Set of reasons explaining why merge operation failed */
    public Set<String> failureReasons = new HashSet<>();
    public Set<String> tripIdsToCheck = new HashSet<>();

    public MergeFeedsResult (MergeFeedsType type) {
        this.type = type;
        this.startTime = new Date();
    }
}
