package com.conveyal.datatools.manager.utils;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.jobs.FeedToMerge;
import com.conveyal.datatools.manager.jobs.MergeFeedsJob;
import com.conveyal.datatools.manager.jobs.MergeFeedsType;
import com.conveyal.datatools.manager.jobs.MergeLineContext;
import com.conveyal.datatools.manager.jobs.MergeStrategy;
import com.conveyal.datatools.manager.models.FeedRetrievalMethod;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.loader.Field;
import com.conveyal.gtfs.loader.Table;
import com.conveyal.gtfs.model.StopTime;
import com.csvreader.CsvReader;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.conveyal.datatools.manager.jobs.MergeFeedsType.REGIONAL;
import static com.conveyal.datatools.manager.jobs.MergeFeedsType.SERVICE_PERIOD;
import static com.conveyal.datatools.manager.jobs.MergeStrategy.CHECK_STOP_TIMES;
import static com.conveyal.datatools.manager.jobs.MergeStrategy.EXTEND_FUTURE;
import static com.conveyal.datatools.manager.models.FeedRetrievalMethod.REGIONAL_MERGE;
import static com.conveyal.datatools.manager.models.FeedRetrievalMethod.SERVICE_PERIOD_MERGE;
import static com.conveyal.gtfs.loader.DateField.GTFS_DATE_FORMATTER;
import static com.conveyal.gtfs.loader.Field.getFieldIndex;

public class MergeFeedUtils {
    private static final Logger LOG = LoggerFactory.getLogger(MergeFeedUtils.class);

    /**
     * Get the ids (e.g., trip_id, service_id) for the provided table from the zipfile.
     */
    public static Set<String> getIdsForTable(ZipFile zipFile, Table table) throws IOException {
        Set<String> ids = new HashSet<>();
        String keyField = table.getKeyFieldName();
        CsvReader csvReader = table.getCsvReader(zipFile, null);
        if (csvReader == null) {
            LOG.warn("Table {} not found in zip file: {}", table.name, zipFile.getName());
            return ids;
        }
        Field[] fieldsFoundInZip = table.getFieldsFromFieldHeaders(csvReader.getHeaders(), null);
        // Get the key field (id value) for each row.
        int keyFieldIndex = getFieldIndex(fieldsFoundInZip, keyField);
        while (csvReader.readRecord()) ids.add(csvReader.get(keyFieldIndex));
        csvReader.close();
        return ids;
    }

    /**
     * Construct stop_code failure message for {@link com.conveyal.datatools.manager.jobs.MergeFeedsJob} in the case of
     * incomplete stop_code values for all records.
     */
    public static String stopCodeFailureMessage(int stopsMissingStopCodeCount, int stopsCount, int specialStopsCount) {
        return String.format(
            "If stop_code is provided for some stops (for those with location_type = " +
                "empty or 0), all stops must have stop_code values. The merge process " +
                "found %d of %d total stops that were incorrectly missing stop_code values. " +
                "Note: \"special\" stops with location_type > 0 need not specify this value " +
                "(%d special stops found in feed).",
            stopsMissingStopCodeCount,
            stopsCount,
            specialStopsCount
        );
    }

    /**
     * Collect zipFiles for each feed version before merging tables.
     * Note: feed versions are sorted by first calendar date so that future dataset is iterated over first. This is
     * required for the MTC merge strategy which prefers entities from the future dataset over active feed entities.
     */
    public static List<FeedToMerge> collectAndSortFeeds(Set<FeedVersion> feedVersions) {
        return feedVersions.stream()
            .map(version -> {
                try {
                    return new FeedToMerge(version);
                } catch (Exception e) {
                    LOG.error("Could not create zip file for version: {}", version.version);
                    return null;
                }
            })
            // Filter out any feeds that do not have zip files (see above try/catch) and feeds that were never fully
            // validated (which suggests that they would break things during validation).
            .filter(Objects::nonNull)
            .filter(
                entry -> entry.version.validationResult != null
                    && entry.version.validationResult.firstCalendarDate != null
            )
            // MTC-specific sort mentioned in above comment.
            // TODO: If another merge strategy requires a different sort order, a merge type check should be added.
            .sorted(
                Comparator.comparing(
                    entry -> entry.version.validationResult.firstCalendarDate,
                    Comparator.reverseOrder())
            ).collect(Collectors.toList());
    }

    /** Get all fields found in the feeds being merged for a specific table. */
    public static Set<Field> getAllFields(List<FeedToMerge> feedsToMerge, Table table) throws IOException {
        Set<Field> sharedFields = new HashSet<>();
        // First, iterate over each feed to collect the shared fields that need to be output in the merged table.
        for (FeedToMerge feed : feedsToMerge) {
            CsvReader csvReader = table.getCsvReader(feed.zipFile, null);
            // If csv reader is null, the table was not found in the zip file.
            if (csvReader == null) {
                continue;
            }
            // Get fields found from headers and add them to the shared fields set.
            Field[] fieldsFoundInZip = table.getFieldsFromFieldHeaders(csvReader.getHeaders(), null);
            sharedFields.addAll(Arrays.asList(fieldsFoundInZip));
        }
        return sharedFields;
    }

    /**
     * Checks whether a collection of fields contains a field with the provided name.
     */
    public static boolean containsField(Collection<Field> fields, String fieldName) {
        for (Field field : fields) if (field.name.equals(fieldName)) return true;
        return false;
    }

    /** Checks that any of a set of errors is of the type {@link NewGTFSErrorType#DUPLICATE_ID}. */
    public static boolean hasDuplicateError(Set<NewGTFSError> errors) {
        for (NewGTFSError error : errors) {
            if (error.errorType.equals(NewGTFSErrorType.DUPLICATE_ID)) return true;
        }
        return false;
    }

    /** Get table-scoped value used for key when remapping references for a particular feed. */
    public static String getTableScopedValue(Table table, String prefix, String id) {
        return String.join(":",
            table.name,
            prefix,
            id);
    }

    /**
     * Checks whether the future and active stop_times for a particular trip_id are an exact match.
     */
    public static boolean stopTimesMatch(List<StopTime> futureStopTimes, List<StopTime> activeStopTimes) {
        if (futureStopTimes.size() != activeStopTimes.size()) {
            return false;
        }
        for (int i = 0; i < activeStopTimes.size(); i++) {
            if (!activeStopTimes.get(i).equals(futureStopTimes.get(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Get parent feed source depending on merge type. Assign regional feed source or simply the first parent feed
     * source found in the feed version list (these should all belong to the same feed source if the merge is not
     * regional).
     */
    public static FeedSource getParentFeedSourceForMerge(MergeFeedsJob job, boolean storeNewVersion) {
        FeedSource regionalFeedSource = null;
        Project project = Persistence.projects.getById(job.projectId);
        // If storing a regional merge as a new version, find the feed source designated by the project.
        if (job.mergeType.equals(REGIONAL) && storeNewVersion) {
            regionalFeedSource = Persistence.feedSources.getById(project.regionalFeedSourceId);
            // Create new feed source if this is the first regional merge.
            if (regionalFeedSource == null) {
                regionalFeedSource = new FeedSource("REGIONAL MERGE", project.id, REGIONAL_MERGE);
                // Store new feed source.
                Persistence.feedSources.create(regionalFeedSource);
                // Update regional feed source ID on project.
                project.regionalFeedSourceId = regionalFeedSource.id;
                Persistence.projects.replace(project.id, project);
            }
        }
        return job.mergeType.equals(REGIONAL)
            ? regionalFeedSource
            : job.getFeedVersions().iterator().next().parentFeedSource();
    }

    public static FeedVersion getMergedVersion(MergeFeedsJob job, boolean storeNewVersion) {
        FeedSource feedSource = getParentFeedSourceForMerge(job, storeNewVersion);
        FeedRetrievalMethod retrievalMethod = job.mergeType.equals(REGIONAL)
            ? REGIONAL_MERGE
            : SERVICE_PERIOD_MERGE;
        return storeNewVersion ? new FeedVersion(feedSource, retrievalMethod) : null;
    }

    public static String getMergeKeyField(Table table, MergeFeedsType mergeType) {
        String keyField = table.getKeyFieldName();
        if (mergeType.equals(SERVICE_PERIOD) && DataManager.isExtensionEnabled("mtc")) {
            // MTC requires that the stop and route records be merged based on different key fields.
            switch (table.name) {
                case "stops":
                    keyField = "stop_code";
                    break;
                case "routes":
                    keyField = "route_short_name";
                    break;
                default:
                    // Otherwise, use the standard key field (see keyField declaration.
                    break;
            }
        }
        return keyField;
    }
}
