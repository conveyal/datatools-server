package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.gtfsplus.tables.GtfsPlusTable;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.loader.Field;
import com.conveyal.gtfs.loader.ReferenceTracker;
import com.conveyal.gtfs.loader.Table;
import com.csvreader.CsvReader;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import static com.conveyal.datatools.manager.jobs.MergeFeedsType.MTC;
import static com.conveyal.datatools.manager.jobs.MergeFeedsType.REGIONAL;
import static com.conveyal.datatools.manager.utils.StringUtils.getCleanName;
import static com.conveyal.gtfs.loader.DateField.GTFS_DATE_FORMATTER;
import static com.conveyal.gtfs.loader.Field.getFieldIndex;

/**
 * This job handles merging two or more feed versions according to logic specific to the specified merge type.
 * The current merge types handled here are:
 * - {@link MergeFeedsType#REGIONAL}: this is essentially a "dumb" merge. For each feed version, each primary key is
 * scoped so that there is no possibility that it will conflict with other IDs
 * found in any other feed version. Note: There is absolutely no attempt to merge
 * entities based on either expected shared IDs or entity location (e.g., stop
 * coordinates).
 * - {@link MergeFeedsType#MTC}:      this strategy is defined in detail at https://github.com/conveyal/datatools-server/issues/185,
 * but in essence, this strategy attempts to merge a current and future feed into
 * a combined file. For certain entities (specifically stops and routes) it uses
 * alternate fields as primary keys (stop_code and route_short_name) if they are
 * available. There is some complexity related to this in {@link #constructMergedTable(Table, List, ZipOutputStream)}.
 * Another defining characteristic is to prefer entities defined in the "future"
 * file if there are matching entities in the current file.
 * Future merge strategies could be added here. For example, some potential customers have mentioned a desire to
 * prefer entities from the "current" version, so that entities edited in Data Tools would override the values found
 * in the "future" file, which may have limited data attributes due to being exported from scheduling software with
 * limited GTFS support.
 *
 * Reproduced from https://github.com/conveyal/datatools-server/issues/185 on 2019/04/23:
 *
 * 1. When a new GTFS+ feed is loaded in TDM, check as part of the loading and validation process if
 *    the dataset is for a future date. (If all services start in the future, consider the dataset
 *    to be for the future).
 * 2. If it is a future dataset, automatically notify the user that the feed needs to be merged with
 *    most recent active version or a selected one in order to further process the feed.
 * 3. Use the chosen version to merge the future feed. The merging process needs to be efficient so
 *    that the user doesn’t need to wait more than a tolerable time.
 * 4. The merge process shall compare the current and future datasets, validate the following rules
 *    and generate the Merge Validation Report:
 *    i. Merging will be based on route_short_name in the current and future datasets. All matching
 *      route_short_names between the datasets shall be considered same route. Any route_short_name
 *      in active data not present in the future will be appended to the future routes file.
 *    ii. Future feed_info.txt file should get priority over active feed file when difference is
 *      identified.
 *    iii. When difference is found in agency.txt file between active and future feeds, the future
 *      agency.txt file data should be used. Possible issue with missing agency_id referenced by routes
 *    iv. When stop_code is included, stop merging will be based on that. If stop_code is not
 *      included, it will be based on stop_id. All stops in future data will be carried forward and
 *      any stops found in active data that are not in the future data shall be appended. If one
 *      of the feed is missing stop_code, merge fails with a notification to the user with
 *      suggestion that the feed with missing stop_code must be fixed with stop_code.
 *    v. If any service_id in the active feed matches with the future feed, it should be modified
 *      and all associated trip records must also be changed with the modified service_id.
 *      If a service_id from the active calendar has both the start_date and end_date in the
 *      future, the service shall not be appended to the merged file. Records in trips,
 *      calendar_dates, and calendar_attributes referencing this service_id shall also be
 *      removed/ignored. Stop_time records for the ignored trips shall also be removed.
 *      If a service_id from the active calendar has only the end_date in the future, the end_date
 *      shall be set to one day prior to the earliest start_date in future dataset before appending
 *      the calendar record to the merged file.
 *      trip_ids between active and future datasets must not match. If any trip_id is found to be
 *      matching, the merge should fail with appropriate notification to user with the cause of the
 *      failure. Notification should include all matched trip_ids.
 *    vi. New shape_ids in the future datasets should be appended in the merged feed.
 *    vii. Merging fare_attributes will be based on fare_id in the current and future datasets. All
 *      matching fare_ids between the datasets shall be considered same fare. Any fare_id in active
 *      data not present in the future will be appended to the future fare_attributes file.
 *    viii. All fare rules from the future dataset will be included. Any identical fare rules from
 *      the current dataset will be discarded. Any fare rules unique to the current dataset will be
 *      appended to the future file.
 *    ix. All transfers.txt entries with unique stop pairs (from - to) from both the future and
 *      current datasets will be included in the merged file. Entries with duplicate stop pairs from
 *      the current dataset will be discarded.
 *    x. All GTFS+ files should be merged based on how the associated base GTFS file is merged. For
 *      example, directions for routes that are not in the future routes.txt file should be appended
 *      to the future directions.txt file in the merged feed.
 */
public class MergeFeedsJob extends MonitorableJob {

    private static final Logger LOG = LoggerFactory.getLogger(MergeFeedsJob.class);
    public static final ObjectMapper mapper = new ObjectMapper();
    private final Set<FeedVersion> feedVersions;
    private final FeedSource feedSource;
    private final ReferenceTracker referenceTracker = new ReferenceTracker();
    public MergeFeedsResult mergeFeedsResult;
    private final String filename;
    public final String projectId;
    public final MergeFeedsType mergeType;
    private File mergedTempFile = null;
    final FeedVersion mergedVersion;
    public boolean failOnDuplicateTripId = true;

    /**
     * @param owner        user ID that initiated job
     * @param feedVersions set of feed versions to merge
     * @param file         resulting merge filename (without .zip)
     * @param mergeType    the type of merge to perform (@link MergeFeedsType)
     */
    public MergeFeedsJob(Auth0UserProfile owner, Set<FeedVersion> feedVersions, String file,
                         MergeFeedsType mergeType) {
        super(owner, mergeType.equals(REGIONAL) ? "Merging project feeds" : "Merging feed versions",
            JobType.MERGE_FEED_VERSIONS);
        this.feedVersions = feedVersions;
        // Grab parent feed source if performing non-regional merge (each version should share the
        // same feed source).
        this.feedSource =
            mergeType.equals(REGIONAL) ? null : feedVersions.iterator().next().parentFeedSource();
        // Construct full filename with extension
        this.filename = String.format("%s.zip", file);
        // If the merge type is regional, the file string should be equivalent to projectId, which
        // is used by the client to download the merged feed upon job completion.
        this.projectId = mergeType.equals(REGIONAL) ? file : null;
        this.mergeType = mergeType;
        // Assuming job is successful, mergedVersion will contain the resulting feed version.
        this.mergedVersion = mergeType.equals(REGIONAL) ? null : new FeedVersion(this.feedSource);
        this.mergeFeedsResult = new MergeFeedsResult(mergeType);
    }

    /**
     * The final stage handles clean up (deleting temp file) and adding the next job to process the
     * new merged version (assuming the merge did not fail).
     */
    public void jobFinished() {
        // Delete temp file to ensure it does not cause storage bloat. Note: merged file has already been stored
        // permanently.
        if (!mergedTempFile.delete()) {
            // FIXME: send to bugsnag?
            LOG.error(
                "Merged feed file {} not deleted. This may contribute to storage space shortages.",
                mergedTempFile.getAbsolutePath());
        }
    }

    /**
     * Primary job logic handles collecting and sorting versions, creating a merged table for all versions, and writing
     * the resulting zip file to storage.
     */
    @Override public void jobLogic() throws IOException {
        // Create temp zip file to add merged feed content to.
        mergedTempFile = File.createTempFile(filename, null);
        mergedTempFile.deleteOnExit();
        // Create the zipfile.
        ZipOutputStream out = new ZipOutputStream(new FileOutputStream(mergedTempFile));
        LOG.info("Created merge file: " + mergedTempFile.getAbsolutePath());
        List<FeedToMerge> feedsToMerge = collectAndSortFeeds(feedVersions);

        // Determine which tables to merge (only merge GTFS+ tables for MTC extension).
        final List<Table> tablesToMerge =
            Arrays.stream(Table.tablesInOrder)
                .filter(Table::isSpecTable)
                .collect(Collectors.toList());
        if (DataManager.isExtensionEnabled("mtc")) {
            // Merge GTFS+ tables only if MTC extension is enabled. We should do this for both
            // regional and MTC merge strategies.
            tablesToMerge.addAll(Arrays.asList(GtfsPlusTable.tables));
        }
        int numberOfTables = tablesToMerge.size();
        // Loop over GTFS tables and merge each feed one table at a time.
        for (int i = 0; i < numberOfTables; i++) {
            Table table = tablesToMerge.get(i);
            if (mergeType.equals(REGIONAL) && table.name.equals(Table.FEED_INFO.name)) {
                // It does not make sense to include the feed_info table when performing a
                // regional feed merge because this file is intended to contain data specific to
                // a single agency feed.
                // TODO: Perhaps future work can generate a special feed_info file for the merged
                //  file.
                LOG.warn("Skipping feed_info table for regional merge.");
                continue;
            }
            if (table.name.equals(Table.PATTERNS.name) || table.name.equals(Table.PATTERN_STOP.name)) {
                LOG.warn("Skipping editor-only table {}.", table.name);
                continue;
            }
            double percentComplete = Math.round((double) i / numberOfTables * 10000d) / 100d;
            status.update("Merging " + table.name, percentComplete);
            // Perform the merge.
            LOG.info("Writing {} to merged feed", table.name);
            int mergedLineNumber = constructMergedTable(table, feedsToMerge, out);
            if (mergedLineNumber == 0) {
                LOG.warn("Skipping {} table. No entries found in zip files.", table.name);
            } else if (mergedLineNumber == -1) {
                LOG.error("Merge {} table failed!", table.name);
            }
        }
        // Close output stream for zip file.
        out.close();
        // Handle writing file to storage (local or s3).
        if (mergeFeedsResult.failed) {
            status.fail("Merging feed versions failed.");
        } else {
            storeMergedFeed();
            status.update(false, "Merged feed created successfully.", 100, true);
        }
        LOG.info("Feed merge is complete.");
        if (!mergeType.equals(REGIONAL) && !status.error && !mergeFeedsResult.failed) {
            // Handle the processing of the new version for non-regional merges (note: s3 upload is handled within this job).
            // We must add this job in jobLogic (rather than jobFinished) because jobFinished is called after this job's
            // subJobs are run.
            ProcessSingleFeedJob processSingleFeedJob =
                new ProcessSingleFeedJob(mergedVersion, owner, true);
            addNextJob(processSingleFeedJob);
        }
    }

    /**
     * Collect zipFiles for each feed version before merging tables.
     * Note: feed versions are sorted by first calendar date so that future dataset is iterated over first. This is
     * required for the MTC merge strategy which prefers entities from the future dataset over past entities.
     */
    private List<FeedToMerge> collectAndSortFeeds(Set<FeedVersion> feedVersions) {
        return feedVersions.stream().map(version -> {
            try {
                return new FeedToMerge(version);
            } catch (Exception e) {
                LOG.error("Could not create zip file for version {}:", version.parentFeedSource(),
                    version.version);
                return null;
            }
        }).filter(Objects::nonNull).filter(entry -> entry.version.validationResult != null
            && entry.version.validationResult.firstCalendarDate != null)
            // MTC-specific sort mentioned in above comment.
            // TODO: If another merge strategy requires a different sort order, a merge type check should be added.
            .sorted(Comparator.comparing(entry -> entry.version.validationResult.firstCalendarDate,
                Comparator.reverseOrder())).collect(Collectors.toList());
    }

    /**
     * Handles writing the GTFS zip file to disk. For REGIONAL merges, this will end up in a project subdirectory on s3.
     * Otherwise, it will write to a new version.
     */
    private void storeMergedFeed() throws IOException {
        if (mergeType.equals(REGIONAL)) {
            status.update(false, "Saving merged feed.", 95);
            // Store the project merged zip locally or on s3
            if (DataManager.useS3) {
                String s3Key = String.join("/", "project", filename);
                FeedStore.s3Client.putObject(DataManager.feedBucket, s3Key, mergedTempFile);
                LOG.info("Storing merged project feed at s3://{}/{}", DataManager.feedBucket,
                    s3Key);
            } else {
                try {
                    FeedVersion.feedStore
                        .newFeed(filename, new FileInputStream(mergedTempFile), null);
                } catch (IOException e) {
                    e.printStackTrace();
                    LOG.error("Could not store feed for project {}", filename);
                    throw e;
                }
            }
        } else {
            // Store the zip file for the merged feed version.
            try {
                FeedVersion.feedStore
                    .newFeed(mergedVersion.id, new FileInputStream(mergedTempFile), feedSource);
            } catch (IOException e) {
                LOG.error("Could not store merged feed for new version");
                throw e;
            }
        }
    }

    /**
     * Merge the specified table for multiple GTFS feeds.
     *
     * @param table        table to merge
     * @param feedsToMerge map of feedSources to zipFiles from which to extract the .txt tables
     * @param out          output stream to write table into
     * @return number of lines in merged table
     */
    private int constructMergedTable(Table table, List<FeedToMerge> feedsToMerge,
        ZipOutputStream out) throws IOException {
        // CSV writer used to write to zip file.
        CsvListWriter writer = new CsvListWriter(new OutputStreamWriter(out), CsvPreference.STANDARD_PREFERENCE);
        String keyField = table.getKeyFieldName();
        String orderField = table.getOrderFieldName();
        if (mergeType.equals(MTC)) {
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
        // Set up objects for tracking the rows encountered
        Map<String, String[]> rowValuesForStopOrRouteId = new HashMap<>();
        Set<String> rowStrings = new HashSet<>();
        // Track shape_ids found in future feed in order to check for conflicts with active feed (MTC only).
        Set<String> shapeIdsInFutureFeed = new HashSet<>();
        int mergedLineNumber = 0;
        // Get the spec fields to export
        List<Field> specFields = table.specFields();
        boolean stopCodeMissingFromFirstTable = false;
        try {
            // Get shared fields between all feeds being merged. This is used to filter the spec fields so that only
            // fields found in the collection of feeds are included in the merged table.
            Set<Field> sharedFields = getSharedFields(feedsToMerge, table);
            // Initialize future feed's first date to the first calendar date from the validation result.
            // This is equivalent to either the earliest date of service defined for a calendar_date record or the
            // earliest start_date value for a calendars.txt record. For MTC, however, they require that GTFS
            // providers use calendars.txt entries and prefer that this value (which is used to determine cutoff
            // dates for the active feed when merging with the future) be strictly assigned the earliest
            // calendar#start_date (unless that table for some reason does not exist).
            LocalDate futureFeedFirstDate = feedsToMerge.get(0).version.validationResult.firstCalendarDate;
            LocalDate futureFirstCalendarStartDate = LocalDate.MAX;
            // Iterate over each zip file.
            for (int feedIndex = 0; feedIndex < feedsToMerge.size(); feedIndex++) {
                boolean keyFieldMissing = false;
                // Use for a new agency ID for use if the feed does not contain one. Initialize to
                // null. If the value becomes non-null, the agency_id is missing and needs to be
                // replaced with the generated value stored in this variable.
                String newAgencyId = null;
                mergeFeedsResult.feedCount++;
                FeedToMerge feed = feedsToMerge.get(feedIndex);
                FeedVersion version = feed.version;
                FeedSource feedSource = version.parentFeedSource();
                // Generate ID prefix to scope GTFS identifiers to avoid conflicts.
                String idScope = getCleanName(feedSource.name) + version.version;
                CsvReader csvReader = table.getCsvReader(feed.zipFile, null);
                // If csv reader is null, the table was not found in the zip file. There is no need
                // to handle merging this table for the current zip file.
                if (csvReader == null) {
                    LOG.warn("Table {} not found in the zip file for {}{}", table.name,
                        feedSource.name, version.version);
                    continue;
                }
                LOG.info("Adding {} table for {}{}", table.name, feedSource.name, version.version);

                Field[] fieldsFoundInZip =
                    table.getFieldsFromFieldHeaders(csvReader.getHeaders(), null);
                List<Field> fieldsFoundList = Arrays.asList(fieldsFoundInZip);
                // Determine the index of the key field for this version's table.
                int keyFieldIndex = getFieldIndex(fieldsFoundInZip, keyField);
                if (keyFieldIndex == -1) {
                    LOG.error("No {} field exists for {} table (feed={})", keyField, table.name,
                        feed.version.id);
                    keyFieldMissing = true;
                    // If there is no agency_id for agency table, create one and ensure that
                    // route#agency_id gets set.
                }
                int lineNumber = 0;
                // Iterate over rows in table, writing them to the out file.
                while (csvReader.readRecord()) {
                    String keyValue = csvReader.get(keyFieldIndex);
                    if (feedIndex > 0 && mergeType.equals(MTC)) {
                        // Always prefer the "future" file for the feed_info table, which means
                        // we can skip any iterations following the first one. If merging the agency
                        // table, we should only skip the following feeds if performing an MTC merge
                        // because that logic assumes the two feeds share the same agency (or
                        // agencies). NOTE: feed_info file is skipped by default (outside of this
                        // method) for a regional merge), which is why this block is exclusively
                        // for an MTC merge. Also, this statement may print multiple log
                        // statements, but it is deliberately nested in the csv while block in
                        // order to detect agency_id mismatches and fail the merge if found.
                        if (table.name.equals("feed_info")) {
                            LOG.warn("Skipping {} file for feed {}/{} (future file preferred)",
                                table.name, feedIndex, feedsToMerge.size());
                            continue;
                        } else if (table.name.equals("agency")) {
                            // The second feed's agency table must contain the same agency_id
                            // value as the first feed.
                            String agencyId = String.join(":", keyField, keyValue);
                            if (!"".equals(keyValue) && !referenceTracker.transitIds.contains(agencyId)) {
                                String otherAgencyId = referenceTracker.transitIds.stream()
                                    .filter(transitId -> transitId.startsWith("agency_id"))
                                    .findAny()
                                    .orElse(null);
                                String message = String.format(
                                    "MTC merge detected mismatching agency_id values between two "
                                        + "feeds (%s and %s). Failing merge operation.",
                                    agencyId,
                                    otherAgencyId
                                );
                                LOG.error(message);
                                mergeFeedsResult.failed = true;
                                mergeFeedsResult.failureReasons.add(message);
                                return -1;
                            }
                            LOG.warn("Skipping {} file for feed {}/{} (future file preferred)",
                                table.name, feedIndex, feedsToMerge.size());
                            continue;
                        } else if (table.name.equals("calendar_dates")) {
                            if (
                                futureFirstCalendarStartDate.isBefore(LocalDate.MAX) &&
                                futureFeedFirstDate.isBefore(futureFirstCalendarStartDate)
                            ) {
                                // If the future feed's first date is before the feed's first calendar start date,
                                // override the future feed first date with the calendar start date for use when checking
                                // MTC calendar_dates and calendar records for modification/exclusion.
                                futureFeedFirstDate = futureFirstCalendarStartDate;
                            }
                        }
                    }
                    // Check certain initial conditions on the first line of the file.
                    if (lineNumber == 0) {
                        if (table.name.equals(Table.AGENCY.name) && (keyFieldMissing || keyValue.equals(""))) {
                            // agency_id is optional if only one agency is present, but that will
                            // cause issues for the feed merge, so we need to insert an agency_id
                            // for the single entry.
                            newAgencyId = UUID.randomUUID().toString();
                            if (keyFieldMissing) {
                                // Only add agency_id field if it is missing in table.
                                List<Field> fieldsList = new ArrayList<>(Arrays.asList(fieldsFoundInZip));
                                fieldsList.add(Table.AGENCY.fields[0]);
                                fieldsFoundInZip = fieldsList.toArray(fieldsFoundInZip);
                                sharedFields.add(Table.AGENCY.fields[0]);
                            }
                            fieldsFoundList = Arrays.asList(fieldsFoundInZip);
                        }
                        if (mergeType.equals(MTC) && table.name.equals("stops")) {
                            // For the first line of the stops table, check that the alt. key
                            // field (stop_code) is present. If it is not, revert to the original
                            // key field. This is only pertinent for the MTC merge type.
                            // TODO: Use more sophisticated check for missing stop_codes than
                            //  simply the first line containing the value.
                            if (feedIndex == 0) {
                                // Check that the first file contains stop_code values.
                                if ("".equals(keyValue)) {
                                    LOG.warn(
                                        "stop_code is not present in file {}/{}. Reverting to stop_id",
                                        feedIndex, feedsToMerge.size());
                                    // If the key value for stop_code is not present, revert to stop_id.
                                    keyField = table.getKeyFieldName();
                                    keyFieldIndex = table.getKeyFieldIndex(fieldsFoundInZip);
                                    keyValue = csvReader.get(keyFieldIndex);
                                    stopCodeMissingFromFirstTable = true;
                                }
                            } else {
                                // Check whether stop_code exists for the subsequent files.
                                String firstStopCodeValue = csvReader.get(getFieldIndex(fieldsFoundInZip, "stop_code"));
                                if (stopCodeMissingFromFirstTable && !"".equals(firstStopCodeValue)) {
                                    // If stop_code was missing from the first file and exists for
                                    // the second, we consider that a failing error.
                                    mergeFeedsResult.failed = true;
                                    mergeFeedsResult.errorCount++;
                                    mergeFeedsResult.failureReasons.add(
                                        "If one stops.txt file contains stop_codes, both feed versions must stop_codes.");
                                }
                            }
                        }
                    }
                    // Filter the spec fields on the set of shared fields found in all feeds to be merged.
                    List<Field> sharedSpecFields = specFields.stream()
                        .filter(field -> containsField(sharedFields, field.name))
                        .collect(Collectors.toList());
                    Field[] sharedSpecFieldsArray = sharedSpecFields.toArray(new Field[0]);
                    boolean skipRecord = false;
                    String[] rowValues = new String[sharedSpecFields.size()];
                    String[] values = csvReader.getValues();
                    if (values.length == 1) {
                        LOG.warn("Found blank line. Skipping...");
                        continue;
                    }
                    // Piece together the row to write, which should look practically identical to the original
                    // row except for the identifiers receiving a prefix to avoid ID conflicts.
                    for (int specFieldIndex = 0; specFieldIndex < sharedSpecFields.size(); specFieldIndex++) {
                        // There is nothing to do in this loop if it has already been determined that the record should
                        // be skipped.
                        if (skipRecord) continue;
                        Field field = sharedSpecFields.get(specFieldIndex);
                        // Get index of field from GTFS spec as it appears in feed
                        int index = fieldsFoundList.indexOf(field);
                        String val = csvReader.get(index);
                        // Default value to write is unchanged from value found in csv.
                        String valueToWrite = val;
                        // Handle filling in agency_id if missing when merging regional feeds.
                        if (newAgencyId != null && field.name.equals("agency_id") && mergeType
                            .equals(REGIONAL)) {
                            if (val.equals("") && table.name.equals("agency") && lineNumber > 0) {
                                // If there is no agency_id value for a second (or greater) agency
                                // record, fail the merge feed job.
                                String message = String.format(
                                    "Feed %s has multiple agency records but no agency_id values.",
                                    feed.version.id);
                                mergeFeedsResult.failed = true;
                                mergeFeedsResult.failureReasons.add(message);
                                LOG.error(message);
                                return -1;
                            }
                            LOG.info("Updating {}#agency_id to (auto-generated) {} for ID {}",
                                table.name, newAgencyId, keyValue);
                            val = newAgencyId;
                        }
                        // Determine if field is a GTFS identifier.
                        boolean isKeyField =
                            field.isReference() || keyField.equals(field.name);
                        if (this.mergeType.equals(REGIONAL) && isKeyField && !val.isEmpty()) {
                            // For regional merge, if field is a GTFS identifier (e.g., route_id,
                            // stop_id, etc.), add scoped prefix.
                            valueToWrite = String.join(":", idScope, val);
                        }
                        // Only need to check for merge conflicts if using MTC merge type because
                        // the regional merge type scopes all identifiers by default. Also, the
                        // reference tracker will get far too large if we attempt to use it to
                        // track references for a large number of feeds (e.g., every feed in New
                        // York State).
                        if (mergeType.equals(MTC)) {
                            Set<NewGTFSError> idErrors;
                            // If analyzing the second feed (non-future feed), the service_id always gets feed scoped.
                            // See https://github.com/ibi-group/datatools-server/issues/244
                            if (feedIndex == 1 && field.name.equals("service_id")) {
                                valueToWrite = String.join(":", idScope, val);
                                mergeFeedsResult.remappedIds.put(
                                    getTableScopedValue(table, idScope, val),
                                    valueToWrite
                                );
                                idErrors = referenceTracker
                                    .checkReferencesAndUniqueness(keyValue, lineNumber, field, valueToWrite,
                                        table, keyField, orderField);
                            } else {
                                idErrors = referenceTracker
                                    .checkReferencesAndUniqueness(keyValue, lineNumber, field, val,
                                        table, keyField, orderField);
                            }

                            // Store values for key fields that have been encountered.
                            // TODO Consider using Strategy Pattern https://en.wikipedia.org/wiki/Strategy_pattern
                            //  instead of a switch statement.
                            switch (table.name) {
                                case "calendar":
                                    // If any service_id in the active feed matches with the future
                                    // feed, it should be modified and all associated trip records
                                    // must also be changed with the modified service_id.
                                    // TODO How can we check that calendar_dates entries are
                                    //  duplicates? I think we would need to consider the
                                    //  service_id:exception_type:date as the unique key and include any
                                    //  all entries as long as they are unique on this key.
                                    if (hasDuplicateError(idErrors)) {
                                        String key = getTableScopedValue(table, idScope, val);
                                        // Modify service_id and ensure that referencing trips
                                        // have service_id updated.
                                        valueToWrite = String.join(":", idScope, val);
                                        mergeFeedsResult.remappedIds.put(key, valueToWrite);
                                    }
                                    int startDateIndex =
                                        getFieldIndex(fieldsFoundInZip, "start_date");
                                    LocalDate startDate = LocalDate
                                        .parse(csvReader.get(startDateIndex),
                                            GTFS_DATE_FORMATTER);
                                    if (feedIndex == 0) {
                                        // For the future feed, check if the calendar's start date is earlier than the
                                        // previous earliest value and update if so.
                                        if (futureFirstCalendarStartDate.isAfter(startDate)) {
                                            futureFirstCalendarStartDate = startDate;
                                        }
                                    } else {
                                        // If a service_id from the active calendar has both the
                                        // start_date and end_date in the future, the service will be
                                        // excluded from the merged file. Records in trips,
                                        // calendar_dates, and calendar_attributes referencing this
                                        // service_id shall also be removed/ignored. Stop_time records
                                        // for the ignored trips shall also be removed.
                                        if (!startDate.isBefore(futureFeedFirstDate)) {
                                            LOG.warn(
                                                "Skipping calendar entry {} because it operates in the time span of future feed.",
                                                keyValue);
                                            String key =
                                                getTableScopedValue(table, idScope, keyValue);
                                            mergeFeedsResult.skippedIds.add(key);
                                            skipRecord = true;
                                            continue;
                                        }
                                        // If a service_id from the active calendar has only the
                                        // end_date in the future, the end_date shall be set to one
                                        // day prior to the earliest start_date in future dataset
                                        // before appending the calendar record to the merged file.
                                        int endDateIndex =
                                            getFieldIndex(fieldsFoundInZip, "end_date");
                                        if (index == endDateIndex) {
                                            LocalDate endDate = LocalDate
                                                .parse(csvReader.get(endDateIndex),
                                                    GTFS_DATE_FORMATTER);
                                            if (!endDate.isBefore(futureFeedFirstDate)) {
                                                val = futureFeedFirstDate
                                                    .minus(1, ChronoUnit.DAYS)
                                                    .format(GTFS_DATE_FORMATTER);
                                            }
                                        }
                                    }
                                    // Track service ID because we want to avoid removing trips that may reference this
                                    // service_id when the service_id is used by calendar_dates that operate in the valid
                                    // date range, i.e., before the future feed's first date.
                                    if (field.name.equals("service_id")) mergeFeedsResult.serviceIds.add(valueToWrite);
                                    break;
                                case "calendar_dates":
                                    // Drop any calendar_dates.txt records from the existing feed for dates that are
                                    // not before the first date of the future feed.
                                    int dateIndex = getFieldIndex(fieldsFoundInZip, "date");
                                    LocalDate date = LocalDate.parse(csvReader.get(dateIndex), GTFS_DATE_FORMATTER);
                                    if (feedIndex > 0) {
                                        if (!date.isBefore(futureFeedFirstDate)) {
                                            LOG.warn(
                                                "Skipping calendar_dates entry {} because it operates in the time span of future feed (i.e., after or on {}).",
                                                keyValue,
                                                futureFeedFirstDate);
                                            String key = getTableScopedValue(table, idScope, keyValue);
                                            mergeFeedsResult.skippedIds.add(key);
                                            skipRecord = true;
                                            continue;
                                        }
                                    }
                                    // Track service ID because we want to avoid removing trips that may reference this
                                    // service_id when the service_id is used by calendar.txt records that operate in
                                    // the valid date range, i.e., before the future feed's first date.
                                    if (field.name.equals("service_id")) mergeFeedsResult.serviceIds.add(keyValue);
                                    break;
                                case "shapes":
                                    // If a shape_id is found in both future and active datasets, all shape points from
                                    // the active dataset must be feed-scoped. Otherwise, the merged dataset may contain
                                    // shape_id:shape_pt_sequence values from both datasets (e.g., if future dataset contains
                                    // sequences 1,2,3,10 and active contains 1,2,7,9,10; the merged set will contain
                                    // 1,2,3,7,9,10).
                                    if (field.name.equals("shape_id")) {
                                        if (feedIndex == 0) {
                                            // Track shape_id if working on future feed.
                                            shapeIdsInFutureFeed.add(val);
                                        } else if (shapeIdsInFutureFeed.contains(val)) {
                                            // For the active feed, if the shape_id was already processed from the
                                            // future feed, we need to add the feed-scope to avoid weird, hybrid shapes
                                            // with points from both feeds.
                                            valueToWrite = String.join(":", idScope, val);
                                            // Update key value for subsequent ID conflict checks for this row.
                                            keyValue = valueToWrite;
                                            mergeFeedsResult.remappedIds.put(
                                                getTableScopedValue(table, idScope, val),
                                                valueToWrite
                                            );
                                            // Re-check refs and uniqueness after changing shape_id value. (Note: this
                                            // probably won't have any impact, but there's not much harm in including it.)
                                            idErrors = referenceTracker
                                                .checkReferencesAndUniqueness(keyValue, lineNumber, field, valueToWrite,
                                                    table, keyField, orderField);
                                        }
                                    }
                                    // Skip record if normal duplicate errors are found.
                                    if (hasDuplicateError(idErrors)) skipRecord = true;
                                    break;
                                case "trips":
                                    // trip_ids between active and future datasets must not match. If any trip_id is found
                                    // to be matching, the merge should fail with appropriate notification to user with the
                                    // cause of the failure. Merge result should include all conflicting trip_ids.
                                    for (NewGTFSError error : idErrors) {
                                        if (error.errorType.equals(NewGTFSErrorType.DUPLICATE_ID)) {
                                            mergeFeedsResult.failureReasons
                                                .add("Trip ID conflict caused merge failure.");
                                            mergeFeedsResult.idConflicts.add(error.badValue);
                                            mergeFeedsResult.errorCount++;
                                            if (failOnDuplicateTripId)
                                                mergeFeedsResult.failed = true;
                                            skipRecord = true;
                                        }
                                    }
                                    break;
                                case "stops":
                                    // When stop_code is included, stop merging will be based on that. If stop_code is not
                                    // included, it will be based on stop_id. All stops in future data will be carried
                                    // forward and any stops found in active data that are not in the future data shall be
                                    // appended. If one of the feed is missing stop_code, merge fails with a notification to
                                    // the user with suggestion that the feed with missing stop_code must be fixed with
                                    // stop_code.
                                    // NOTE: route case is also used by the stops case, so the route
                                    // case must follow this block.
                                case "routes":
                                    boolean useAltKey =
                                        keyField.equals("stop_code") || keyField.equals("route_short_name");
                                    // First, check uniqueness of primary key value (i.e., stop or route ID)
                                    // in case the stop_code or route_short_name are being used. This
                                    // must occur unconditionally because each record must be tracked
                                    // by the reference tracker.
                                    String primaryKeyValue =
                                        csvReader.get(table.getKeyFieldIndex(fieldsFoundInZip));
                                    Set<NewGTFSError> primaryKeyErrors = referenceTracker
                                        .checkReferencesAndUniqueness(primaryKeyValue, lineNumber,
                                            field, val, table);
                                    // Merging will be based on route_short_name/stop_code in the current and future datasets. All
                                    // matching route_short_names/stop_codes between the datasets shall be considered same route/stop. Any
                                    // route_short_name/stop_code in active data not present in the future will be appended to the
                                    // future routes/stops file.
                                    if (useAltKey) {
                                        if ("".equals(keyValue) && field.name.equals(table.getKeyFieldName())) {
                                            // If alt key is empty (which is permitted), skip
                                            // checking of alt key dupe errors/re-mapping values and
                                            // simply use the primary key (route_id/stop_id).
                                            if (hasDuplicateError(primaryKeyErrors)) {
                                                skipRecord = true;
                                            }
                                        } else if (hasDuplicateError(idErrors)) {
                                            // If we encounter a route/stop that shares its alt.
                                            // ID with a previous route/stop, we need to
                                            // remap its route_id/stop_id field so that
                                            // references point to the previous
                                            // route_id/stop_id. For example,
                                            // route_short_name in both feeds is "ABC" but
                                            // each route has a different route_id (123 and
                                            // 456). This block will map references to 456 to
                                            // 123 so that ABC/123 is the route of record.
                                            ////////////////////////////////////////////////////////
                                            // Get current route/stop ID. (Note: primary
                                            // ID index is always zero because we're
                                            // iterating over the spec fields).
                                            String currentPrimaryKey = rowValues[0];
                                            // Get unique key to check for remapped ID when
                                            // writing values to file.
                                            String key =
                                                getTableScopedValue(table, idScope, currentPrimaryKey);
                                            // Extract the route/stop ID value used for the
                                            // route/stop with already encountered matching
                                            // short name/stop code.
                                            String[] strings = rowValuesForStopOrRouteId.get(
                                                String.join(":", keyField, val)
                                            );
                                            String keyForMatchingAltId = strings[0];
                                            if (!keyForMatchingAltId.equals(currentPrimaryKey)) {
                                                // Remap this row's route_id/stop_id to ensure
                                                // that referencing entities (trips, stop_times)
                                                // have their references updated.
                                                mergeFeedsResult.remappedIds.put(key, keyForMatchingAltId);
                                            }
                                            skipRecord = true;
                                        }
                                        // Next check for regular ID conflicts (e.g., on route_id or stop_id) because any
                                        // conflicts here will actually break the feed. This essentially handles the case
                                        // where two routes have different short_names, but share the same route_id. We want
                                        // both of these routes to end up in the merged feed in this case because we're
                                        // matching on short name, so we must modify the route_id.
                                        if (!skipRecord && !referenceTracker.transitIds
                                            .contains(String.join(":", keyField, keyValue))) {
                                            if (hasDuplicateError(primaryKeyErrors)) {
                                                String key = getTableScopedValue(table, idScope, val);
                                                // Modify route_id and ensure that referencing trips
                                                // have route_id updated.
                                                valueToWrite = String.join(":", idScope, val);
                                                mergeFeedsResult.remappedIds.put(key, valueToWrite);
                                            }
                                        }
                                    } else {
                                        // Key field has defaulted to the standard primary key field
                                        // (stop_id or route_id), which makes the check much
                                        // simpler (just skip the duplicate record).
                                        if (hasDuplicateError(idErrors)) skipRecord = true;
                                    }

                                    if (newAgencyId != null && field.name.equals("agency_id")) {
                                        LOG.info(
                                            "Updating route#agency_id to (auto-generated) {} for route={}",
                                            newAgencyId, keyValue);
                                        val = newAgencyId;
                                    }
                                    break;
                                default:
                                    // For any other table, skip any duplicate record.
                                    if (hasDuplicateError(idErrors)) skipRecord = true;
                                    break;
                            }
                        }

                        if (field.isReference()) {
                            String key = getTableScopedValue(field.referenceTable, idScope, val);
                            // If the current foreign ref points to another record that has been skipped, skip this
                            // record and add its primary key to the list of skipped IDs (so that other references can
                            // be properly omitted).
                            if (mergeFeedsResult.skippedIds.contains(key)) {
                                // If a calendar#service_id has been skipped, but there were valid service_ids found in
                                // calendar_dates, do not skip that record for both the calendar_date and any related
                                // trips.
                                if (field.name.equals("service_id") && mergeFeedsResult.serviceIds.contains(val)) {
                                    LOG.warn("Not skipping valid service_id {} for {} {}", val, table.name, keyValue);
                                } else {
                                    String skippedKey = getTableScopedValue(table, idScope, keyValue);
                                    if (orderField != null) {
                                        skippedKey = String.join(":", skippedKey,
                                            csvReader.get(getFieldIndex(fieldsFoundInZip, orderField)));
                                    }
                                    mergeFeedsResult.skippedIds.add(skippedKey);
                                    skipRecord = true;
                                    continue;
                                }
                            }
                            // If the field is a foreign reference, check to see whether the reference has been
                            // remapped due to a conflicting ID from another feed (e.g., calendar#service_id).
                            if (mergeFeedsResult.remappedIds.containsKey(key)) {
                                mergeFeedsResult.remappedReferences++;
                                // If the value has been remapped update the value to write.
                                valueToWrite = mergeFeedsResult.remappedIds.get(key);
                            }
                        }
                        rowValues[specFieldIndex] = valueToWrite;
                    } // End of iteration over each field for a row.
                    // Do not write rows that are designated to be skipped.
                    if (skipRecord && this.mergeType.equals(MTC)) {
                        mergeFeedsResult.recordsSkipCount++;
                        continue;
                    }
                    String newLine = String.join(",", rowValues);
                    switch (table.name) {
                        // Store row values for route or stop ID (or alternative ID field) in order
                        // to check for ID conflicts. NOTE: This is only intended to be used for
                        // routes and stops. Otherwise, this might (will) consume too much memory.
                        case "stops":
                        case "routes":
                            // FIXME: This should be revised for tables with order fields, but it should work fine for its
                            //  primary purposes: to detect exact copy rows and to temporarily hold the data in case a reference
                            //  needs to be looked up in order to remap an entity to that key.
                            // Here we need to get the key field index according to the spec
                            // table definition. Otherwise, if we use the keyFieldIndex variable
                            // defined above, we will be using the found fields index, which will
                            // cause major issues when trying to put and get values into the
                            // below map.
                            int index = getFieldIndex(sharedSpecFieldsArray, keyField);
                            String key = String.join(":", keyField, rowValues[index]);
                            rowValuesForStopOrRouteId.put(key, rowValues);
                            break;
                        case "transfers":
                        case "fare_rules":
                        case "directions": // GTFS+ table
                            if (!rowStrings.add(newLine)) {
                                // The line already exists in the output file, do not append it again. This prevents duplicate
                                // entries for certain files that do not contain primary keys (e.g., fare_rules and transfers) and
                                // do not otherwise have convenient ways to track uniqueness (like an order field).
                                // FIXME: add ordinal field/compound keys for transfers (from/to_stop_id) and fare_rules (?).
                                //  Perhaps it makes sense to include all unique fare rules rows, but transfers that share the
                                //  same from/to stop IDs but different transfer times or other values should not both be
                                //  included in the merged feed (yet this strategy would fail to filter those out).
                                mergeFeedsResult.recordsSkipCount++;
                                continue;
                            }
                            break;
                        default:
                            // Do nothing.
                            break;

                    }
                    // Finally, handle writing lines to zip entry.
                    if (mergedLineNumber == 0) {
                        // Create entry for zip file.
                        ZipEntry tableEntry = new ZipEntry(table.name + ".txt");
                        out.putNextEntry(tableEntry);
                        // Write headers to table.
                        String[] headers = sharedSpecFields.stream()
                            .map(field -> field.name)
                            .toArray(String[]::new);
                        writer.write(headers);
                    }
                    // Write line to table (plus new line char).
                    writer.write(rowValues);
                    lineNumber++;
                    mergedLineNumber++;
                } // End of iteration over each row.
            }
            writer.flush();
            out.closeEntry();
        } catch (Exception e) {
            LOG.error("Error merging feed sources: {}",
                feedVersions.stream().map(version -> version.parentFeedSource().name)
                    .collect(Collectors.toList()).toString());
            e.printStackTrace();
            throw e;
        }
        // Track the number of lines in the merged table and return final number.
        mergeFeedsResult.linesPerTable.put(table.name, mergedLineNumber);
        return mergedLineNumber;
    }

    /** Get the set of shared fields for all feeds being merged for a specific table. */
    private Set<Field> getSharedFields(List<FeedToMerge> feedsToMerge, Table table) throws IOException {
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
    private boolean containsField(Collection<Field> fields, String fieldName) {
        for (Field field : fields) if (field.name.equals(fieldName)) return true;
        return false;
    }

    /** Checks that any of a set of errors is of the type {@link NewGTFSErrorType#DUPLICATE_ID}. */
    private boolean hasDuplicateError(Set<NewGTFSError> errors) {
        for (NewGTFSError error : errors) {
            if (error.errorType.equals(NewGTFSErrorType.DUPLICATE_ID)) return true;
        }
        return false;
    }

    /** Get table-scoped value used for key when remapping references for a particular feed. */
    private static String getTableScopedValue(Table table, String prefix, String id) {
        return String.join(":",
            table.name,
            prefix,
            id);
    }

    /**
     * Helper class that collects the feed version and its zip file. Note: this class helps with sorting versions to
     * merge in a list collection.
     */
    private class FeedToMerge {
        public FeedVersion version;
        public ZipFile zipFile;

        FeedToMerge(FeedVersion version) throws IOException {
            this.version = version;
            this.zipFile = new ZipFile(version.retrieveGtfsFile());
        }
    }
}
