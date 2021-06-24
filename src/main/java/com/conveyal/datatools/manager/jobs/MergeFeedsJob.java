package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.FeedSourceJob;
import com.conveyal.datatools.common.utils.aws.CheckedAWSException;
import com.conveyal.datatools.common.utils.aws.S3Utils;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.gtfsplus.tables.GtfsPlusTable;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.utils.ErrorUtils;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.loader.Table;
import com.conveyal.gtfs.model.StopTime;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.bson.codecs.pojo.annotations.BsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipOutputStream;

import static com.conveyal.datatools.manager.jobs.MergeFeedsType.SERVICE_PERIOD;
import static com.conveyal.datatools.manager.jobs.MergeFeedsType.REGIONAL;
import static com.conveyal.datatools.manager.jobs.MergeStrategy.CHECK_STOP_TIMES;
import static com.conveyal.datatools.manager.jobs.MergeStrategy.EXTEND_FUTURE;
import static com.conveyal.datatools.manager.utils.MergeFeedUtils.*;

/**
 * This job handles merging two or more feed versions according to logic specific to the specified merge type.
 * The merge types handled here are:
 * - {@link MergeFeedsType#REGIONAL}: this is essentially a "dumb" merge. For each feed version, each primary key is
 * scoped so that there is no possibility that it will conflict with other IDs
 * found in any other feed version. Note: There is absolutely no attempt to merge
 * entities based on either expected shared IDs or entity location (e.g., stop
 * coordinates).
 * - {@link MergeFeedsType#SERVICE_PERIOD}:      this strategy is defined in detail at https://github.com/conveyal/datatools-server/issues/185,
 * but in essence, this strategy attempts to merge an active and future feed into
 * a combined file. For certain entities (specifically stops and routes) it uses
 * alternate fields as primary keys (stop_code and route_short_name) if they are
 * available. There is some complexity related to this in {@link #constructMergedTable(Table, List, ZipOutputStream)}.
 * Another defining characteristic is to prefer entities defined in the "future"
 * file if there are matching entities in the active file.
 * Future merge strategies could be added here. For example, some potential customers have mentioned a desire to
 * prefer entities from the active version, so that entities edited in Data Tools would override the values found
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
 * 4. The merge process shall compare the active and future datasets, validate the following rules
 *    and generate the Merge Validation Report:
 *    i. Merging will be based on route_short_name in the active and future datasets. All matching
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
 *    vii. Merging fare_attributes will be based on fare_id in the active and future datasets. All
 *      matching fare_ids between the datasets shall be considered same fare. Any fare_id in active
 *      data not present in the future will be appended to the future fare_attributes file.
 *    viii. All fare rules from the future dataset will be included. Any identical fare rules from
 *      the active dataset will be discarded. Any fare rules unique to the active dataset will be
 *      appended to the future file.
 *    ix. All transfers.txt entries with unique stop pairs (from - to) from both the future and
 *      active datasets will be included in the merged file. Entries with duplicate stop pairs from
 *      the active dataset will be discarded.
 *    x. All GTFS+ files should be merged based on how the associated base GTFS file is merged. For
 *      example, directions for routes that are not in the future routes.txt file should be appended
 *      to the future directions.txt file in the merged feed.
 */
public class MergeFeedsJob extends FeedSourceJob {

    private static final Logger LOG = LoggerFactory.getLogger(MergeFeedsJob.class);
    public static final ObjectMapper mapper = new ObjectMapper();
    private final Set<FeedVersion> feedVersions;
    public final MergeFeedsResult mergeFeedsResult;
    private final String filename;
    public final String projectId;
    public final MergeFeedsType mergeType;
    private File mergedTempFile = null;
    /**
     * If {@link MergeFeedsJob} storeNewVersion variable is true, a new version will be created from the merged GTFS
     * dataset. Otherwise, this will be null throughout the life of the job.
     */
    final FeedVersion mergedVersion;
    @JsonIgnore @BsonIgnore
    public Set<String> tripIdsToModifyForActiveFeed = new HashSet<>();
    @JsonIgnore @BsonIgnore
    public Set<String> tripIdsToSkipForActiveFeed = new HashSet<>();
    @JsonIgnore @BsonIgnore
    public Set<String> serviceIdsToExtend = new HashSet<>();
    @JsonIgnore @BsonIgnore
    public Set<String> serviceIdsToCloneAndRename = new HashSet<>();
    private List<FeedToMerge> feedsToMerge;

    public MergeFeedsJob(Auth0UserProfile owner, Set<FeedVersion> feedVersions, String file, MergeFeedsType mergeType) {
        this(owner, feedVersions, file, mergeType, true);
    }

    /** Shorthand method to get the future feed during a service period merge */
    @BsonIgnore @JsonIgnore
    public FeedToMerge getFutureFeed() {
        return feedsToMerge.get(0);
    }

    /** Shorthand method to get the active feed during a service period merge */
    @BsonIgnore @JsonIgnore
    public FeedToMerge getActiveFeed() {
        return feedsToMerge.get(1);
    }

    /**
     * @param owner             user ID that initiated job
     * @param feedVersions      set of feed versions to merge
     * @param file              resulting merge filename (without .zip)
     * @param mergeType         the type of merge to perform {@link MergeFeedsType}
     * @param storeNewVersion   whether to store merged feed as new version
     */
    public MergeFeedsJob(Auth0UserProfile owner, Set<FeedVersion> feedVersions, String file,
                         MergeFeedsType mergeType, boolean storeNewVersion) {
        super(owner, mergeType.equals(REGIONAL) ? "Merging project feeds" : "Merging feed versions",
            JobType.MERGE_FEED_VERSIONS);
        this.feedVersions = feedVersions;
        // Construct full filename with extension
        this.filename = String.format("%s.zip", file);
        // If the merge type is regional, the file string should be equivalent to projectId, which
        // is used by the client to download the merged feed upon job completion.
        this.projectId = mergeType.equals(REGIONAL) ? file : null;
        this.mergeType = mergeType;
        // Assuming job is successful, mergedVersion will contain the resulting feed version.
        // Merged version will be null if the new version should not be stored.
        this.mergedVersion = getMergedVersion(this, storeNewVersion);
        this.mergeFeedsResult = new MergeFeedsResult(mergeType);
    }

    @BsonIgnore @JsonIgnore
    public Set<FeedVersion> getFeedVersions() {
        return this.feedVersions;
    }

    @BsonIgnore @JsonIgnore
    public List<FeedToMerge> getFeedsToMerge() {
        return this.feedsToMerge;
    }

    /**
     * The final stage handles clean up (deleting temp file) and adding the next job to process the
     * new merged version (assuming the merge did not fail).
     */
    @Override
    public void jobFinished() {
        // Delete temp file to ensure it does not cause storage bloat. Note: merged file has already been stored
        // permanently.
        try {
            Files.delete(mergedTempFile.toPath());
        } catch (IOException e) {
            LOG.error(
                "Merged feed file {} not deleted. This may contribute to storage space shortages.",
                mergedTempFile.getAbsolutePath(),
                e
            );
            ErrorUtils.reportToBugsnag(e, owner);
        }
    }

    /**
     * Primary job logic handles collecting and sorting versions, creating a merged table for all versions, and writing
     * the resulting zip file to storage.
     */
    @Override
    public void jobLogic() throws IOException, CheckedAWSException {
        // Create temp zip file to add merged feed content to.
        mergedTempFile = File.createTempFile(filename, null);
        mergedTempFile.deleteOnExit();
        // Create the zipfile with try with resources so that it is always closed.
        try (ZipOutputStream out = new ZipOutputStream(new FileOutputStream(mergedTempFile))) {
            LOG.info("Created merge file: {}", mergedTempFile.getAbsolutePath());
            feedsToMerge = collectAndSortFeeds(feedVersions);

            // Determine which tables to merge (only merge GTFS+ tables for MTC extension).
            final List<Table> tablesToMerge = getTablesToMerge();
            int numberOfTables = tablesToMerge.size();
            // Before initiating the merge process, get the merge strategy to use, which runs some pre-processing to
            // check for id conflicts for certain tables (e.g., trips and calendars).
            if (mergeType.equals(SERVICE_PERIOD)) {
                mergeFeedsResult.mergeStrategy = getMergeStrategy();
            }
            // Skip merging process altogether if the failing condition is met.
            if (MergeStrategy.FAIL_DUE_TO_MATCHING_TRIP_IDS.equals(mergeFeedsResult.mergeStrategy)) {
                failMergeJob("Feed merge failed because the trip_ids are identical in the future and active feeds. A new service requires unique trip_ids for merging.");
                return;
            }
            // Loop over GTFS tables and merge each feed one table at a time.
            for (int i = 0; i < numberOfTables; i++) {
                Table table = tablesToMerge.get(i);
                if (shouldSkipTable(table.name)) continue;
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
        }
        if (!mergeFeedsResult.failed) {
            // Store feed locally and (if applicable) upload regional feed to S3.
            storeMergedFeed();
            status.completeSuccessfully("Merged feed created successfully.");
        }
        LOG.info("Feed merge is complete.");
        if (shouldLoadAsNewFeed()) {
            mergedVersion.inputVersions = feedVersions.stream().map(FeedVersion::retrieveId).collect(Collectors.toSet());
            // Handle the processing of the new version when storing new version (note: s3 upload is handled within this job).
            // We must add this job in jobLogic (rather than jobFinished) because jobFinished is called after this job's
            // subJobs are run.
            addNextJob(new ProcessSingleFeedJob(mergedVersion, owner, true));
        }
    }

    private List<Table> getTablesToMerge() {
        List<Table> tablesToMerge = Arrays.stream(Table.tablesInOrder)
            .filter(Table::isSpecTable)
            .collect(Collectors.toList());
        if (DataManager.isExtensionEnabled("mtc")) {
            // Merge GTFS+ tables only if MTC extension is enabled. We should do this for both
            // regional and MTC merge strategies.
            tablesToMerge.addAll(Arrays.asList(GtfsPlusTable.tables));
        }
        return tablesToMerge;
    }

    /**
     * Check if the table should be skipped in the merged output.
     */
    private boolean shouldSkipTable(String tableName) {
        if (mergeType.equals(REGIONAL) && tableName.equals(Table.FEED_INFO.name)) {
            // It does not make sense to include the feed_info table when performing a
            // regional feed merge because this file is intended to contain data specific to
            // a single agency feed.
            // TODO: Perhaps future work can generate a special feed_info file for the merged
            //  file.
            LOG.warn("Skipping feed_info table for regional merge.");
            return true;
        }
        if (tableName.equals(Table.PATTERNS.name) || tableName.equals(Table.PATTERN_STOP.name)) {
            LOG.warn("Skipping editor-only table {}.", tableName);
            return true;
        }
        return false;
    }

    /**
     * Determines whether {@link ProcessSingleFeedJob} should be run as a follow on task at the completion of the merge.
     * The merge must have no errors and the mergedVersion must not be null (i.e., we need somewhere to put the new
     * version).
     */
    private boolean shouldLoadAsNewFeed() {
        return mergedVersion != null && !status.error && !mergeFeedsResult.failed;
    }

    /**
     * Handle updating {@link MergeFeedsResult} and the overall job status when a failure condition is triggered while
     * merging feeds.
     */
    public void failMergeJob(String failureMessage) {
        LOG.error(failureMessage);
        mergeFeedsResult.failed = true;
        mergeFeedsResult.errorCount++;
        mergeFeedsResult.failureReasons.add(failureMessage);
        // Use generic message for overall job status.
        status.fail("Merging feed versions failed.");
    }

    /**
     * Handles writing the GTFS zip file to disk. For REGIONAL merges, this will end up in a project subdirectory on s3.
     * Otherwise, it will write to a new version.
     */
    private void storeMergedFeed() throws IOException, CheckedAWSException {
        if (mergedVersion != null) {
            // Store the zip file for the merged feed version.
            try {
                mergedVersion.newGtfsFile(new FileInputStream(mergedTempFile));
            } catch (IOException e) {
                LOG.error("Could not store merged feed for new version", e);
                throw e;
            }
        }
        // Write the new latest regional merge file to s3://$BUCKET/project/$PROJECT_ID.zip
        if (mergeType.equals(REGIONAL)) {
            status.update("Saving merged feed.", 95);
            // Store the project merged zip locally or on s3
            if (DataManager.useS3) {
                String s3Key = String.join("/", "project", filename);
                S3Utils.getDefaultS3Client().putObject(S3Utils.DEFAULT_BUCKET, s3Key, mergedTempFile);
                LOG.info("Storing merged project feed at {}", S3Utils.getDefaultBucketUriForKey(s3Key));
            } else {
                try {
                    FeedVersion.feedStore.newFeed(filename, new FileInputStream(mergedTempFile), null);
                } catch (IOException e) {
                    LOG.error("Could not store feed for project " + filename, e);
                    throw e;
                }
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
        MergeLineContext ctx = new MergeLineContext(this, table, out);
        try {
            // Iterate over each zip file. For service period merge, the first feed is the future GTFS.
            for (int feedIndex = 0; feedIndex < feedsToMerge.size(); feedIndex++) {
                ctx.startNewFeed(feedIndex);
                mergeFeedsResult.feedCount++;
                if (ctx.skipFile) continue;
                LOG.info("Adding {} table for {}{}", table.name, ctx.feedSource.name, ctx.version.version);
                // Iterate over the rows of the table and write them to the merged output table. If an error was
                // encountered, return -1 to fail the merge job immediately.
                if (!ctx.iterateOverRows()) {
                    return -1;
                }
            }
            ctx.flushAndClose();
        } catch (Exception e) {
            List<String> versionNames = feedVersions.stream()
                .map(version -> version.parentFeedSource().name)
                .collect(Collectors.toList());
            LOG.error("Error merging feed sources: {}", versionNames, e);
            throw e;
        }
        // Track the number of lines in the merged table and return final number.
        mergeFeedsResult.linesPerTable.put(table.name, ctx.mergedLineNumber);
        return ctx.mergedLineNumber;
    }

    /**
     * Get the merge strategy to use for MTC service period merges by checking the active and future feeds for various
     * combinations of matching trip and service IDs.
     */
    private MergeStrategy getMergeStrategy() throws IOException {
        boolean shouldFailJob = false;
        FeedToMerge futureFeedToMerge = getFutureFeed();
        FeedToMerge activeFeedToMerge = getActiveFeed();
        futureFeedToMerge.collectTripAndServiceIds();
        activeFeedToMerge.collectTripAndServiceIds();
        Set<String> activeTripIds = activeFeedToMerge.idsForTable.get(Table.TRIPS);
        Set<String> futureTripIds = futureFeedToMerge.idsForTable.get(Table.TRIPS);
        // Determine whether service and trip IDs are exact matches.
        boolean serviceIdsMatch = activeFeedToMerge.serviceIds.equals(futureFeedToMerge.serviceIds);
        boolean tripIdsMatch = activeTripIds.equals(futureTripIds);
        if (tripIdsMatch) {
            // Effectively this exact match condition means that the future feed will be used as is
            // (including stops, routes, etc.), the only modification being service date ranges.
            // This is Condition 2 in the docs.
            // If only trip IDs match, do not permit merge to continue.
            return serviceIdsMatch ? EXTEND_FUTURE : MergeStrategy.FAIL_DUE_TO_MATCHING_TRIP_IDS;
        }
        if (serviceIdsMatch) {
            // If just the service_ids are an exact match, check the that the stop_times having matching signatures
            // between the two feeds (i.e., each stop time in the ordered list is identical between the two feeds).
            Feed futureFeed = new Feed(DataManager.GTFS_DATA_SOURCE, futureFeedToMerge.version.namespace);
            Feed activeFeed = new Feed(DataManager.GTFS_DATA_SOURCE, activeFeedToMerge.version.namespace);
            Set<String> sharedTripIds = Sets.intersection(activeTripIds, futureTripIds);
            for (String tripId : sharedTripIds) {
                if (compareStopTimes(tripId, futureFeed, activeFeed)) {
                    shouldFailJob = true;
                }
            }
            // If a trip only in the active feed references a service_id that is set to be extended, that
            // service_id needs to be cloned and renamed to differentiate it from the same service_id in
            // the future feed. (The trip in question will be linked to the cloned service_id.)
            Set<String> tripsOnlyInActiveFeed = Sets.difference(activeTripIds, futureTripIds);
            tripsOnlyInActiveFeed.stream()
                .map(tripId -> activeFeed.trips.get(tripId).service_id)
                .filter(serviceId -> serviceIdsToExtend.contains(serviceId))
                .forEach(serviceId -> serviceIdsToCloneAndRename.add(serviceId));
            // If a trip only in the future feed references a service_id that is set to be extended, that
            // service_id needs to be cloned and renamed to differentiate it from the same service_id in
            // the future feed. (The trip in question will be linked to the cloned service_id.)
            Set<String> tripsOnlyInFutureFeed = Sets.difference(futureTripIds, activeTripIds);
            tripsOnlyInFutureFeed.stream()
                .map(tripId -> futureFeed.trips.get(tripId).service_id)
                .filter(serviceId -> serviceIdsToExtend.contains(serviceId))
                .forEach(serviceId -> serviceIdsToCloneAndRename.add(serviceId));
            // If a failure was encountered above, use failure strategy. Otherwise, use check stop times to proceed with
            // feed merge.
            return shouldFailJob ? MergeStrategy.FAIL_DUE_TO_MATCHING_TRIP_IDS : CHECK_STOP_TIMES;
        }
        // If neither the trips or services are exact matches, use the default merge strategy.
        return MergeStrategy.DEFAULT;
    }

    /**
     * Compare stop times for the given tripId between the future and active feeds. The comparison will inform whether
     * trip and/or service IDs should be modified in the output merged feed.
     * @return true if an error was encountered during the check. false if no error was encountered.
     */
    private boolean compareStopTimes(String tripId, Feed futureFeed, Feed activeFeed) {
        // Fetch all ordered stop_times for each shared trip_id and compare the two sets for the
        // future and active feed. If the stop_times are an exact match, include one instance of the trip
        // (ignoring the other identical one). If they do not match, modify the active trip_id and include.
        List<StopTime> futureStopTimes = Lists.newArrayList(futureFeed.stopTimes.getOrdered(tripId));
        List<StopTime> activeStopTimes = Lists.newArrayList(activeFeed.stopTimes.getOrdered(tripId));
        String futureServiceId = futureFeed.trips.get(tripId).service_id;
        String activeServiceId = activeFeed.trips.get(tripId).service_id;
        if (!futureServiceId.equals(activeServiceId)) {
            // We cannot account for the case where service_ids do not match! It would be a bit too complicated
            // to handle this unique case, so instead just include in the failure reasons and use failure
            // strategy.
            failMergeJob(
                String.format("Shared trip_id (%s) had mismatched service id between two feeds (active: %s, future: %s)",
                    tripId,
                    activeServiceId,
                    futureServiceId
                )
            );
            return true;
        }
        if (!stopTimesMatch(futureStopTimes, activeStopTimes)) {
            // If stop_times or services do not match, the trip will be cloned. Also, track the service_id
            // (it will need to be cloned and renamed for both active feeds).
            tripIdsToModifyForActiveFeed.add(tripId);
            serviceIdsToCloneAndRename.add(futureServiceId);
        } else {
            // If the trip's stop_times are an exact match, we can safely include just the
            // future trip and exclude the active one. Also, track the service_id (it will need to be
            // extended to the full time range).
            tripIdsToSkipForActiveFeed.add(tripId);
            serviceIdsToExtend.add(futureServiceId);
        }
        return false;
    }

    public String getFeedSourceId() {
        return feedSource.id;
    }
}
