package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.loader.Field;
import com.conveyal.gtfs.loader.ReferenceTracker;
import com.conveyal.gtfs.loader.Table;
import com.csvreader.CsvReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static com.conveyal.datatools.manager.jobs.MergeFeedsType.REGIONAL;
import static com.conveyal.datatools.manager.jobs.MergeFeedsType.SERVICE_PERIOD;
import static com.conveyal.datatools.manager.jobs.MergeStrategy.CHECK_STOP_TIMES;
import static com.conveyal.datatools.manager.jobs.MergeStrategy.EXTEND_FUTURE;
import static com.conveyal.datatools.manager.utils.MergeFeedUtils.containsField;
import static com.conveyal.datatools.manager.utils.MergeFeedUtils.getAllFields;
import static com.conveyal.datatools.manager.utils.MergeFeedUtils.getMergeKeyField;
import static com.conveyal.datatools.manager.utils.MergeFeedUtils.getTableScopedValue;
import static com.conveyal.datatools.manager.utils.MergeFeedUtils.hasDuplicateError;
import static com.conveyal.datatools.manager.utils.MergeFeedUtils.stopCodeFailureMessage;
import static com.conveyal.datatools.manager.utils.StringUtils.getCleanName;
import static com.conveyal.gtfs.loader.DateField.GTFS_DATE_FORMATTER;
import static com.conveyal.gtfs.loader.Field.getFieldIndex;

public class MergeLineContext {
    private static final String AGENCY_ID = "agency_id";
    private static final String SERVICE_ID = "service_id";
    private static final String STOPS = "stops";
    private static final Logger LOG = LoggerFactory.getLogger(MergeLineContext.class);
    private final MergeFeedsJob job;
    private final ZipOutputStream out;
    private final Set<Field> allFields;
    private LocalDate futureFirstCalendarStartDate;
    private final LocalDate activeFeedFirstDate;
    private LocalDate futureFeedFirstDate;
    private boolean handlingActiveFeed;
    private boolean handlingFutureFeed;
    private String idScope;
    // CSV writer used to write to zip file.
    private final CsvListWriter writer;
    private CsvReader csvReader;
    private boolean skipRecord;
    private String newAgencyId;
    private boolean keyFieldMissing;
    private String[] rowValues;
    private int lineNumber = 0;
    private final Table table;
    private FeedToMerge feed;
    private String keyValue;
    private final ReferenceTracker referenceTracker = new ReferenceTracker();
    private String keyField;
    private String orderField;
    private final MergeFeedsResult mergeFeedsResult;
    private final List<FeedToMerge> feedsToMerge;
    private int keyFieldIndex;
    private Field[] fieldsFoundInZip;
    private List<Field> fieldsFoundList;
    private Field field;
    // Set up objects for tracking the rows encountered
    private final Map<String, String[]> rowValuesForStopOrRouteId = new HashMap<>();
    private final Set<String> rowStrings = new HashSet<>();
    private boolean stopCodeMissingFromFutureFeed = false;
    // Track shape_ids found in future feed in order to check for conflicts with active feed (MTC only).
    private final Set<String> shapeIdsInFutureFeed = new HashSet<>();
    private List<Field> sharedSpecFields;
    private int index;
    private String val;
    private String valueToWrite;
    private int feedIndex;

    public FeedVersion version;
    public FeedSource feedSource;
    public boolean skipFile;
    public int mergedLineNumber = 0;

    public MergeLineContext(MergeFeedsJob job, Table table, ZipOutputStream out) throws IOException {
        this.job = job;
        this.table = table;
        this.feedsToMerge = job.getFeedsToMerge();
        // Get shared fields between all feeds being merged. This is used to filter the spec fields so that only
        // fields found in the collection of feeds are included in the merged table.
        allFields = getAllFields(feedsToMerge, table);
        this.mergeFeedsResult = job.mergeFeedsResult;
        this.writer = new CsvListWriter(new OutputStreamWriter(out), CsvPreference.STANDARD_PREFERENCE);
        this.out = out;
        // Initialize future and active feed's first date to the first calendar date from validation result.
        // This is equivalent to either the earliest date of service defined for a calendar_date record or the
        // earliest start_date value for a calendars.txt record. For MTC, however, they require that GTFS
        // providers use calendars.txt entries and prefer that this value (which is used to determine cutoff
        // dates for the active feed when merging with the future) be strictly assigned the earliest
        // calendar#start_date (unless that table for some reason does not exist).
        futureFeedFirstDate = job.getFutureFeed().version.validationResult.firstCalendarDate;
        activeFeedFirstDate = job.getActiveFeed().version.validationResult.firstCalendarDate;
        futureFirstCalendarStartDate = LocalDate.MAX;
    }

    public void startNewFeed(int feedIndex) throws IOException {
        lineNumber = 0;
        handlingActiveFeed = feedIndex > 0;
        handlingFutureFeed = feedIndex == 0;
        this.feedIndex = feedIndex;
        this.feed = feedsToMerge.get(feedIndex);
        this.version = feed.version;
        this.feedSource = version.parentFeedSource();
        keyField = getMergeKeyField(table, job.mergeType);
        orderField = table.getOrderFieldName();
        keyFieldMissing = false;
        // Use for a new agency ID for use if the feed does not contain one. Initialize to
        // null. If the value becomes non-null, the agency_id is missing and needs to be
        // replaced with the generated value stored in this variable.
        newAgencyId = null;
        // Generate ID prefix to scope GTFS identifiers to avoid conflicts.
        idScope = getCleanName(feedSource.name) + version.version;
        csvReader = table.getCsvReader(feed.zipFile, null);
        // If csv reader is null, the table was not found in the zip file. There is no need
        // to handle merging this table for this zip file.
        // No need to iterate over second (active) file if strategy is to simply extend the future GTFS
        // service to start earlier.
        skipFile = shouldSkipFile();
        if (csvReader == null) {
            skipFile = true;
            LOG.warn("Table {} not found in the zip file for {}{}", table.name, feedSource.name, version.version);
            return;
        }
        fieldsFoundInZip = table.getFieldsFromFieldHeaders(csvReader.getHeaders(), null);
        fieldsFoundList = Arrays.asList(fieldsFoundInZip);
        // Determine the index of the key field for this version's table.
        keyFieldIndex = getFieldIndex(fieldsFoundInZip, keyField);
        if (keyFieldIndex == -1) {
            LOG.error("No {} field exists for {} table (feed={})", keyField, table.name, version.id);
            keyFieldMissing = true;
            // If there is no agency_id for agency table, create one and ensure that
            // route#agency_id gets set.
        }
    }

    public boolean shouldSkipFile() {
        if (handlingActiveFeed && job.mergeType.equals(SERVICE_PERIOD)) {
            // Always prefer the "future" file for the feed_info table, which means
            // we can skip any iterations following the first one.
            return EXTEND_FUTURE.equals(mergeFeedsResult.mergeStrategy) || table.name.equals("feed_info");
        }
        return false;
    }

    /**
     * Iterate over all rows in table and write them to the output zip.
     * @return false, if a failing condition was encountered. true, if everything was ok.
     */
    public boolean iterateOverRows() throws IOException {
        // Iterate over rows in table, writing them to the out file.
        while (csvReader.readRecord()) {
            startNewRow();
            if (checkMismatchedAgency()) {
                // If there is a mismatched agency, return immediately.
                return false;
            }
            updateFutureFeedFirstDate();
            // If checkMismatchedAgency flagged skipFile, loop back to the while loop. (Note: this is
            // intentional because we want to check all of the agency ids in the file).
            if (skipFile || lineIsBlank()) continue;
            // Check certain initial conditions on the first line of the file.
            checkFirstLineConditions();
            initializeRowValues();
            // Construct row values. If a failure condition was encountered, return.
            if (!constructRowValues()) {
                return false;
            }
            finishRowAndWriteToZip();
        }
        return true;
    }

    public void startNewRow() throws IOException {
        keyValue = csvReader.get(keyFieldIndex);
        // Get the spec fields to export
        List<Field> specFields = table.specFields();
        // Filter the spec fields on the set of fields found in all feeds to be merged.
        sharedSpecFields = specFields.stream()
            .filter(f -> containsField(allFields, f.name))
            .collect(Collectors.toList());
    }

    public boolean areForeignRefsOk() throws IOException {
        if (field.isForeignReference()) {
            String key = getTableScopedValue(field.referenceTable, idScope, val);
            // Check if we're performing a service period merge, this ref field is a service_id, and it
            // is not found in the list of service_ids (e.g., it was removed).
            boolean isValidServiceId = mergeFeedsResult.serviceIds.contains(valueToWrite);

            // If the current foreign ref points to another record that has
            // been skipped or is a ref to a non-existent service_id during a service period merge, skip
            // this record and add its primary key to the list of skipped IDs (so that other references
            // can be properly omitted).
            if (serviceIdHasOrShouldBeSkipped(key, isValidServiceId)) {
                // If a calendar#service_id has been skipped (it's listed in skippedIds), but there were
                // valid service_ids found in calendar_dates, do not skip that record for both the
                // calendar_date and any related trips.
                if (field.name.equals(SERVICE_ID) && isValidServiceId) {
                    LOG.warn("Not skipping valid service_id {} for {} {}", valueToWrite, table.name, keyValue);
                } else {
                    String skippedKey = getTableScopedValue(table, idScope, keyValue);
                    if (orderField != null) {
                        skippedKey = String.join(":", skippedKey,
                            csvReader.get(getFieldIndex(fieldsFoundInZip, orderField)));
                    }
                    mergeFeedsResult.skippedIds.add(skippedKey);
                    skipRecord = true;
                    return false;
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
        return true;
    }

    private boolean serviceIdHasOrShouldBeSkipped(String key, boolean isValidServiceId) {
        boolean serviceIdShouldBeSkipped = job.mergeType.equals(SERVICE_PERIOD) &&
            field.name.equals(SERVICE_ID) &&
            !isValidServiceId;
        return mergeFeedsResult.skippedIds.contains(key) || serviceIdShouldBeSkipped;
    }

    public boolean checkFieldForMergeConflicts() throws IOException {
        Set<NewGTFSError> idErrors = getIdErrors();
        // Store values for key fields that have been encountered and update any key values that need modification due
        // to conflicts.
        switch (table.name) {
            case "calendar":
                if (checkCalendarIds(idErrors)) return true;
                break;
            case "calendar_dates":
                if (checkCalendarDatesIds()) return true;
                break;
            case "shapes":
                checkShapeIds(idErrors);
                break;
            case "trips":
                checkTripIds(idErrors);
                break;
            // When stop_code is included, stop merging will be based on that. If stop_code is not
            // included, it will be based on stop_id. All stops in future data will be carried
            // forward and any stops found in active data that are not in the future data shall be
            // appended. If one of the feed is missing stop_code, merge fails with a notification to
            // the user with suggestion that the feed with missing stop_code must be fixed with
            // stop_code.
            // NOTE: route case is also used by the stops case, so the route
            // case must follow this block.
            case STOPS:
            case "routes":
                checkRoutesAndStopsIds(idErrors);
                break;
            default:
                // For any other table, skip any duplicate record.
                if (hasDuplicateError(idErrors)) skipRecord = true;
                break;
        }
        return false;
    }

    private Set<NewGTFSError> getIdErrors() {
        Set<NewGTFSError> idErrors;
        // If analyzing the second feed (active feed), the service_id always gets feed scoped.
        // See https://github.com/ibi-group/datatools-server/issues/244
        if (handlingActiveFeed && field.name.equals(SERVICE_ID)) {
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
        return idErrors;
    }

    private void checkTripIds(Set<NewGTFSError> idErrors) {
        // trip_ids between active and future datasets must not match. The tripIdsToSkip and
        // tripIdsToModify sets below are determined based on the MergeStrategy used for MTC
        // service period merges.
        if (handlingActiveFeed) {
            // Handling active feed. Skip or modify trip id if found in one of the
            // respective sets.
            if (job.tripIdsToSkipForActiveFeed.contains(keyValue)) {
                skipRecord = true;
            } else if (job.tripIdsToModifyForActiveFeed.contains(keyValue)) {
                valueToWrite = String.join(":", idScope, val);
                // Update key value for subsequent ID conflict checks for this row.
                keyValue = valueToWrite;
                mergeFeedsResult.remappedIds.put(
                    getTableScopedValue(table, idScope, val),
                    valueToWrite
                );
            }
        }
        for (NewGTFSError error : idErrors) {
            if (error.errorType.equals(NewGTFSErrorType.DUPLICATE_ID)) {
                valueToWrite = String.join(":", idScope, val);
                // Update key value for subsequent ID conflict checks for this row.
                keyValue = valueToWrite;
                mergeFeedsResult.remappedIds.put(
                    getTableScopedValue(table, idScope, val),
                    valueToWrite
                );
            }
        }
    }

    private void checkShapeIds(Set<NewGTFSError> idErrors) {
        // If a shape_id is found in both future and active datasets, all shape points from
        // the active dataset must be feed-scoped. Otherwise, the merged dataset may contain
        // shape_id:shape_pt_sequence values from both datasets (e.g., if future dataset contains
        // sequences 1,2,3,10 and active contains 1,2,7,9,10; the merged set will contain
        // 1,2,3,7,9,10).
        if (field.name.equals("shape_id")) {
            if (handlingFutureFeed) {
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
    }

    private boolean checkCalendarDatesIds() throws IOException {
        // Drop any calendar_dates.txt records from the existing feed for dates that are
        // not before the first date of the future feed.
        int dateIndex = getFieldIndex(fieldsFoundInZip, "date");
        LocalDate date = LocalDate.parse(csvReader.get(dateIndex), GTFS_DATE_FORMATTER);
        if (handlingActiveFeed && !date.isBefore(futureFeedFirstDate)) {
                LOG.warn(
                    "Skipping calendar_dates entry {} because it operates in the time span of future feed (i.e., after or on {}).",
                    keyValue,
                    futureFeedFirstDate);
                String key = getTableScopedValue(table, idScope, keyValue);
                mergeFeedsResult.skippedIds.add(key);
                skipRecord = true;
                return true;
        }
        // Track service ID because we want to avoid removing trips that may reference this
        // service_id when the service_id is used by calendar.txt records that operate in
        // the valid date range, i.e., before the future feed's first date.
        if (field.name.equals(SERVICE_ID)) mergeFeedsResult.serviceIds.add(valueToWrite);
        return false;
    }

    private boolean checkCalendarIds(Set<NewGTFSError> idErrors) throws IOException {
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
        int startDateIndex = getFieldIndex(fieldsFoundInZip, "start_date");
        LocalDate startDate = LocalDate.parse(csvReader.get(startDateIndex), GTFS_DATE_FORMATTER);
        if (handlingFutureFeed) {
            // For the future feed, check if the calendar's start date is earlier than the
            // previous earliest value and update if so.
            if (futureFirstCalendarStartDate.isAfter(startDate)) {
                futureFirstCalendarStartDate = startDate;
            }
            // FIXME: Move this below so that a cloned service doesn't get prematurely
            //  modified? (do we want the cloned record to have the original values?)
            if (shouldUpdateFutureFeedStartDate(startDateIndex)) {
                // Update start_date to extend service through the active feed's
                // start date if the merge strategy dictates. The justification for this logic is that the active feed's
                // service_id will be modified to a different unique value and the trips shared between the future/active
                // service are exactly matching.
                val = valueToWrite = activeFeedFirstDate.format(GTFS_DATE_FORMATTER);
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
                    "Skipping calendar entry {} because it operates fully within the time span of future feed.",
                    keyValue);
                String key = getTableScopedValue(table, idScope, keyValue);
                mergeFeedsResult.skippedIds.add(key);
                skipRecord = true;
                return true;
            }
            // If a service_id from the active calendar has only the
            // end_date in the future, the end_date shall be set to one
            // day prior to the earliest start_date in future dataset
            // before appending the calendar record to the merged file.
            int endDateIndex = getFieldIndex(fieldsFoundInZip, "end_date");
            if (index == endDateIndex) {
                LocalDate endDate = LocalDate
                    .parse(csvReader.get(endDateIndex), GTFS_DATE_FORMATTER);
                if (!endDate.isBefore(futureFeedFirstDate)) {
                    val = valueToWrite = futureFeedFirstDate
                        .minus(1, ChronoUnit.DAYS)
                        .format(GTFS_DATE_FORMATTER);
                }
            }
        }
        // Track service ID because we want to avoid removing trips that may reference this
        // service_id when the service_id is used by calendar_dates that operate in the valid
        // date range, i.e., before the future feed's first date.
        if (field.name.equals(SERVICE_ID)) mergeFeedsResult.serviceIds.add(valueToWrite);
        return false;
    }

    private boolean shouldUpdateFutureFeedStartDate(int startDateIndex) {
        return index == startDateIndex &&
            (
                EXTEND_FUTURE == mergeFeedsResult.mergeStrategy ||
                (
                    CHECK_STOP_TIMES == mergeFeedsResult.mergeStrategy &&
                        job.serviceIdsToExtend.contains(keyValue)
                )
            );
    }

    private void checkRoutesAndStopsIds(Set<NewGTFSError> idErrors) throws IOException {
        // First, check uniqueness of primary key value (i.e., stop or route ID)
        // in case the stop_code or route_short_name are being used. This
        // must occur unconditionally because each record must be tracked
        // by the reference tracker.
        String primaryKeyValue = csvReader.get(table.getKeyFieldIndex(fieldsFoundInZip));
        Set<NewGTFSError> primaryKeyErrors = referenceTracker
            .checkReferencesAndUniqueness(primaryKeyValue, lineNumber, field, val, table);
        // Merging will be based on route_short_name/stop_code in the active and future datasets. All
        // matching route_short_names/stop_codes between the datasets shall be considered same route/stop. Any
        // route_short_name/stop_code in active data not present in the future will be appended to the
        // future routes/stops file.
        if (useAltKey()) {
            if (hasPrimaryKeyErrors(primaryKeyErrors)) {
                // If alt key is empty (which is permitted), skip
                // checking of alt key dupe errors/re-mapping values and
                // simply use the primary key (route_id/stop_id).
                skipRecord = true;
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
                String key = getTableScopedValue(table, idScope, currentPrimaryKey);
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
            if (!skipRecord && !referenceTracker.transitIds.contains(String.join(":", keyField, keyValue)) && hasDuplicateError(primaryKeyErrors)) {
                String key = getTableScopedValue(table, idScope, val);
                // Modify route_id and ensure that referencing trips
                // have route_id updated.
                valueToWrite = String.join(":", idScope, val);
                mergeFeedsResult.remappedIds.put(key, valueToWrite);
            }
        } else {
            // Key field has defaulted to the standard primary key field
            // (stop_id or route_id), which makes the check much
            // simpler (just skip the duplicate record).
            if (hasDuplicateError(idErrors)) skipRecord = true;
        }

        if (newAgencyId != null && field.name.equals(AGENCY_ID)) {
            LOG.info(
                "Updating route#agency_id to (auto-generated) {} for route={}",
                newAgencyId, keyValue);
            val = newAgencyId;
        }
    }

    private boolean hasPrimaryKeyErrors(Set<NewGTFSError> primaryKeyErrors) {
        return "".equals(keyValue) && field.name.equals(table.getKeyFieldName()) && hasDuplicateError(primaryKeyErrors);
    }

    private boolean useAltKey() {
        return keyField.equals("stop_code") || keyField.equals("route_short_name");
    }

    public void checkMissingAgencyId() {
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
                allFields.add(Table.AGENCY.fields[0]);
            }
            fieldsFoundList = Arrays.asList(fieldsFoundInZip);
        }
    }

    public void checkStopCodeStuff() throws IOException {
        if (shouldCheckStopCodes()) {
            // Before reading any lines in stops.txt, first determine whether all records contain
            // properly filled stop_codes. The rules governing this logic are as follows:
            // 1. Stops with location_type greater than 0 (i.e., anything but 0 or empty) are permitted
            //    to have empty stop_codes (even if there are other stops in the feed that have
            //    stop_code values). This is because these location_types represent special entries
            //    that are either stations, entrances/exits, or generic nodes (e.g., for
            //    pathways.txt).
            // 2. For regular stops (location_type = 0 or empty), all or none of the stops must
            //    contain stop_codes. Otherwise, the merge feeds job will be failed.
            int stopsMissingStopCodeCount = 0;
            int stopsCount = 0;
            int specialStopsCount = 0;
            int locationTypeIndex = getFieldIndex(fieldsFoundInZip, "location_type");
            int stopCodeIndex = getFieldIndex(fieldsFoundInZip, "stop_code");
            // Get special stops reader to iterate over every stop and determine if stop_code values
            // are present.
            CsvReader stopsReader = table.getCsvReader(feed.zipFile, null);
            while (stopsReader.readRecord()) {
                stopsCount++;
                // Special stop records (i.e., a station, entrance, or anything with
                // location_type > 0) do not need to specify stop_code. Other stops should.
                String stopCode = stopsReader.get(stopCodeIndex);
                boolean stopCodeIsMissing = "".equals(stopCode);
                String locationType = stopsReader.get(locationTypeIndex);
                if (isSpecialStop(locationType)) specialStopsCount++;
                else if (stopCodeIsMissing) stopsMissingStopCodeCount++;
            }
            LOG.info("total stops: {}", stopsCount);
            LOG.info("stops missing stop_code: {}", stopsMissingStopCodeCount);
            if (stopsMissingStopCodeCount == stopsCount) {
                // If all stops are missing stop_code, we simply default to merging on stop_id.
                LOG.warn(
                    "stop_code is not present in file {}/{}. Reverting to stop_id",
                    feedIndex + 1, feedsToMerge.size());
                // If the key value for stop_code is not present, revert to stop_id.
                keyField = table.getKeyFieldName();
                keyFieldIndex = table.getKeyFieldIndex(fieldsFoundInZip);
                keyValue = csvReader.get(keyFieldIndex);
                // When all stops missing stop_code for the first feed, there's nothing to do (i.e.,
                // no failure condition has been triggered yet). Just indicate this in the flag and
                // proceed with the merge.
                if (handlingFutureFeed) stopCodeMissingFromFutureFeed = true;
                // However... if the second feed was missing stop_codes and the first feed was not,
                // fail the merge job.
                else if (!stopCodeMissingFromFutureFeed) {
                    job.failMergeJob(
                        stopCodeFailureMessage(stopsMissingStopCodeCount, stopsCount, specialStopsCount)
                    );
                }
            } else if (stopsMissingStopCodeCount > 0) {
                // If some, but not all, stops are missing stop_code, the merge feeds job must fail.
                job.failMergeJob(
                    stopCodeFailureMessage(stopsMissingStopCodeCount, stopsCount, specialStopsCount)
                );
            }
        }
    }

    private boolean shouldCheckStopCodes() {
        return job.mergeType.equals(SERVICE_PERIOD) && table.name.equals(STOPS);
    }

    /** Determine if stop is "special" via its locationType. I.e., a station, entrance, (location_type > 0). */
    private boolean isSpecialStop(String locationType) {
        return !"".equals(locationType) && !"0".equals(locationType);
    }

    public boolean updateAgencyIdIfNeeded() {
        if (newAgencyId != null && field.name.equals(AGENCY_ID) && job.mergeType.equals(REGIONAL)) {
            if (val.equals("") && table.name.equals("agency") && lineNumber > 0) {
                // If there is no agency_id value for a second (or greater) agency
                // record, return null which will trigger a failed merge feed job.
                job.failMergeJob(String.format(
                    "Feed %s has multiple agency records but no agency_id values.",
                    feed.version.id
                ));
                return false;
            }
            LOG.info("Updating {}#agency_id to (auto-generated) {} for ID {}", table.name, newAgencyId, keyValue);
            val = newAgencyId;
        }
        return true;
    }

    public void startNewField(int specFieldIndex) throws IOException {
        this.field = sharedSpecFields.get(specFieldIndex);
        // Get index of field from GTFS spec as it appears in feed
        index = fieldsFoundList.indexOf(field);
        val = csvReader.get(index);
        // Default value to write is unchanged from value found in csv (i.e. val). Note: if looking to
        // modify the value that is written in the merged file, you must update valueToWrite (e.g.,
        // updating this feed's end_date or accounting for cases where IDs conflict).
        valueToWrite = val;
    }

    public boolean storeRowAndStopValues() {
        String newLine = String.join(",", rowValues);
        switch (table.name) {
            // Store row values for route or stop ID (or alternative ID field) in order
            // to check for ID conflicts. NOTE: This is only intended to be used for
            // routes and stops. Otherwise, this might (will) consume too much memory.
            case STOPS:
            case "routes":
                // FIXME: This should be revised for tables with order fields, but it should work fine for its
                //  primary purposes: to detect exact copy rows and to temporarily hold the data in case a reference
                //  needs to be looked up in order to remap an entity to that key.
                // Here we need to get the key field index according to the spec
                // table definition. Otherwise, if we use the keyFieldIndex variable
                // defined above, we will be using the found fields index, which will
                // cause major issues when trying to put and get values into the
                // below map.
                int fieldIndex = getFieldIndex(sharedSpecFields.toArray(new Field[0]), keyField);
                String key = String.join(":", keyField, rowValues[fieldIndex]);
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
                    return true;
                }
                break;
            default:
                // Do nothing.
                break;
        }
        return false;
    }

    public void checkFirstLineConditions() throws IOException {
        if (lineNumber == 0) {
            checkMissingAgencyId();
            checkStopCodeStuff();
        }
    }

    /**
     * Check for some conditions that could occur when handling a service period merge.
     * @return true if the merge encountered failing conditions
     */
    public boolean checkMismatchedAgency() {
        if (handlingActiveFeed && job.mergeType.equals(SERVICE_PERIOD) && table.name.equals("agency")) {
            // If merging the agency table, we should only skip the following feeds if performing an MTC merge
            // because that logic assumes the two feeds share the same agency (or
            // agencies). NOTE: feed_info file is skipped by default (outside of this
            // method) for a regional merge), which is why this block is exclusively
            // for an MTC merge. Note, this statement may print multiple log
            // statements, but it is deliberately nested in the csv while block in
            // order to detect agency_id mismatches and fail the merge if found.
            // The second feed's agency table must contain the same agency_id
            // value as the first feed.
            String agencyId = String.join(":", keyField, keyValue);
            if (!"".equals(keyValue) && !referenceTracker.transitIds.contains(agencyId)) {
                String otherAgencyId = referenceTracker.transitIds.stream()
                    .filter(transitId -> transitId.startsWith(AGENCY_ID))
                    .findAny()
                    .orElse(null);
                job.failMergeJob(String.format(
                    "MTC merge detected mismatching agency_id values between two "
                        + "feeds (%s and %s). Failing merge operation.",
                    agencyId,
                    otherAgencyId
                ));
                return true;
            }
            LOG.warn("Skipping {} file for feed {}/{} (future file preferred)", table.name, feedIndex, feedsToMerge.size());
            skipFile = true;
        }
        return false;
    }

    public void scopeValueIfNeeded() {
        boolean isKeyField = field.isForeignReference() || keyField.equals(field.name);
        if (job.mergeType.equals(REGIONAL) && isKeyField && !val.isEmpty()) {
            // For regional merge, if field is a GTFS identifier (e.g., route_id,
            // stop_id, etc.), add scoped prefix.
            valueToWrite = String.join(":", idScope, val);
        }
    }

    public void initializeRowValues() {
        // Re-initialize skipRecord to false for next row.
        skipRecord = false;
        // Reset the row values (this must happen after the first line is checked).
        rowValues = new String[sharedSpecFields.size()];
    }

    public void addClonedServiceId() throws IOException {
        if ((table.name.equals("calendar")) && job.serviceIdsToCloneAndRename.contains(rowValues[keyFieldIndex])) {
            // FIXME: Do we need to worry about calendar_dates?
            String[] clonedValues = rowValues.clone();
            String newServiceId = clonedValues[keyFieldIndex] = String.join(":", idScope, rowValues[keyFieldIndex]);
            // Modify start/end date.
            int startDateIndex = Table.CALENDAR.getFieldIndex("start_date");
            int endDateIndex = Table.CALENDAR.getFieldIndex("end_date");
            clonedValues[startDateIndex] = feed.version.validationResult.firstCalendarDate.format(GTFS_DATE_FORMATTER);
            clonedValues[endDateIndex] = feed.version.validationResult.lastCalendarDate.format(GTFS_DATE_FORMATTER);
            referenceTracker.checkReferencesAndUniqueness(
                keyValue,
                lineNumber,
                table.fields[0],
                newServiceId,
                table,
                keyField,
                orderField
            );
            writeValuesToTable(clonedValues, true);
        }
    }

    public void writeValuesToTable(String[] values, boolean incrementLineNumbers) throws IOException {
        writer.write(values);
        if (incrementLineNumbers) {
            lineNumber++;
            mergedLineNumber++;
        }
    }

    public void flushAndClose() throws IOException {
        writer.flush();
        out.closeEntry();
    }

    public void writeHeaders() throws IOException {
        // Create entry for zip file.
        ZipEntry tableEntry = new ZipEntry(table.name + ".txt");
        out.putNextEntry(tableEntry);
        // Write headers to table.
        String[] headers = sharedSpecFields.stream()
            .map(f -> f.name)
            .toArray(String[]::new);
        writeValuesToTable(headers, false);
    }

    public void updateFutureFeedFirstDate() {
        if (
            table.name.equals("calendar_dates") &&
                handlingActiveFeed &&
                job.mergeType.equals(SERVICE_PERIOD) &&
                futureFirstCalendarStartDate.isBefore(LocalDate.MAX) &&
                futureFeedFirstDate.isBefore(futureFirstCalendarStartDate)
        ) {
            // If the future feed's first date is before its first calendar start date,
            // override the future feed first date with the calendar start date for use when checking
            // MTC calendar_dates and calendar records for modification/exclusion.
            futureFeedFirstDate = futureFirstCalendarStartDate;
        }
    }

    public boolean constructRowValues() throws IOException {
        // Piece together the row to write, which should look practically identical to the original
        // row except for the identifiers receiving a prefix to avoid ID conflicts.
        for (int specFieldIndex = 0; specFieldIndex < sharedSpecFields.size(); specFieldIndex++) {
            // There is nothing to do in this loop if it has already been determined that the record should
            // be skipped.
            if (skipRecord) continue;
            startNewField(specFieldIndex);
            // Handle filling in agency_id if missing when merging regional feeds. If false is returned,
            // the job has encountered a failing condition (the method handles failing the job itself).
            if (!updateAgencyIdIfNeeded()) {
                return false;
            }
            // Determine if field is a GTFS identifier (and scope if needed).
            scopeValueIfNeeded();
            // Only need to check for merge conflicts if using MTC merge type because
            // the regional merge type scopes all identifiers by default. Also, the
            // reference tracker will get far too large if we attempt to use it to
            // track references for a large number of feeds (e.g., every feed in New
            // York State).
            if (job.mergeType.equals(SERVICE_PERIOD) && checkFieldForMergeConflicts()) continue;
            // If the current field is a foreign reference, check if the reference has been removed in the
            // merged result. If this is the case (or other conditions are met), we will need to skip this
            // record. Likewise, if the reference has been modified, ensure that the value written to the
            // merged result is correctly updated.
            if (!areForeignRefsOk()) continue;
            rowValues[specFieldIndex] = valueToWrite;
        }
        return true;
    }

    public void finishRowAndWriteToZip() throws IOException {
        // Do not write rows that are designated to be skipped.
        if (skipRecord && job.mergeType.equals(SERVICE_PERIOD)) {
            mergeFeedsResult.recordsSkipCount++;
            return;
        }
        // Store row and stop values. If the return value is true, the record has been skipped and we
        // should skip writing the row to the merged table.
        if (storeRowAndStopValues()) {
            return;
        }
        // Finally, handle writing lines to zip entry.
        if (mergedLineNumber == 0) {
            writeHeaders();
        }
        // Write line to table.
        writeValuesToTable(rowValues, true);
        // If the current row is for a calendar service_id that is marked for cloning/renaming, clone the
        // values, change the ID, extend the start/end dates to the feed's full range, and write the
        // additional line to the file.
        addClonedServiceId();
    }

    public boolean lineIsBlank() throws IOException {
        if (csvReader.getValues().length == 1) {
            LOG.warn("Found blank line. Skipping...");
            return true;
        }
        return false;
    }
}
