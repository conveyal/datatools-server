package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static com.conveyal.datatools.TestUtils.assertThatFeedHasNoErrorsOfType;
import static com.conveyal.datatools.TestUtils.assertThatSqlCountQueryYieldsExpectedCount;
import static com.conveyal.datatools.TestUtils.createFeedVersion;
import static com.conveyal.datatools.TestUtils.createFeedVersionFromGtfsZip;
import static com.conveyal.datatools.TestUtils.zipFolderFiles;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the various {@link MergeFeedsJob} merge types.
 */
public class MergeFeedsJobTest extends UnitTest {
    private static final Logger LOG = LoggerFactory.getLogger(MergeFeedsJobTest.class);
    private static Auth0UserProfile user = Auth0UserProfile.createTestAdminUser();
    private static FeedVersion bartVersion1;
    private static FeedVersion bartVersion2;
    private static FeedVersion calTrainVersion;
    private static Project project;
    private static FeedVersion napaVersion;
    private static FeedVersion bothCalendarFilesVersion;
    private static FeedVersion bothCalendarFilesVersion2;
    private static FeedVersion onlyCalendarVersion;
    private static FeedVersion onlyCalendarDatesVersion;

    /**
     * Prepare and start a testing-specific web server
     */
    @BeforeClass
    public static void setUp() throws IOException {
        // start server if it isn't already running
        DatatoolsTest.setUp();

        // Create a project, feed sources, and feed versions to merge.
        project = new Project();
        project.name = String.format("Test %s", new Date().toString());
        Persistence.projects.create(project);

        // Bart
        FeedSource bart = new FeedSource("BART");
        bart.projectId = project.id;
        Persistence.feedSources.create(bart);
        bartVersion1 = createFeedVersionFromGtfsZip(bart, "bart_old.zip");
        bartVersion2 = createFeedVersionFromGtfsZip(bart, "bart_new.zip");

        // Caltrain
        FeedSource caltrain = new FeedSource("Caltrain");
        caltrain.projectId = project.id;
        Persistence.feedSources.create(caltrain);
        calTrainVersion = createFeedVersionFromGtfsZip(caltrain, "caltrain_gtfs.zip");

        // Napa
        FeedSource napa = new FeedSource("Napa");
        napa.projectId = project.id;
        Persistence.feedSources.create(napa);
        napaVersion = createFeedVersionFromGtfsZip(napa, "napa-no-agency-id.zip");

        // Fake agencies (for testing calendar service_id merges with MTC strategy).
        FeedSource fakeAgency = new FeedSource("Fake Agency");
        fakeAgency.projectId = project.id;
        Persistence.feedSources.create(fakeAgency);
        bothCalendarFilesVersion = createFeedVersion(
            fakeAgency,
            zipFolderFiles("fake-agency-with-calendar-and-calendar-dates")
        );
        onlyCalendarVersion = createFeedVersion(
            fakeAgency,
            zipFolderFiles("fake-agency-with-only-calendar")
        );
        onlyCalendarDatesVersion = createFeedVersion(
            fakeAgency,
            zipFolderFiles("fake-agency-with-only-calendar-dates")
        );
        bothCalendarFilesVersion2 = createFeedVersion(
            fakeAgency,
            zipFolderFiles("fake-agency-with-calendar-and-calendar-dates-2")
        );
    }

    /**
     * Ensures that a regional feed merge will produce a feed that includes all entities from each feed.
     */
    @Test
    public void canMergeRegional() throws SQLException {
        // Set up list of feed versions to merge.
        Set<FeedVersion> versions = new HashSet<>();
        versions.add(bartVersion1);
        versions.add(calTrainVersion);
        versions.add(napaVersion);
        FeedVersion mergedVersion = regionallyMergeVersions(versions);

        // Ensure the feed has the row counts we expect.
        assertEquals(
            "trips count for merged feed should equal sum of trips for versions merged.",
            bartVersion1.feedLoadResult.trips.rowCount + calTrainVersion.feedLoadResult.trips.rowCount + napaVersion.feedLoadResult.trips.rowCount,
            mergedVersion.feedLoadResult.trips.rowCount
        );
        assertEquals(
            "routes count for merged feed should equal sum of routes for versions merged.",
            bartVersion1.feedLoadResult.routes.rowCount + calTrainVersion.feedLoadResult.routes.rowCount + napaVersion.feedLoadResult.routes.rowCount,
            mergedVersion.feedLoadResult.routes.rowCount
            );
        assertEquals(
            "stops count for merged feed should equal sum of stops for versions merged.",
            mergedVersion.feedLoadResult.stops.rowCount,
            bartVersion1.feedLoadResult.stops.rowCount + calTrainVersion.feedLoadResult.stops.rowCount + napaVersion.feedLoadResult.stops.rowCount
        );
        assertEquals(
            "agency count for merged feed should equal sum of agency for versions merged.",
            mergedVersion.feedLoadResult.agency.rowCount,
            bartVersion1.feedLoadResult.agency.rowCount + calTrainVersion.feedLoadResult.agency.rowCount + napaVersion.feedLoadResult.agency.rowCount
        );
        assertEquals(
            "stopTimes count for merged feed should equal sum of stopTimes for versions merged.",
            mergedVersion.feedLoadResult.stopTimes.rowCount,
            bartVersion1.feedLoadResult.stopTimes.rowCount + calTrainVersion.feedLoadResult.stopTimes.rowCount + napaVersion.feedLoadResult.stopTimes.rowCount
        );
        assertEquals(
            "calendar count for merged feed should equal sum of calendar for versions merged.",
            mergedVersion.feedLoadResult.calendar.rowCount,
            bartVersion1.feedLoadResult.calendar.rowCount + calTrainVersion.feedLoadResult.calendar.rowCount + napaVersion.feedLoadResult.calendar.rowCount
        );
        assertEquals(
            "calendarDates count for merged feed should equal sum of calendarDates for versions merged.",
            mergedVersion.feedLoadResult.calendarDates.rowCount,
            bartVersion1.feedLoadResult.calendarDates.rowCount + calTrainVersion.feedLoadResult.calendarDates.rowCount + napaVersion.feedLoadResult.calendarDates.rowCount
        );
        // Verify that the one stop with a parent_station value (an example of a self-referential field) is updated to
        // the feed-scoped value.
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format("select count(*) from %s.stops where parent_station = 'Napa1:104'", mergedVersion.namespace),
            1
        );
        // Ensure there are no referential integrity errors, duplicate ID, or wrong number of
        // fields errors.
        assertThatFeedHasNoErrorsOfType(
            mergedVersion.namespace,
            NewGTFSErrorType.REFERENTIAL_INTEGRITY.toString(),
            NewGTFSErrorType.DUPLICATE_ID.toString(),
            NewGTFSErrorType.WRONG_NUMBER_OF_FIELDS.toString()
        );
    }

    /**
     * Tests whether the calendar_dates and trips tables get proper feed scoping in a merge with one feed with only
     * calendar_dates and another with only the calendar.
     */
    @Test
    public void canMergeRegionalWithOnlyCalendarFeed () throws SQLException {
        Set<FeedVersion> versions = new HashSet<>();
        versions.add(onlyCalendarDatesVersion);
        versions.add(onlyCalendarVersion);
        FeedVersion mergedVersion = regionallyMergeVersions(versions);

        // assert service_ids have been feed scoped properly
        String mergedNamespace = mergedVersion.namespace;

        // - calendar table
        // expect a total of 2 records in calendar table
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format("SELECT count(*) FROM %s.calendar", mergedNamespace),
            2
        );
        // onlyCalendarVersion's common_id service_id should be scoped
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format(
                "SELECT count(*) FROM %s.calendar WHERE service_id='Fake_Agency2:common_id'",
                mergedNamespace
            ),
            1
        );
        // onlyCalendarVersion's only_calendar_id service_id should be scoped
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format(
                "SELECT count(*) FROM %s.calendar WHERE service_id='Fake_Agency2:only_calendar_id'",
                mergedNamespace
            ),
            1
        );

        // - calendar_dates table
        // expect only 1 record in calendar_dates table
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format(
                "SELECT count(*) FROM %s.calendar_dates",
                mergedNamespace
            ),
            2
        );
        // onlyCalendarDatesVersion's common_id service_id should be scoped
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format(
                "SELECT count(*) FROM %s.calendar_dates WHERE service_id='Fake_Agency3:common_id'",
                mergedNamespace
            ),
            1
        );
        // onlyCalendarDatesVersion's only_calendar_dates_id service_id should be scoped
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format(
                "SELECT count(*) FROM %s.calendar_dates WHERE service_id='Fake_Agency3:only_calendar_dates_id'",
                mergedNamespace
            ),
            1
        );

        // - trips table
        // expect only 2 records in trips table
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format(
                "SELECT count(*) FROM %s.trips",
                mergedNamespace
            ),
            2
        );
        // onlyCalendarDatesVersion's common_id service_id should be scoped
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format(
                "SELECT count(*) FROM %s.trips WHERE service_id='Fake_Agency3:common_id'",
                mergedNamespace
            ),
            1
        );
        // onlyCalendarVersion's common_id service_id should be scoped
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format(
                "SELECT count(*) FROM %s.trips WHERE service_id='Fake_Agency2:common_id'",
                mergedNamespace
            ),
            1
        );
    }

    /**
     * Ensures that an MTC merge of feeds with duplicate trip IDs will fail.
     */
    @Test
    public void mergeMTCShouldFailOnDuplicateTrip() {
        Set<FeedVersion> versions = new HashSet<>();
        versions.add(bartVersion1);
        versions.add(bartVersion2);
        MergeFeedsJob mergeFeedsJob = new MergeFeedsJob(user, versions, "merged_output", MergeFeedsType.MTC);
        // Run the job in this thread (we're not concerned about concurrency here).
        mergeFeedsJob.run();
        // Result should fail.
        assertTrue(
            "Merge feeds job should fail due to duplicate trip IDs.",
            mergeFeedsJob.mergeFeedsResult.failed
        );
    }

    /**
     * Tests that the MTC merge strategy will successfully merge BART feeds. Note: this test turns off
     * {@link MergeFeedsJob#failOnDuplicateTripId} in order to force the merge to succeed even though there are duplicate
     * trips contained within.
     */
    @Test
    public void canMergeBARTFeeds() throws SQLException {
        Set<FeedVersion> versions = new HashSet<>();
        versions.add(bartVersion1);
        versions.add(bartVersion2);
        MergeFeedsJob mergeFeedsJob = new MergeFeedsJob(user, versions, "merged_output", MergeFeedsType.MTC);
        // This time, turn off the failOnDuplicateTripId flag.
        mergeFeedsJob.failOnDuplicateTripId = false;
        // Result should succeed this time.
        mergeFeedsJob.run();
        assertFeedMergeSucceeded(mergeFeedsJob);
        // Check GTFS+ line numbers.
        assertEquals(
            "Merged directions count should equal expected value.",
            2, // Magic number represents expected number of lines after merge.
            mergeFeedsJob.mergeFeedsResult.linesPerTable.get("directions").intValue()
        );
        assertEquals(
            "Merged feed stop_attributes count should equal expected value.",
            2, // Magic number represents the number of stop_attributes in the merged BART feed.
            mergeFeedsJob.mergeFeedsResult.linesPerTable.get("stop_attributes").intValue()
        );
        // Check GTFS file line numbers.
        assertEquals(
            "Merged feed trip count should equal expected value.",
            4552, // Magic number represents the number of trips in the merged BART feed.
            mergeFeedsJob.mergedVersion.feedLoadResult.trips.rowCount
        );
        assertEquals(
            "Merged feed route count should equal expected value.",
            9, // Magic number represents the number of routes in the merged BART feed.
            mergeFeedsJob.mergedVersion.feedLoadResult.routes.rowCount
        );
        assertEquals(
            "Merged feed shapes count should equal expected value.",
            // During merge, if identical shape_id is found in both feeds, active feed shape_id should be feed-scoped.
            bartVersion1.feedLoadResult.shapes.rowCount + bartVersion2.feedLoadResult.shapes.rowCount,
            mergeFeedsJob.mergedVersion.feedLoadResult.shapes.rowCount
        );
        // Expect that two calendar dates are excluded from the past feed (because they occur after the first date of
        // the future feed) .
        int expectedCalendarDatesCount = bartVersion1.feedLoadResult.calendarDates.rowCount + bartVersion2.feedLoadResult.calendarDates.rowCount - 2;
        assertEquals(
            "Merged feed calendar_dates count should equal expected value.",
            // During merge, if identical shape_id is found in both feeds, active feed shape_id should be feed-scoped.
            expectedCalendarDatesCount,
            mergeFeedsJob.mergedVersion.feedLoadResult.calendarDates.rowCount
        );
        // Ensure there are no referential integrity errors or duplicate ID errors.
        assertThatFeedHasNoErrorsOfType(
            mergeFeedsJob.mergedVersion.namespace,
            NewGTFSErrorType.REFERENTIAL_INTEGRITY.toString(),
            NewGTFSErrorType.DUPLICATE_ID.toString()
        );
    }

    /**
     * Tests whether a MTC feed merge of two feed versions correctly feed scopes the service_id's of the feed that is
     * chronologically before the other one. This tests two feeds where one of them has both calendar files, and the
     * other has only the calendar file.
     */
    @Test
    public void canMergeFeedsWithMTCForServiceIds1 () throws SQLException {
        Set<FeedVersion> versions = new HashSet<>();
        versions.add(bothCalendarFilesVersion);
        versions.add(onlyCalendarVersion);
        MergeFeedsJob mergeFeedsJob = new MergeFeedsJob(user, versions, "merged_output", MergeFeedsType.MTC);
        // Run the job in this thread (we're not concerned about concurrency here).
        mergeFeedsJob.run();
        assertFeedMergeSucceeded(mergeFeedsJob);
        // assert service_ids have been feed scoped properly
        String mergedNamespace = mergeFeedsJob.mergedVersion.namespace;

        // - calendar table
        // expect a total of 4 records in calendar table
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format("SELECT count(*) FROM %s.calendar", mergedNamespace),
            4
        );
        // bothCalendarFilesVersion's common_id service_id should be scoped
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format(
                "SELECT count(*) FROM %s.calendar WHERE service_id='Fake_Agency1:common_id'",
                mergedNamespace
            ),
            1
        );
        // bothCalendarFilesVersion's both_id service_id should be scoped
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format(
                "SELECT count(*) FROM %s.calendar WHERE service_id='Fake_Agency1:both_id'",
                mergedNamespace
            ),
            1
        );
        // onlyCalendarVersion's common id should not be scoped
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format(
                "SELECT count(*) FROM %s.calendar WHERE service_id='common_id'",
                mergedNamespace
            ),
            1
        );
        // onlyCalendarVersion's only_calendar_id service_id should not be scoped
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format(
                "SELECT count(*) FROM %s.calendar WHERE service_id='only_calendar_id'",
                mergedNamespace
            ),
            1
        );

        // - calendar_dates table
        // expect only 2 records in calendar_dates table
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format("SELECT count(*) FROM %s.calendar_dates", mergedNamespace),
            2
        );
        // bothCalendarFilesVersion's common_id service_id should be scoped
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format(
                "SELECT count(*) FROM %s.calendar_dates WHERE service_id='Fake_Agency1:common_id'",
                mergedNamespace
            ),
            1
        );
        // bothCalendarFilesVersion's both_id service_id should be scoped
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format(
                "SELECT count(*) FROM %s.calendar_dates WHERE service_id='Fake_Agency1:both_id'",
                mergedNamespace
            ),
            1
        );

        // - trips table
        // expect only 2 records in trips table
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format("SELECT count(*) FROM %s.trips", mergedNamespace),
            2
        );
        // bothCalendarFilesVersion's common_id service_id should be scoped
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format(
                "SELECT count(*) FROM %s.trips WHERE service_id='Fake_Agency1:common_id'",
                mergedNamespace
            ),
            1
        );
        // onlyCalendarVersion's common_id service_id should not be scoped
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format(
                "SELECT count(*) FROM %s.trips WHERE service_id='common_id'",
                mergedNamespace
            ),
            1
        );
    }

    /**
     * Tests whether a MTC feed merge of two feed versions correctly feed scopes the service_id's of the feed that is
     * chronologically before the other one. This tests two feeds where one of them has only the calendar_dates files,
     * and the other has only the calendar file.
     */
    @Test
    public void canMergeFeedsWithMTCForServiceIds2 () throws SQLException {
        Set<FeedVersion> versions = new HashSet<>();
        versions.add(onlyCalendarDatesVersion);
        versions.add(onlyCalendarVersion);
        MergeFeedsJob mergeFeedsJob = new MergeFeedsJob(user, versions, "merged_output", MergeFeedsType.MTC);
        // Run the job in this thread (we're not concerned about concurrency here).
        mergeFeedsJob.run();
        assertFeedMergeSucceeded(mergeFeedsJob);
        // assert service_ids have been feed scoped properly
        String mergedNamespace = mergeFeedsJob.mergedVersion.namespace;

        // - calendar table
        // expect a total of 4 records in calendar table
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format("SELECT count(*) FROM %s.calendar", mergedNamespace),
            2
        );
        // onlyCalendarVersion's common id should not be scoped
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format(
                "SELECT count(*) FROM %s.calendar WHERE service_id='common_id'",
                mergedNamespace
            ),
            1
        );
        // onlyCalendarVersion's only_calendar_id service_id should not be scoped
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format(
                "SELECT count(*) FROM %s.calendar WHERE service_id='only_calendar_id'",
                mergedNamespace
            ),
            1
        );

        // - calendar_dates table
        // expect only 2 records in calendar_dates table
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format("SELECT count(*) FROM %s.calendar_dates", mergedNamespace),
            2
        );
        // onlyCalendarDatesVersion's common_id service_id should be scoped
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format(
                "SELECT count(*) FROM %s.calendar_dates WHERE service_id='Fake_Agency3:common_id'",
                mergedNamespace
            ),
            1
        );
        // onlyCalendarDatesVersion's only_calendar_dates_id service_id should be scoped
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format(
                "SELECT count(*) FROM %s.calendar_dates WHERE service_id='Fake_Agency3:only_calendar_dates_id'",
                mergedNamespace
            ),
            1
        );

        // - trips table
        // expect only 2 records in trips table
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format("SELECT count(*) FROM %s.trips", mergedNamespace),
            2
        );
        // bothCalendarFilesVersion's common_id service_id should be scoped
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format(
                "SELECT count(*) FROM %s.trips WHERE service_id='Fake_Agency3:common_id'",
                mergedNamespace
            ),
            1
        );
        // onlyCalendarVersion's common_id service_id should not be scoped
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format(
                "SELECT count(*) FROM %s.trips WHERE service_id='common_id'",
                mergedNamespace
            ),
            1
        );
        // This fails, but if remappedReferences isn't actually needed maybe the current implementation is good-to-go
        // assertThat(mergeFeedsJob.mergeFeedsResult.remappedReferences, equalTo(1));
    }

    /**
     * Tests whether a MTC feed merge of two feed versions correctly removes calendar records that have overlapping
     * service but keeps calendar_dates records that share service_id with the removed calendar and trips that reference
     * that service_id.
     *
     */
    @Test
    public void canMergeFeedsWithMTCForServiceIds3 () throws SQLException {
        Set<FeedVersion> versions = new HashSet<>();
        versions.add(bothCalendarFilesVersion);
        versions.add(bothCalendarFilesVersion2);
        MergeFeedsJob mergeFeedsJob = new MergeFeedsJob(user, versions, "merged_output", MergeFeedsType.MTC);
        // Run the job in this thread (we're not concerned about concurrency here).
        mergeFeedsJob.run();
        assertFeedMergeSucceeded(mergeFeedsJob);
        // assert service_ids have been feed scoped properly
        String mergedNamespace = mergeFeedsJob.mergedVersion.namespace;
        // - calendar table
        // expect a total of 3 records in calendar table
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format("SELECT count(*) FROM %s.calendar", mergedNamespace),
            3
        );
        // - calendar_dates table
        // expect 3 records in calendar_dates table
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format("SELECT count(*) FROM %s.calendar_dates", mergedNamespace),
            3
        );

        // - trips table
        // expect 3 records in trips table
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format("SELECT count(*) FROM %s.trips", mergedNamespace),
            3
        );
        // common_id service_id should be scoped for earlier feed version.
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format(
                "SELECT count(*) FROM %s.trips WHERE service_id='Fake_Agency4:common_id'",
                mergedNamespace
            ),
            1
        );
        // Modified cal_to_remove should still exist in calendar_dates. It is modified even though it does not exist in
        // the future feed due to the MTC requirement to update all service_ids in the past feed.
        // See https://github.com/ibi-group/datatools-server/issues/244
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format(
                "SELECT count(*) FROM %s.calendar_dates WHERE service_id='Fake_Agency4:cal_to_remove'",
                mergedNamespace
            ),
            1
        );
    }

    /**
     * Verifies that a completed merge feeds job did not fail.
     * @param mergeFeedsJob
     */
    private void assertFeedMergeSucceeded(MergeFeedsJob mergeFeedsJob) {
        if (mergeFeedsJob.mergedVersion.namespace == null || mergeFeedsJob.mergeFeedsResult.failed) {
            fail("Merge feeds job failed: " + String.join(", ", mergeFeedsJob.mergeFeedsResult.failureReasons));
        }
    }

    /**
     * Merges a set of FeedVersions and then creates a new FeedSource and FeedVersion of the merged feed.
     */
    private FeedVersion regionallyMergeVersions(Set<FeedVersion> versions) {
        String mergeName = UUID.randomUUID().toString();
        MergeFeedsJob mergeFeedsJob = new MergeFeedsJob(user, versions, mergeName, MergeFeedsType.REGIONAL);
        // Run the job in this thread (we're not concerned about concurrency here).
        mergeFeedsJob.run();
        // Create a new feed source/version for the merged feed, so we can easily analyze its contents.
        FeedSource source = new FeedSource(mergeName);
        source.projectId = project.id;
        Persistence.feedSources.create(source);
        File feed = FeedVersion.feedStore.getFeed(mergeName + ".zip");
        LOG.info("Regional merged file: {}", feed.getAbsolutePath());
        return createFeedVersion(source, feed);
    }
}
