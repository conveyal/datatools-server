package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.TestUtils;
import com.conveyal.datatools.common.utils.Scheduler;
import com.conveyal.datatools.manager.auth.Auth0Connection;
import com.conveyal.datatools.manager.models.*;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.HttpUtils;
import com.conveyal.datatools.manager.utils.json.JsonUtil;
import org.apache.http.HttpResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;

import static org.eclipse.jetty.http.HttpStatus.OK_200;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class FeedSourceControllerTest extends DatatoolsTest {
    private static Project project = null;
    private static FeedSource feedSourceWithUrl = null;
    private static FeedSource feedSourceWithNoUrl = null;
    private static Label publicLabel = null;
    private static Label adminOnlyLabel = null;

    @BeforeAll
    public static void setUp() throws IOException {
        DatatoolsTest.setUp();
        Auth0Connection.setAuthDisabled(true);
        project = new Project();
        project.name = "ProjectOne";
        project.autoFetchFeeds = true;
        Persistence.projects.create(project);
        feedSourceWithUrl = createFeedSource("FeedSourceOne", new URL("http://www.feedsource.com"));
        feedSourceWithNoUrl = createFeedSource("FeedSourceTwo", null);

        adminOnlyLabel = createLabel("Admin Only Label");
        adminOnlyLabel.adminOnly = true;
        publicLabel = createLabel("first");
    }

    @AfterAll
    public static void tearDown() {
        Auth0Connection.setAuthDisabled(Auth0Connection.getDefaultAuthDisabled());
        if (project != null) {
            Persistence.projects.removeById(project.id);
        }
        if (feedSourceWithUrl != null) {
            Persistence.feedSources.removeById(feedSourceWithUrl.id);
        }
        if (feedSourceWithNoUrl != null) {
            Persistence.feedSources.removeById(feedSourceWithNoUrl.id);
        }
        if (publicLabel != null) {
            Persistence.labels.removeById(publicLabel.id);
        }
        if (adminOnlyLabel != null) {
            Persistence.labels.removeById(adminOnlyLabel.id);
        }
    }

    /**
     * Manipulate a feed source and confirm that it is correctly scheduled:
     *  1. Create a feed source with auto fetch enabled and confirm it is scheduled.
     *  2. Update the same feed source turning off auth fetch and confirm it is no longer scheduled.
     *  3. Update the same feed source turning auth fetch back on and confirm it is scheduled once more.
     */
    @Test
    public void createFeedSourceWithUrlTest() {
        // create a feed source.
        HttpResponse createFeedSourceResponse = TestUtils.makeRequest("/api/manager/secure/feedsource",
            JsonUtil.toJson(feedSourceWithUrl),
            HttpUtils.REQUEST_METHOD.POST
        );
        assertEquals(OK_200, createFeedSourceResponse.getStatusLine().getStatusCode());
        assertEquals(1, jobCountForFeed(feedSourceWithUrl.id));

        // update feed source to disable feed fetch.
        feedSourceWithUrl.retrievalMethod = FeedRetrievalMethod.MANUALLY_UPLOADED;
        HttpResponse updateFeedSourceResponse
            = TestUtils.makeRequest(String.format("/api/manager/secure/feedsource/%s", feedSourceWithUrl.id),
            JsonUtil.toJson(feedSourceWithUrl),
            HttpUtils.REQUEST_METHOD.PUT
        );
        assertEquals(OK_200, updateFeedSourceResponse.getStatusLine().getStatusCode());
        assertEquals(0, jobCountForFeed(feedSourceWithUrl.id));

        // update feed source to enable auth fetch once more.
        feedSourceWithUrl.retrievalMethod = FeedRetrievalMethod.FETCHED_AUTOMATICALLY;
        updateFeedSourceResponse
            = TestUtils.makeRequest(String.format("/api/manager/secure/feedsource/%s", feedSourceWithUrl.id),
            JsonUtil.toJson(feedSourceWithUrl),
            HttpUtils.REQUEST_METHOD.PUT
        );
        assertEquals(OK_200, updateFeedSourceResponse.getStatusLine().getStatusCode());
        assertEquals(1, jobCountForFeed(feedSourceWithUrl.id));
    }

    /**
     * Create some labels, add them to the feed source make them admin only, and check that they don't appear if not an admin
     */
    @Test
    public void createFeedSourceWithLabels() {
        // Testing the following (important) components of label is as of yet not supported.
        // Running the JsonUtil.toJson method uses Jackson's serializer which breaks the input.
        // The only solution is to generate the JSON in another way...

        // The feedSourceWithNoUrl will be serialized as a full Label

        // Test that they're "rendered" as full label objects
        // Test that an admin can see 2 of them

        // Test that a non-admin can only one of them
    }

    /**
     * Create a feed source without defining the feed source url. Confirm that the feed source is not scheduled.
     */
    @Test
    public void createFeedSourceWithNoUrlTest() {
        HttpResponse createFeedSourceResponse = TestUtils.makeRequest("/api/manager/secure/feedsource",
            JsonUtil.toJson(feedSourceWithNoUrl),
            HttpUtils.REQUEST_METHOD.POST
        );
        assertEquals(OK_200, createFeedSourceResponse.getStatusLine().getStatusCode());
        assertEquals(0, jobCountForFeed(feedSourceWithNoUrl.id));
    }

    /**
     * Helper method to create feed source.
     */
    private static FeedSource createFeedSource(String name, URL url) {
        FeedSource feedSource = new FeedSource();
        feedSource.fetchFrequency = FetchFrequency.MINUTES;
        feedSource.fetchInterval = 1;
        feedSource.deployable = false;
        feedSource.name = name;
        feedSource.projectId = project.id;
        feedSource.retrievalMethod = FeedRetrievalMethod.FETCHED_AUTOMATICALLY;
        feedSource.url = url;
        return feedSource;
    }

    /**
     * Helper method to create label
     */
    private static Label createLabel(String name) {
        Label label = new Label(name, "A label used during testing", "#123", false, project.id);
        return label;
    }

    /**
     * Provide the job count for a given feed source.
     */
    private int jobCountForFeed(String feedSourceId) {
        return Scheduler.scheduledJobsForFeedSources.get(feedSourceId).size();
    }
}
