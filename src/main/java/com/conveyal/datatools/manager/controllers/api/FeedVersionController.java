package com.conveyal.datatools.manager.controllers.api;

import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.actions.S3Actions;
import com.conveyal.datatools.common.utils.SparkUtils;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.jobs.BuildTransportNetworkJob;
import com.conveyal.datatools.manager.jobs.CreateFeedVersionFromSnapshotJob;
import com.conveyal.datatools.manager.jobs.ProcessSingleFeedJob;
import com.conveyal.datatools.manager.jobs.ReadTransportNetworkJob;
import com.conveyal.datatools.manager.models.FeedDownloadToken;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.HashUtils;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.conveyal.r5.analyst.PointSet;
import com.conveyal.r5.analyst.cluster.AnalystClusterRequest;
import com.conveyal.r5.analyst.cluster.ResultEnvelope;
import com.conveyal.r5.analyst.cluster.TaskStatistics;
import com.conveyal.r5.api.util.LegMode;
import com.conveyal.r5.api.util.TransitModes;
import com.conveyal.r5.profile.ProfileRequest;
import com.conveyal.r5.profile.RepeatedRaptorProfileRouter;
import com.conveyal.r5.profile.StreetMode;
import com.conveyal.r5.streets.LinkedPointSet;
import com.conveyal.r5.transit.TransportNetwork;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import javax.servlet.MultipartConfigElement;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletRequestWrapper;
import javax.servlet.http.Part;

import static com.conveyal.datatools.common.utils.S3Utils.getS3Credentials;
import static com.conveyal.datatools.common.utils.SparkUtils.downloadFile;
import static com.conveyal.datatools.common.utils.SparkUtils.haltWithError;
import static com.conveyal.datatools.manager.controllers.api.FeedSourceController.checkFeedSourcePermissions;
import static spark.Spark.*;

public class FeedVersionController  {

    // TODO use this instead of stringly typed permissions
    enum Permission {
        VIEW, MANAGE
    }

    public static final Logger LOG = LoggerFactory.getLogger(FeedVersionController.class);
    private static ObjectMapper mapper = new ObjectMapper();
    public static JsonManager<FeedVersion> json =
            new JsonManager<FeedVersion>(FeedVersion.class, JsonViews.UserInterface.class);
    private static Set<String> readingNetworkVersionList = new HashSet<>();

    /**
     * Grab the feed version for the ID supplied in the request.
     * If you pass in ?summarized=true, don't include the full tree of validation results, only the counts.
     */
    public static FeedVersion getFeedVersion (Request req, Response res) throws JsonProcessingException {
        FeedVersion feedVersion = requestFeedVersion(req, "view");
        return feedVersion;
    }

    /**
     * Get all feed versions for a given feedSource (whose ID is specified in the request).
     */
    public static Collection<FeedVersion> getAllFeedVersionsForFeedSource(Request req, Response res) throws JsonProcessingException {
        // Check permissions and get the FeedSource whose FeedVersions we want.
        FeedSource feedSource = requestFeedSourceById(req, "view");
        Collection<FeedVersion> feedVersions = feedSource.retrieveFeedVersions();
        return feedVersions;
    }

    private static FeedSource requestFeedSourceById(Request req, String action) {
        String id = req.queryParams("feedSourceId");
        if (id == null) {
            halt(SparkUtils.formatJSON("Please specify feedSourceId param", 400));
        }
        return checkFeedSourcePermissions(req, Persistence.feedSources.getById(id), action);
    }

    /**
     * Upload a feed version directly. This is done behind Backbone's back, and as such uses
     * x-multipart-formdata rather than a json blob. This is done because uploading files in a JSON
     * blob is not pretty, and we don't really need to retrieveById the Backbone object directly; page re-render isn't
     * a problem.
     *
     * Auto-fetched feeds are no longer restricted from having directly-uploaded versions, so we're not picky about
     * that anymore.
     *
     * @return the job ID that allows monitoring progress of the load process
     */
    public static String createFeedVersion (Request req, Response res) throws IOException, ServletException {

        Auth0UserProfile userProfile = req.attribute("user");
        FeedSource feedSource = requestFeedSourceById(req, "manage");
        FeedVersion latestVersion = feedSource.retrieveLatest();
        FeedVersion newFeedVersion = new FeedVersion(feedSource);
        newFeedVersion.retrievalMethod = FeedSource.FeedRetrievalMethod.MANUALLY_UPLOADED;


        // FIXME: Make the creation of new GTFS files generic to handle other feed creation methods, including fetching
        // by URL and loading from the editor.
        File newGtfsFile = new File(DataManager.getConfigPropertyAsText("application.data.gtfs"), newFeedVersion.id);
        try {
            // Bypass Spark's request wrapper which always caches the request body in memory that may be a very large
            // GTFS file. Also, the body of the request is the GTFS file instead of using multipart form data because
            // multipart form handling code also caches the request body.
            ServletInputStream inputStream = ((ServletRequestWrapper) req.raw()).getRequest().getInputStream();
            FileOutputStream fileOutputStream = new FileOutputStream(newGtfsFile);
            // Guava's ByteStreams.copy uses a 4k buffer (no need to wrap output stream), but does not close streams.
            ByteStreams.copy(inputStream, fileOutputStream);
            fileOutputStream.close();
            inputStream.close();
            // Set last modified based on value of query param. This is determined/supplied by the client
            // request because this data gets lost in the uploadStream otherwise.
            Long lastModified = req.queryParams("lastModified") != null ? Long.valueOf(req.queryParams("lastModified")) : null;
            if (lastModified != null) newGtfsFile.setLastModified(lastModified);
            LOG.info("Last modified: {}", new Date(newGtfsFile.lastModified()));
            LOG.info("Saving feed from upload {}", feedSource);
        } catch (Exception e) {
            LOG.error("Unable to open input stream from uploaded file", e);
            haltWithError(400, "Unable to read uploaded feed");
        }

        // TODO: fix FeedVersion.hash() call when called in this context. Nothing gets hashed because the file has not been saved yet.
        // newFeedVersion.hash();
        newFeedVersion.hash = HashUtils.hashFile(newGtfsFile);

        // Check that the hashes of the feeds don't match, i.e. that the feed has changed since the last version.
        // (as long as there is a latest version, i.e. the feed source is not completely new)
        if (latestVersion != null && latestVersion.hash.equals(newFeedVersion.hash)) {
            // Uploaded feed matches latest. Delete GTFS file because it is a duplicate.
            LOG.error("Upload version {} matches latest version {}.", newFeedVersion.id, latestVersion.id);
            newGtfsFile.delete();
            LOG.warn("File deleted");

            // There is no need to delete the newFeedVersion because it has not yet been persisted to MongoDB.
            haltWithError(304, "Uploaded feed is identical to the latest version known to the database.");
        }

        newFeedVersion.setName(newFeedVersion.formattedTimestamp() + " Upload");
        // TODO newFeedVersion.fileTimestamp still exists

        // Must be handled by executor because it takes a long time.
        ProcessSingleFeedJob processSingleFeedJob = new ProcessSingleFeedJob(newFeedVersion, userProfile.getUser_id());
        DataManager.heavyExecutor.execute(processSingleFeedJob);

        return processSingleFeedJob.jobId;
    }

    public static boolean createFeedVersionFromSnapshot (Request req, Response res) throws IOException, ServletException {

        Auth0UserProfile userProfile = req.attribute("user");
        // TODO: Should the ability to create a feedVersion from snapshot be controlled by the 'edit-gtfs' privilege?
        FeedSource feedSource = requestFeedSourceById(req, "manage");
        FeedVersion feedVersion = new FeedVersion(feedSource);
        CreateFeedVersionFromSnapshotJob createFromSnapshotJob =
                new CreateFeedVersionFromSnapshotJob(feedVersion, req.queryParams("snapshotId"), userProfile.getUser_id());
        DataManager.heavyExecutor.execute(createFromSnapshotJob);

        return true;
    }

    /**
     * Spark HTTP API handler that deletes a single feed version based on the ID in the request.
     */
    public static FeedVersion deleteFeedVersion(Request req, Response res) {
        FeedVersion version = requestFeedVersion(req, "manage");
        version.delete();
        return version;
    }

    public static FeedVersion requestFeedVersion(Request req, String action) {
        String id = req.params("id");
        FeedVersion version = Persistence.feedVersions.getById(id);
        if (version == null) {
            halt(404, "Version ID does not exist");
        }
        // Performs permissions checks on the feed source this feed version belongs to, and halts if permission is denied.
        checkFeedSourcePermissions(req, version.parentFeedSource(), action);
        return version;
    }

//    public static JsonNode getValidationResult(Request req, Response res) {
//        return getValidationResult(req, res, false);
//    }

//    public static JsonNode getPublicValidationResult(Request req, Response res) {
//        return getValidationResult(req, res, true);
//    }

    // FIXME: this used to control authenticated access to validation results.
//    public static JsonNode getValidationResult(Request req, Response res, boolean checkPublic) {
//        FeedVersion version = requestFeedVersion(req, "view");
//
//        return version.retrieveValidationResult(false);
//    }

    public static JsonNode getIsochrones(Request req, Response res) {
        FeedVersion version = requestFeedVersion(req, "view");

        Auth0UserProfile userProfile = req.attribute("user");
        // if tn is null, check first if it's being built, else try reading in tn
        if (version.transportNetwork == null) {
            buildOrReadTransportNetwork(version, userProfile);
        }
        else {
            // remove version from list of reading network
            if (readingNetworkVersionList.contains(version.id)) {
                readingNetworkVersionList.remove(version.id);
            }
            AnalystClusterRequest clusterRequest = buildProfileRequest(req);
            return getRouterResult(version.transportNetwork, clusterRequest);
        }
        return null;
    }

    private static void buildOrReadTransportNetwork(FeedVersion version, Auth0UserProfile userProfile) {
        InputStream is = null;
        try {
            if (!readingNetworkVersionList.contains(version.id)) {
                is = new FileInputStream(version.transportNetworkPath());
                readingNetworkVersionList.add(version.id);
                try {
//                    version.transportNetwork = TransportNetwork.read(is);
                    ReadTransportNetworkJob rtnj = new ReadTransportNetworkJob(version, userProfile.getUser_id());
                    DataManager.heavyExecutor.execute(rtnj);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            halt(202, "Try again later. Reading transport network");
        }
        // Catch exception if transport network not built yet
        catch (Exception e) {
            if (DataManager.isModuleEnabled("r5_network") && !readingNetworkVersionList.contains(version.id)) {
                LOG.warn("Transport network not found. Beginning build.", e);
                readingNetworkVersionList.add(version.id);
                BuildTransportNetworkJob btnj = new BuildTransportNetworkJob(version, userProfile.getUser_id());
                DataManager.heavyExecutor.execute(btnj);
            }
            halt(202, "Try again later. Building transport network");
        }
    }

    private static JsonNode getRouterResult(TransportNetwork transportNetwork, AnalystClusterRequest clusterRequest) {
        PointSet targets;
        if (transportNetwork.gridPointSet == null) {
            transportNetwork.rebuildLinkedGridPointSet();
        }
        targets = transportNetwork.gridPointSet;
        StreetMode mode = StreetMode.WALK;
        final LinkedPointSet linkedTargets = targets.link(transportNetwork.streetLayer, mode);
        RepeatedRaptorProfileRouter router = new RepeatedRaptorProfileRouter(transportNetwork, clusterRequest, linkedTargets, new TaskStatistics());
        ResultEnvelope result = router.route();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            JsonGenerator jgen = new JsonFactory().createGenerator(out);
            jgen.writeStartObject();
            result.avgCase.writeIsochrones(jgen);
            jgen.writeEndObject();
            jgen.close();
            out.close();
            String outString = new String( out.toByteArray(), StandardCharsets.UTF_8 );
            return mapper.readTree(outString);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static AnalystClusterRequest buildProfileRequest(Request req) {
        // required fields?
        Double fromLat = Double.valueOf(req.queryParams("fromLat"));
        Double fromLon = Double.valueOf(req.queryParams("fromLon"));
        Double toLat = Double.valueOf(req.queryParams("toLat"));
        Double toLon = Double.valueOf(req.queryParams("toLon"));
        LocalDate date = req.queryParams("date") != null ? LocalDate.parse(req.queryParams("date"), DateTimeFormatter.ISO_LOCAL_DATE) : LocalDate.now(); // 2011-12-03

        // optional with defaults
        Integer fromTime = req.queryParams("fromTime") != null ? Integer.valueOf(req.queryParams("fromTime")) : 9 * 3600;
        Integer toTime = req.queryParams("toTime") != null ? Integer.valueOf(req.queryParams("toTime")) : 10 * 3600;

        // build request with transit as default mode
        AnalystClusterRequest clusterRequest = new AnalystClusterRequest();
        clusterRequest.profileRequest = new ProfileRequest();
        clusterRequest.profileRequest.transitModes = EnumSet.of(TransitModes.TRANSIT);
        clusterRequest.profileRequest.accessModes = EnumSet.of(LegMode.WALK);
        clusterRequest.profileRequest.date = date;
        clusterRequest.profileRequest.fromLat = fromLat;
        clusterRequest.profileRequest.fromLon = fromLon;
        clusterRequest.profileRequest.toLat = toLat;
        clusterRequest.profileRequest.toLon = toLon;
        clusterRequest.profileRequest.fromTime = fromTime;
        clusterRequest.profileRequest.toTime = toTime;
        clusterRequest.profileRequest.egressModes = EnumSet.of(LegMode.WALK);
        clusterRequest.profileRequest.zoneId = ZoneId.of("America/New_York");

        return clusterRequest;
    }

    public static Boolean renameFeedVersion (Request req, Response res) throws JsonProcessingException {
        FeedVersion v = requestFeedVersion(req, "manage");

        String name = req.queryParams("name");
        if (name == null) {
            halt(400, "Name parameter not specified");
        }

        Persistence.feedVersions.updateField(v.id, "name", name);
        return true;
    }

    private static Object downloadFeedVersionDirectly(Request req, Response res) {
        FeedVersion version = requestFeedVersion(req, "view");
        return downloadFile(version.retrieveGtfsFile(), version.id, res);
    }

    /**
     * Returns credentials that a client may use to then download a feed version. Functionality
     * changes depending on whether application.data.use_s3_storage config property is true.
     */
    public static Object getFeedDownloadCredentials(Request req, Response res) {
        FeedVersion version = requestFeedVersion(req, "view");

        // if storing feeds on s3, return temporary s3 credentials for that zip file
        if (DataManager.useS3) {
            return getS3Credentials(DataManager.awsRole, DataManager.feedBucket, FeedStore.s3Prefix + version.id, Statement.Effect.Allow, S3Actions.GetObject, 900);
        } else {
            // when feeds are stored locally, single-use download token will still be used
            FeedDownloadToken token = new FeedDownloadToken(version);
            Persistence.tokens.create(token);
            return token;
        }
    }

    /**
     * API endpoint that instructs application to validate a feed if validation does not exist for version.
     */
    private static JsonNode validate (Request req, Response res) {
        FeedVersion version = requestFeedVersion(req, "manage");

        // FIXME: Update for sql-loader validation process?
        return null;
//        return version.retrieveValidationResult(true);
    }

    private static FeedVersion publishToExternalResource (Request req, Response res) {
        FeedVersion version = requestFeedVersion(req, "manage");

        // notify any extensions of the change
        for(String resourceType : DataManager.feedResources.keySet()) {
            DataManager.feedResources.get(resourceType).feedVersionCreated(version, null);
        }
        // update published version ID on feed source
        Persistence.feedSources.update(version.feedSourceId, String.format("{publishedVersionId: %s}", version.id));
        return version;
    }

    /**
     * Download locally stored feed version with token supplied by this application. This method is only used when
     * useS3 is set to false. Otherwise, a direct download from s3 should be used.
     */
    private static Object downloadFeedVersionWithToken (Request req, Response res) {
        String tokenValue = req.params("token");
        FeedDownloadToken token = Persistence.tokens.getById(tokenValue);

        if(token == null || !token.isValid()) {
            halt(400, "Feed download token not valid");
        }

        // Fetch feed version to download.
        FeedVersion version = token.retrieveFeedVersion();
        if (version == null) {
            haltWithError(400, "Could not retrieve version to download");
        }
        // Remove token so that it cannot be used again for feed download
        Persistence.tokens.removeById(tokenValue);
        File file = version.retrieveGtfsFile();
        return downloadFile(file, version.id, res);
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/feedversion/:id", FeedVersionController::getFeedVersion, json::write);
        get(apiPrefix + "secure/feedversion/:id/download", FeedVersionController::downloadFeedVersionDirectly);
        get(apiPrefix + "secure/feedversion/:id/downloadtoken", FeedVersionController::getFeedDownloadCredentials, json::write);
//        get(apiPrefix + "secure/feedversion/:id/validation", FeedVersionController::getValidationResult, json::write);
        post(apiPrefix + "secure/feedversion/:id/validate", FeedVersionController::validate, json::write);
        get(apiPrefix + "secure/feedversion/:id/isochrones", FeedVersionController::getIsochrones, json::write);
        get(apiPrefix + "secure/feedversion", FeedVersionController::getAllFeedVersionsForFeedSource, json::write);
        post(apiPrefix + "secure/feedversion", FeedVersionController::createFeedVersion, json::write);
        post(apiPrefix + "secure/feedversion/fromsnapshot", FeedVersionController::createFeedVersionFromSnapshot, json::write);
        put(apiPrefix + "secure/feedversion/:id/rename", FeedVersionController::renameFeedVersion, json::write);
        post(apiPrefix + "secure/feedversion/:id/publish", FeedVersionController::publishToExternalResource, json::write);
        delete(apiPrefix + "secure/feedversion/:id", FeedVersionController::deleteFeedVersion, json::write);

        get(apiPrefix + "public/feedversion", FeedVersionController::getAllFeedVersionsForFeedSource, json::write);
//        get(apiPrefix + "public/feedversion/:id/validation", FeedVersionController::getPublicValidationResult, json::write);
        get(apiPrefix + "public/feedversion/:id/downloadtoken", FeedVersionController::getFeedDownloadCredentials, json::write);

        get(apiPrefix + "downloadfeed/:token", FeedVersionController::downloadFeedVersionWithToken);

    }
}
