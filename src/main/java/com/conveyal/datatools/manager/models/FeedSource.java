package com.conveyal.datatools.manager.models;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.conveyal.datatools.common.status.FeedSourceJob;
import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.common.utils.Scheduler;
import com.conveyal.datatools.common.utils.aws.CheckedAWSException;
import com.conveyal.datatools.common.utils.aws.S3Utils;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.jobs.CreateFeedVersionFromSnapshotJob;
import com.conveyal.datatools.manager.jobs.FetchSingleFeedJob;
import com.conveyal.datatools.manager.jobs.MergeFeedsJob;
import com.conveyal.datatools.manager.jobs.ProcessSingleFeedJob;
import com.conveyal.datatools.manager.models.transform.FeedTransformRules;
import com.conveyal.datatools.manager.models.transform.FeedTransformation;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.serializers.LabelSerializer;
import com.conveyal.datatools.manager.utils.JobUtils;
import com.conveyal.gtfs.GTFS;
import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Sorts;
import org.bson.codecs.pojo.annotations.BsonIgnore;
import org.bson.codecs.pojo.annotations.BsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.conveyal.datatools.manager.utils.StringUtils.getCleanName;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

/**
 * Created by demory on 3/22/16.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FeedSource extends Model implements Cloneable {

    private static final long serialVersionUID = 1L;

    public static final Logger LOG = LoggerFactory.getLogger(FeedSource.class);

    /**
     * The collection of which this feed is a part
     */
    public String projectId;

    /**
     * When snapshotting a GTFS feed for editing, gtfs-lib currently defaults to normalize stop sequence values to be
     * zero-based and incrementing. This can muck with GTFS files that are linked to GTFS-rt feeds by stop_sequence, so
     * this override flag currently provides a workaround for feeds that need to be edited but do not need to edit
     * stop_times or individual patterns. WARNING: enabling this flag for a feed and then attempting to edit patterns in
     * complicated ways (e.g., modifying the order of pattern stops) could have unexpected consequences. There is no UI
     * setting for this and it is not recommended to do this unless absolutely necessary.
     */
    public boolean preserveStopTimesSequence;

    /**
     * Get the Project of which this feed is a part
     */
    public Project retrieveProject() {
        return projectId != null ? Persistence.projects.getById(projectId) : null;
    }

    @JsonProperty("organizationId")
    public String organizationId () {
        Project project = retrieveProject();
        return project == null ? null : project.organizationId;
    }

    public List<FeedTransformRules> transformRules = new ArrayList<>();

    /** The name of this feed source, e.g. MTA New York City Subway */
    public String name;

    /** Is this feed public, i.e. should it be listed on the
     * public feeds page for download?
     */
    public boolean isPublic;

    /** Is this feed deployable? */
    public boolean deployable;

    /**
     * How do we receive this feed?
     */
    public FeedRetrievalMethod retrievalMethod;

    /**
     * How frequently should we fetch this feed (at the feed's {@link #url} if {@link #retrievalMethod} is
     * {@link FeedRetrievalMethod#FETCHED_AUTOMATICALLY}). Daily defaults to using time defined in
     * {@link Project#autoFetchHour}:{@link Project#autoFetchMinute}.
     */
    public FetchFrequency fetchFrequency;
    /**
     * The fetch interval taken into account with {@link #fetchFrequency} defines the interval at which a feed will be
     * automatically checked for updates. E.g., 1 DAYS will trigger a fetch every day, 5 MINUTES will trigger a fetch
     * every five minutes.
     */
    public int fetchInterval;

    /**
     * When was this feed last fetched?
     */
    public Date lastFetched;

    /**
     * When was this feed last updated?
     * FIXME: this is currently dynamically determined by lastUpdated() with calls retrieveLatest().
     */
//    public transient Date lastUpdated;

    /**
     * From whence is this feed fetched?
     */
    public URL url;

    /**
     * Where the feed exists on s3
     */
    public URL s3Url;

    /**
     * What is the GTFS Editor snapshot for this feed?
     *
     * This is the String-formatted snapshot ID, which is the base64-encoded ID and the version number.
     */
    public String snapshotVersion;

    // Using a custom serializer, the input can be an ID of a label, while the output will be the
    // entire label object. This also allows storing labels as IDs.
    @JsonSerialize(using = LabelSerializer.class)
    public List<String> labels = new ArrayList<>();

    /**
     * The SQL namespace for the most recently verified published {@link FeedVersion}.
     *
     * FIXME During migration to RDBMS for GTFS data, this field changed to map to the SQL unique ID,
     *  however the name of the field suggests it maps to the feed version ID stored in MongoDB. Now
     *  that both published namespace/version ID are available in {@link #publishedValidationSummary()}
     *  it might make sense to migrate this field back to the versionID for MTC (or rename it to
     *  publishedNamespace). Both efforts would require some level of db migration + code changes.
     */
    public String publishedVersionId;

    public String editorNamespace;

    /**
     * Create a new feed.
     */
    public FeedSource (String name) {
        super();
        this.name = name;
        this.retrievalMethod = FeedRetrievalMethod.MANUALLY_UPLOADED;
    }

    public FeedSource (String name, String projectId, FeedRetrievalMethod retrievalMethod) {
        super();
        this.name = name;
        this.projectId = projectId;
        this.retrievalMethod = retrievalMethod;
    }

    /**
     * No-arg constructor to yield an uninitialized feed source, for dump/restore.
     * Should not be used in general code.
     */
    public FeedSource () {
        this(null);
    }


    public FeedVersion fetch (MonitorableJob.Status status) {
        return fetch(status, null);
    }
    /**
     * Fetch the latest version of the feed. Optionally provide an override URL from which to fetch the feed. This
     * optional URL is used for a one-level deep recursive call of fetch when a redirect is encountered.
     *
     * FIXME: Should the FeedSource fetch URL field be updated if a recursive call with new URL is successful?
     *
     * @return the fetched FeedVersion if a new version is available or null if nothing needs to be updated.
     */
    public FeedVersion fetch (MonitorableJob.Status status, String optionalUrlOverride) {
        status.message = "Downloading file";

        // We create a new FeedVersion now, so that the fetched date is (milliseconds) before
        // fetch occurs. That way, in the highly unlikely event that a feed is updated while we're
        // fetching it, we will not miss a new feed.
        FeedVersion version = new FeedVersion(this, FeedRetrievalMethod.FETCHED_AUTOMATICALLY);

        // build the URL from which to fetch
        URL url = null;
        try {
            // If an optional URL is provided (in the case of a recursive fetch) use that. Otherwise, use the fetch URL
            url = optionalUrlOverride != null ? new URL(optionalUrlOverride) : this.url;
        } catch (MalformedURLException e) {
            e.printStackTrace();
            status.fail(String.format("Could not connect to bad redirect URL %s", optionalUrlOverride));
        }
        LOG.info("Fetching from {}", url.toString());

        // make the request, using the proper HTTP caching headers to prevent refetch, if applicable
        HttpURLConnection conn;
        try {
            conn = (HttpURLConnection) url.openConnection();
            // Set user agent request header in order to avoid 403 Forbidden response from some servers.
            // https://stackoverflow.com/questions/13670692/403-forbidden-with-java-but-not-web-browser
            conn.setRequestProperty(
                    "User-Agent",
                    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.95 Safari/537.11"
            );
        } catch (IOException e) {
            status.fail(String.format("Unable to open connection to %s; not fetching feed %s", url, this.name), e);
            return null;
        }

        conn.setDefaultUseCaches(true);
        // Get latest version to check that the fetched version does not duplicate a feed already loaded.
        FeedVersion latest = retrieveLatest();
        // lastFetched is set to null when the URL changes and when latest feed version is deleted
        if (latest != null && this.lastFetched != null)
            conn.setIfModifiedSince(Math.min(latest.updated.getTime(), this.lastFetched.getTime()));

        File newGtfsFile;

        try {
            conn.connect();
            String message;
            int responseCode = conn.getResponseCode();
            LOG.info("Fetch feed response code={}", responseCode);
            switch (responseCode) {
                case HttpURLConnection.HTTP_NOT_MODIFIED:
                    message = String.format("Feed %s has not been modified", this.name);
                    LOG.warn(message);
                    status.completeSuccessfully(message);
                    return null;
                case HttpURLConnection.HTTP_OK:
                    // Response is OK. Continue on to save the GTFS file.
                    message = String.format("Saving %s feed.", this.name);
                    LOG.info(message);
                    status.update(message, 75.0);
                    // Create new file from input stream (this also handles hashing the file and other version fields
                    // calculated from the GTFS file.
                    newGtfsFile = version.newGtfsFile(conn.getInputStream());
                    break;
                case HttpURLConnection.HTTP_MOVED_TEMP:
                case HttpURLConnection.HTTP_MOVED_PERM:
                case HttpURLConnection.HTTP_SEE_OTHER:
                    // Get redirect url from "location" header field
                    String newUrl = conn.getHeaderField("Location");
                    if (optionalUrlOverride != null) {
                        // Only permit recursion one level deep. If more than one redirect is detected, fail the job and
                        // suggest that user try again with new URL.
                        message = String.format("More than one redirects for fetch URL detected. Please try fetch again with latest URL: %s", newUrl);
                        LOG.error(message);
                        status.fail(message);
                        return null;
                    } else {
                        // If override URL is null, this is the zeroth fetch. Recursively call fetch, but only one time
                        // to prevent multiple (possibly infinite?) redirects. Any more redirects than one should
                        // probably be met with user action to update the fetch URL.
                        LOG.info("Recursively calling fetch feed with new URL: {}", newUrl);
                        return fetch(status, newUrl);
                    }
                default:
                    // Any other HTTP codes result in failure.
                    // FIXME Are there "success" codes we're not accounting for?
                    message = String.format("HTTP status (%d: %s) retrieving %s feed", responseCode, conn.getResponseMessage(), this.name);
                    LOG.error(message);
                    status.fail(message);
                    return null;
            }
        } catch (IOException e) {
            String message = String.format("Unable to connect to %s; not fetching %s feed", url, this.name);
            LOG.error(message);
            status.fail(message);
            e.printStackTrace();
            return null;
        }
        if (latest != null && version.hash.equals(latest.hash)) {
            // If new version hash equals the hash for the latest version, do not error. Simply indicate that server
            // operators should add If-Modified-Since support to avoid wasting bandwidth.
            String message = String.format("Feed %s was fetched but has not changed; server operators should add If-Modified-Since support to avoid wasting bandwidth", this.name);
            LOG.warn(message);
            String filePath = newGtfsFile.getAbsolutePath();
            if (newGtfsFile.delete()) {
                LOG.info("Deleting redundant GTFS file: {}", filePath);
            } else {
                LOG.warn("Failed to delete unneeded GTFS file at: {}", filePath);
            }
            status.completeSuccessfully(message);
            return null;
        } else {
            version.userId = this.userId;

            // Update last fetched value for feed source.
            Persistence.feedSources.updateField(this.id, "lastFetched", version.updated);

            // Set file timestamp according to last modified header from connection
            version.fileTimestamp = conn.getLastModified();
            String message = String.format("Fetch complete for %s", this.name);
            LOG.info(message);
            status.completeSuccessfully(message);
            return version;
        }
    }

    public int compareTo(FeedSource o) {
        return this.name.compareTo(o.name);
    }

    public String toString () {
        return "<FeedSource " + this.name + " (" + this.id + ")>";
    }

    /**
     * Get the latest version of this feed
     * @return the latest version of this feed
     */
    @JsonIgnore
    public FeedVersion retrieveLatest() {
        FeedVersion newestVersion = Persistence.feedVersions
                .getOneFiltered(eq("feedSourceId", this.id), Sorts.descending("version"));
        if (newestVersion == null) {
            // Is this what happens if there are none?
            return null;
        }
        return newestVersion;
    }

    /**
     * Fetches the published {@link FeedVersion} for this feed source according to the
     * {@link #publishedVersionId} field (which currently maps to {@link FeedVersion#namespace}.
     */
    public FeedVersion retrievePublishedVersion() {
        if (this.publishedVersionId == null) return null;
        FeedVersion publishedVersion = Persistence.feedVersions
            // Sort is unnecessary here.
            .getOneFiltered(eq("namespace", this.publishedVersionId), Sorts.descending("version"));
        if (publishedVersion == null) {
            // Is this what happens if there are none?
            return null;
        }
        return publishedVersion;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonView(JsonViews.UserInterface.class)
    @JsonProperty("publishedValidationSummary")
    private FeedValidationResultSummary publishedValidationSummary() {
        FeedVersion publishedVersion = retrievePublishedVersion();
        return publishedVersion != null ? new FeedValidationResultSummary(publishedVersion) : null;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonView(JsonViews.UserInterface.class)
    @JsonProperty("latestVersionId")
    public String latestVersionId() {
        FeedVersion latest = retrieveLatest();
        return latest != null ? latest.id : null;
    }

    /**
     * Number of {@link FeedVersion}s that exist for the feed source.
     */
    @BsonIgnore
    public long getVersionCount() {
        return Persistence.feedVersions.count(eq("feedSourceId", this.id));
    }

    /**
     * We can't pass the entire latest feed version back, because it contains references back to this feedsource,
     * so Jackson doesn't work. So instead we specifically expose the validation results and the latest update.
     * @return
     */
    // TODO: use summarized feed source here. requires serious refactoring on client side.
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonView(JsonViews.UserInterface.class)
    @JsonProperty("lastUpdated")
    public Date lastUpdated() {
        FeedVersion latest = retrieveLatest();
        return latest != null ? latest.updated : null;
    }


    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonView(JsonViews.UserInterface.class)
    @JsonProperty("latestValidation")
    public FeedValidationResultSummary latestValidation() {
        FeedVersion latest = retrieveLatest();
        return latest != null ? new FeedValidationResultSummary(latest) : null;
    }

    // TODO: figure out some way to indicate whether feed has been edited since last snapshot (i.e, there exist changes)
//    @JsonInclude(JsonInclude.Include.NON_NULL)
//    @JsonView(JsonViews.UserInterface.class)
//    public boolean getEditedSinceSnapshot() {
////        FeedTx tx;
////        try {
////            tx = VersionedDataStore.getFeedTx(id);
////        } catch (Exception e) {
////
////        }
////        return tx.editedSinceSnapshot.retrieveById();
//        return false;
//    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonView(JsonViews.UserInterface.class)
    @JsonProperty("externalProperties")
    public Map<String, Map<String, String>> externalProperties() {

        Map<String, Map<String, String>> resourceTable = new HashMap<>();

        for(String resourceType : DataManager.feedResources.keySet()) {
            Map<String, String> propTable = new HashMap<>();

            // Get all external properties for the feed source/resource type and fill prop table.
            Persistence.externalFeedSourceProperties
                .getFiltered(and(eq("feedSourceId", this.id), eq("resourceType", resourceType)))
                .forEach(prop -> propTable.put(prop.name, prop.value));

            resourceTable.put(resourceType, propTable);
        }
        return resourceTable;
    }

    /**
     * Get all of the feed versions for this source
     * @return collection of feed versions
     */
    @JsonIgnore
    public Collection<FeedVersion> retrieveFeedVersions() {
        return Persistence.feedVersions.getFiltered(eq("feedSourceId", this.id));
    }

    /**
     * Get all of the snapshots for this source
     * @return collection of snapshots
     */
    @JsonIgnore
    public Collection<Snapshot> retrieveSnapshots() {
        return Persistence.snapshots.getFiltered(eq(Snapshot.FEED_SOURCE_REF, this.id));
    }

    /**
     * Get all of the test deployments for this feed source.
     * @return collection of deloyments
     */
    @JsonIgnore
    public Collection<Deployment> retrieveDeployments () {
        return Persistence.deployments.getFiltered(eq(Snapshot.FEED_SOURCE_REF, this.id));
    }

//    @JsonView(JsonViews.UserInterface.class)
//    @JsonProperty("feedVersionCount")
    public int feedVersionCount() {
        return retrieveFeedVersions().size();
    }

    @JsonView(JsonViews.UserInterface.class)
    @JsonProperty("noteCount")
    public int noteCount() {
        return this.noteIds != null ? this.noteIds.size() : 0;
    }

    public String toPublicKey() {
        return "public/" + getCleanName(this.name) + ".zip";
    }

    /**
     * Makes the feed source's latest version have public access on AWS S3.
     */
    public void makePublic() throws CheckedAWSException {
        String sourceKey = S3Utils.DEFAULT_BUCKET_GTFS_FOLDER + this.id + ".zip";
        String publicKey = toPublicKey();
        String versionId = this.latestVersionId();
        String latestVersionKey = S3Utils.DEFAULT_BUCKET_GTFS_FOLDER + versionId;

        // only deploy to public if storing feeds on s3 (no mechanism for downloading/publishing
        // them otherwise)
        if (DataManager.useS3) {
            AmazonS3 defaultS3Client = S3Utils.getDefaultS3Client();
            boolean sourceExists = defaultS3Client.doesObjectExist(S3Utils.DEFAULT_BUCKET, sourceKey);
            ObjectMetadata sourceMetadata = sourceExists
                    ? defaultS3Client.getObjectMetadata(S3Utils.DEFAULT_BUCKET, sourceKey)
                    : null;
            boolean latestExists = defaultS3Client.doesObjectExist(S3Utils.DEFAULT_BUCKET, latestVersionKey);
            ObjectMetadata latestVersionMetadata = latestExists
                    ? defaultS3Client.getObjectMetadata(S3Utils.DEFAULT_BUCKET, latestVersionKey)
                    : null;
            boolean latestVersionMatchesSource = sourceMetadata != null &&
                    latestVersionMetadata != null &&
                    sourceMetadata.getETag().equals(latestVersionMetadata.getETag());
            if (sourceExists && latestVersionMatchesSource) {
                LOG.info("copying feed {} to s3 public folder", this);
                defaultS3Client.setObjectAcl(S3Utils.DEFAULT_BUCKET, sourceKey, CannedAccessControlList.PublicRead);
                defaultS3Client.copyObject(S3Utils.DEFAULT_BUCKET, sourceKey, S3Utils.DEFAULT_BUCKET, publicKey);
                defaultS3Client.setObjectAcl(S3Utils.DEFAULT_BUCKET, publicKey, CannedAccessControlList.PublicRead);
            } else {
                LOG.warn("Latest feed source {} on s3 at {} does not exist or does not match latest version. Using latest version instead.", this, sourceKey);
                if (defaultS3Client.doesObjectExist(S3Utils.DEFAULT_BUCKET, latestVersionKey)) {
                    LOG.info("copying feed version {} to s3 public folder", versionId);
                    defaultS3Client.setObjectAcl(S3Utils.DEFAULT_BUCKET, latestVersionKey, CannedAccessControlList.PublicRead);
                    defaultS3Client.copyObject(S3Utils.DEFAULT_BUCKET, latestVersionKey, S3Utils.DEFAULT_BUCKET, publicKey);
                    defaultS3Client.setObjectAcl(S3Utils.DEFAULT_BUCKET, publicKey, CannedAccessControlList.PublicRead);

                    // also copy latest version to feedStore latest
                    defaultS3Client.copyObject(S3Utils.DEFAULT_BUCKET, latestVersionKey, S3Utils.DEFAULT_BUCKET, sourceKey);
                }
            }
        }
    }

    /**
     * Makes the feed source's latest version have private access on AWS S3.
     */
    public void makePrivate() throws CheckedAWSException {
        String sourceKey = S3Utils.DEFAULT_BUCKET_GTFS_FOLDER + this.id + ".zip";
        String publicKey = toPublicKey();
        AmazonS3 defaultS3Client = S3Utils.getDefaultS3Client();
        if (defaultS3Client.doesObjectExist(S3Utils.DEFAULT_BUCKET, sourceKey)) {
            LOG.info("removing feed {} from s3 public folder", this);
            defaultS3Client.setObjectAcl(S3Utils.DEFAULT_BUCKET, sourceKey, CannedAccessControlList.AuthenticatedRead);
            defaultS3Client.deleteObject(S3Utils.DEFAULT_BUCKET, publicKey);
        }
    }

    // TODO don't number the versions just timestamp them
    // FIXME for a brief moment feed version numbers are incoherent. Do this in a single operation or eliminate feed version numbers.
    public void renumberFeedVersions() {
        int i = 1;
        FindIterable<FeedVersion> orderedFeedVersions = Persistence.feedVersions.getMongoCollection()
                .find(eq("feedSourceId", this.id))
                .sort(Sorts.ascending("updated"));
        for (FeedVersion feedVersion : orderedFeedVersions) {
            // Yes it's ugly to pass in a string, but we need to change the parameter type of update to take a Document.
            Persistence.feedVersions.update(feedVersion.id, String.format("{version:%d}", i));
            i += 1;
        }
    }

    // TODO don't number the snapshots just timestamp them
    // FIXME for a brief moment snapshot numbers are incoherent. Do this in a single operation or eliminate snapshot version numbers.
    public void renumberSnapshots() {
        int i = 1;
        FindIterable<Snapshot> orderedSnapshots = Persistence.snapshots.getMongoCollection()
                .find(eq(Snapshot.FEED_SOURCE_REF, this.id))
                .sort(Sorts.ascending("snapshotTime"));
        for (Snapshot snapshot : orderedSnapshots) {
            // Yes it's ugly to pass in a string, but we need to change the parameter type of update to take a Document.
            Persistence.snapshots.updateField(snapshot.id, "version", i);
            i += 1;
        }
    }

    /**
     * Delete this feed source and everything that it contains.
     *
     * FIXME: Use a Mongo transaction to handle the deletion of these related objects.
     */
    public void delete() {
        try {
            // Remove all feed version records for this feed source
            retrieveFeedVersions().forEach(FeedVersion::delete);
            // Remove all snapshot records for this feed source
            retrieveSnapshots().forEach(Snapshot::delete);
            // Remove any scheduled job for feed source.
            Scheduler.removeAllFeedSourceJobs(this.id, true);
            // Delete active editor buffer if exists.
            if (this.editorNamespace != null) {
                GTFS.delete(this.editorNamespace, DataManager.GTFS_DATA_SOURCE);
            }
            // Delete latest copy of feed source on S3.
            if (DataManager.useS3) {
                AmazonS3 defaultS3Client = S3Utils.getDefaultS3Client();
                DeleteObjectsRequest delete = new DeleteObjectsRequest(S3Utils.DEFAULT_BUCKET);
                delete.withKeys("public/" + this.name + ".zip", S3Utils.DEFAULT_BUCKET_GTFS_FOLDER + this.id + ".zip");
                defaultS3Client.deleteObjects(delete);
            }
            // Remove all external properties for this feed source.
            Persistence.externalFeedSourceProperties.removeFiltered(eq("feedSourceId", this.id));

            // FIXME: Should this delete related feed versions from the SQL database (for both published versions and
            // editor snapshots)?

            // Finally, delete the feed source mongo document.
            Persistence.feedSources.removeById(this.id);
        } catch (Exception e) {
            LOG.error("Could not delete feed source", e);
        }
    }

    public FeedSource clone () throws CloneNotSupportedException {
        return (FeedSource) super.clone();
    }

    /**
     * Check if there are active jobs to fetch or process a new version for this feed source. This is helpful in the
     * context of auto-deploying to OTP to determine if there are any jobs occurring that could result in a new feed
     * version being created (we want to throttle auto-deployments if multiple jobs to create new feed versions are
     * occurring at the same time).
     */
    public boolean hasJobsInProgress() {
        return JobUtils.getAllActiveJobs().stream().anyMatch(job -> {
            String jobFeedSourceId = null;
            if (
                job instanceof FetchSingleFeedJob ||
                job instanceof ProcessSingleFeedJob ||
                job instanceof CreateFeedVersionFromSnapshotJob ||
                job instanceof MergeFeedsJob
            ) {
                jobFeedSourceId = ((FeedSourceJob) job).getFeedSourceId();
            }
            return this.id.equals(jobFeedSourceId);
        });
    }

    public <T extends FeedTransformation> boolean hasTransformationsOfType(FeedVersion target, Class<T> clazz) {
        return getActiveTransformations(target, clazz).size() > 0;
    }

    public boolean hasRulesForRetrievalMethod(FeedRetrievalMethod retrievalMethod) {
        return getRulesForRetrievalMethod(retrievalMethod) != null;
    }

    /**
     * Get transform rules for the retrieval method or null if none exist. Note: this will return the first rule set
     * found. There should not be multiple rule sets for a given retrieval method.
     */
    public FeedTransformRules getRulesForRetrievalMethod(FeedRetrievalMethod retrievalMethod) {
        return transformRules.stream()
            .filter(feedTransformRules -> feedTransformRules.hasRetrievalMethod(retrievalMethod))
            .findFirst()
            .orElse(null);
    }

    public <T extends FeedTransformation> List<T> getActiveTransformations(FeedVersion target, Class<T> clazz) {
        return transformRules.stream()
            .filter(FeedTransformRules::isActive)
            .map(rule -> rule.getActiveTransformations(target, clazz))
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
    }
}
