package com.conveyal.datatools.manager.models;

import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.editor.datastore.GlobalTx;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.jobs.NotifyUsersForSubscriptionJob;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.HashUtils;
import com.conveyal.gtfs.validator.ValidationResult;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;
import com.mongodb.client.model.Sorts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.conveyal.datatools.manager.utils.StringUtils.getCleanName;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;

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
    //@JsonView(JsonViews.DataDump.class)
    public String projectId;

//    public String[] regions = {"1"};
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

    // TODO: Add back in regions once they have been refactored
//    public List<Region> retrieveRegionList () {
//        return Region.retrieveAll().stream().filter(r -> Arrays.asList(regions).contains(r.id)).collect(Collectors.toList());
//    }

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

    public String publishedVersionId;

    /**
     * Create a new feed.
     */
    public FeedSource (String name) {
        super();
        this.name = name;
        this.retrievalMethod = FeedRetrievalMethod.MANUALLY_UPLOADED;
    }

    /**
     * No-arg constructor to yield an uninitialized feed source, for dump/restore.
     * Should not be used in general code.
     */
    public FeedSource () {
        this(null);
    }

    /**
     * Fetch the latest version of the feed.
     *
     * @return the fetched FeedVersion if a new version is available or null if nothing needs to be updated.
     */
    public FeedVersion fetch (MonitorableJob.Status status, String fetchUser) {
        status.message = "Downloading file";

        FeedVersion latest = retrieveLatest();

        // We create a new FeedVersion now, so that the fetched date is (milliseconds) before
        // fetch occurs. That way, in the highly unlikely event that a feed is updated while we're
        // fetching it, we will not miss a new feed.
        FeedVersion version = new FeedVersion(this);
        version.retrievalMethod = FeedRetrievalMethod.FETCHED_AUTOMATICALLY;

        // build the URL from which to fetch
        URL url = this.url;
        LOG.info("Fetching from {}", url.toString());

        // make the request, using the proper HTTP caching headers to prevent refetch, if applicable
        HttpURLConnection conn;
        try {
            conn = (HttpURLConnection) url.openConnection();
        } catch (Exception e) {
            String message = String.format("Unable to open connection to %s; not fetching feed %s", url, this.name);
            LOG.error(message);
            // TODO use this update function throughout this class
            status.update(true, message, 0);
            return null;
        }

        conn.setDefaultUseCaches(true);

        // lastFetched is set to null when the URL changes and when latest feed version is deleted
        if (latest != null && this.lastFetched != null)
            conn.setIfModifiedSince(Math.min(latest.updated.getTime(), this.lastFetched.getTime()));

        File newGtfsFile;

        try {
            conn.connect();
            String message;
            switch (conn.getResponseCode()) {
                case HttpURLConnection.HTTP_NOT_MODIFIED:
                    message = String.format("Feed %s has not been modified", this.name);
                    LOG.warn(message);
                    status.update(false, message, 100.0);
                    return null;
                case HttpURLConnection.HTTP_OK:
                case HttpURLConnection.HTTP_MOVED_TEMP:
                case HttpURLConnection.HTTP_MOVED_PERM:
                    message = String.format("Saving %s feed.", this.name);
                    LOG.info(message);
                    status.update(false, message, 75.0);
                    newGtfsFile = version.newGtfsFile(conn.getInputStream());
                    break;
                default:
                    message = String.format("HTTP status (%d: %s) retrieving %s feed", conn.getResponseCode(), conn.getResponseMessage(), this.name);
                    LOG.error(message);
                    status.update(true, message, 100.0);
                    return null;
            }
        } catch (IOException e) {
            String message = String.format("Unable to connect to %s; not fetching %s feed", url, this.name);
            LOG.error(message);
            status.update(true, message, 100.0);
            e.printStackTrace();
            return null;
        }

        // note that anything other than a new feed fetched successfully will have already returned from the function
//        version.hash();
        version.hash = HashUtils.hashFile(newGtfsFile);


        if (latest != null && version.hash.equals(latest.hash)) {
            // If new version hash equals the hash for the latest version, do not error. Simply indicate that server
            // operators should add If-Modified-Since support to avoid wasting bandwidth.
            String message = String.format("Feed %s was fetched but has not changed; server operators should add If-Modified-Since support to avoid wasting bandwidth", this.name);
            LOG.warn(message);
            newGtfsFile.delete();
            status.update(false, message, 100.0, true);
            return null;
        }
        else {
            version.userId = this.userId;

            // FIXME: Does this work?
            Persistence.feedSources.updateField(this.id, "lastFetched", version.updated);

            // Set file timestamp according to last modified header from connection
            version.fileTimestamp = conn.getLastModified();
            NotifyUsersForSubscriptionJob notifyFeedJob = new NotifyUsersForSubscriptionJob("feed-updated", this.id, "New feed version created for " + this.name);
            DataManager.lightExecutor.execute(notifyFeedJob);

            String message = String.format("Fetch complete for %s", this.name);
            LOG.info(message);
            status.update(false, message, 100.0);
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

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonView(JsonViews.UserInterface.class)
    @JsonProperty("latestVersionId")
    public String latestVersionId() {
        FeedVersion latest = retrieveLatest();
        return latest != null ? latest.id : null;
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
        ValidationResult result = latest != null ? latest.validationResult : null;
        return result != null ?new FeedValidationResultSummary(result, latest.feedLoadResult) : null;
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

            // FIXME: use mongo filters instead
            Persistence.externalFeedSourceProperties.getAll().stream()
                    .filter(prop -> prop.feedSourceId.equals(this.id))
                    .forEach(prop -> propTable.put(prop.name, prop.value));

            resourceTable.put(resourceType, propTable);
        }
        return resourceTable;
    }

    public static FeedSource retrieve(String id) {
//        return sourceStore.getById(id);
        return null;
    }

    public static Collection<FeedSource> retrieveAll() {
//        return sourceStore.getAll();
        return null;
    }

    /**
     * Get all of the feed versions for this source
     * @return collection of feed versions
     */
    @JsonIgnore
    public Collection<FeedVersion> retrieveFeedVersions() {
        return Persistence.feedVersions.getFiltered(eq("feedSourceId", this.id));
    }

    @JsonView(JsonViews.UserInterface.class)
    @JsonProperty("feedVersionCount")
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

    public void makePublic() {
        String sourceKey = FeedStore.s3Prefix + this.id + ".zip";
        String publicKey = toPublicKey();
        String versionId = this.latestVersionId();
        String latestVersionKey = FeedStore.s3Prefix + versionId;

        // only deploy to public if storing feeds on s3 (no mechanism for downloading/publishing
        // them otherwise)
        if (DataManager.useS3) {
            boolean sourceExists = FeedStore.s3Client.doesObjectExist(DataManager.feedBucket, sourceKey);
            ObjectMetadata sourceMetadata = sourceExists
                    ? FeedStore.s3Client.getObjectMetadata(DataManager.feedBucket, sourceKey)
                    : null;
            boolean latestExists = FeedStore.s3Client.doesObjectExist(DataManager.feedBucket, latestVersionKey);
            ObjectMetadata latestVersionMetadata = latestExists
                    ? FeedStore.s3Client.getObjectMetadata(DataManager.feedBucket, latestVersionKey)
                    : null;
            boolean latestVersionMatchesSource = sourceMetadata != null &&
                    latestVersionMetadata != null &&
                    sourceMetadata.getETag().equals(latestVersionMetadata.getETag());
            if (sourceExists && latestVersionMatchesSource) {
                LOG.info("copying feed {} to s3 public folder", this);
                FeedStore.s3Client.setObjectAcl(DataManager.feedBucket, sourceKey, CannedAccessControlList.PublicRead);
                FeedStore.s3Client.copyObject(DataManager.feedBucket, sourceKey, DataManager.feedBucket, publicKey);
                FeedStore.s3Client.setObjectAcl(DataManager.feedBucket, publicKey, CannedAccessControlList.PublicRead);
            } else {
                LOG.warn("Latest feed source {} on s3 at {} does not exist or does not match latest version. Using latest version instead.", this, sourceKey);
                if (FeedStore.s3Client.doesObjectExist(DataManager.feedBucket, latestVersionKey)) {
                    LOG.info("copying feed version {} to s3 public folder", versionId);
                    FeedStore.s3Client.setObjectAcl(DataManager.feedBucket, latestVersionKey, CannedAccessControlList.PublicRead);
                    FeedStore.s3Client.copyObject(DataManager.feedBucket, latestVersionKey, DataManager.feedBucket, publicKey);
                    FeedStore.s3Client.setObjectAcl(DataManager.feedBucket, publicKey, CannedAccessControlList.PublicRead);

                    // also copy latest version to feedStore latest
                    FeedStore.s3Client.copyObject(DataManager.feedBucket, latestVersionKey, DataManager.feedBucket, sourceKey);
                }
            }
        }
    }

    public void makePrivate() {
        String sourceKey = FeedStore.s3Prefix + this.id + ".zip";
        String publicKey = toPublicKey();
        if (FeedStore.s3Client.doesObjectExist(DataManager.feedBucket, sourceKey)) {
            LOG.info("removing feed {} from s3 public folder", this);
            FeedStore.s3Client.setObjectAcl(DataManager.feedBucket, sourceKey, CannedAccessControlList.AuthenticatedRead);
            FeedStore.s3Client.deleteObject(DataManager.feedBucket, publicKey);
        }
    }

    // TODO don't number the versions just timestamp them
    // FIXME for a brief moment feed version numbers are incoherent. Do this in a single operation or eliminate feed version numbers.
    public void renumberFeedVersions() {
        int i = 1;
        for (FeedVersion feedVersion : Persistence.feedVersions.getMongoCollection().find(eq("feedSourceId", this.id)).sort(Sorts.ascending("updated"))) {
            // Yes it's ugly to pass in a string, but we need to change the parameter type of update to take a Document.
            Persistence.feedVersions.update(feedVersion.id, String.format("{version:%d}", i));
            i += 1;
        }
    }

    /**
     * Represents ways feeds can be retrieved
     */
    public enum FeedRetrievalMethod {
        FETCHED_AUTOMATICALLY, // automatically retrieved over HTTP on some regular basis
        MANUALLY_UPLOADED, // manually uploaded by someone, perhaps the agency, or perhaps an internal user
        PRODUCED_IN_HOUSE // produced in-house in a GTFS Editor instance
    }

    /**
     * Delete this feed source and everything that it contains.
     */
    public void delete() {
        retrieveFeedVersions().forEach(FeedVersion::delete);

        // delete latest copy of feed source
        if (DataManager.useS3) {
            DeleteObjectsRequest delete = new DeleteObjectsRequest(DataManager.feedBucket);
            delete.withKeys("public/" + this.name + ".zip", FeedStore.s3Prefix + this.id + ".zip");
            FeedStore.s3Client.deleteObjects(delete);
        }

        // Delete editor feed mapdb
        // TODO: does the mapdb folder need to be deleted separately?
        GlobalTx gtx = VersionedDataStore.getGlobalTx();
        if (!gtx.feeds.containsKey(id)) {
            gtx.rollback();
        }
        else {
            gtx.feeds.remove(id);
            gtx.commit();
        }

        // FIXME use Mongo filters instead
        Persistence.externalFeedSourceProperties.getAll().stream()
                .filter(prop -> prop.feedSourceId.equals(this.id))
                .forEach(prop -> Persistence.externalFeedSourceProperties.removeById(prop.id));

        // TODO: add delete for osm extract and r5 network (maybe that goes with version)
        Persistence.feedSources.removeById(this.id);
    }

    public FeedSource clone () throws CloneNotSupportedException {
        return (FeedSource) super.clone();
    }

}
