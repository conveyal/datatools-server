package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.FeedVersionJob;
import com.conveyal.datatools.editor.jobs.CreateSnapshotJob;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedRetrievalMethod;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Snapshot;
import com.conveyal.datatools.manager.models.transform.DbTransformation;
import com.conveyal.datatools.manager.models.transform.FeedTransformDbTarget;
import com.conveyal.datatools.manager.models.transform.FeedTransformRules;
import com.conveyal.datatools.manager.models.transform.FeedTransformZipTarget;
import com.conveyal.datatools.manager.models.transform.ZipTransformation;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

/**
 * Process/validate a single GTFS feed. This job is called once a GTFS file had been uploaded or fetched and is ready to
 * be loaded into the GTFS database. This job acts as a parent job that chains together multiple server jobs. Loading
 * the feed and validating the feed are a part of this chain unconditionally. However, depending on which modules are
 * enabled, other jobs may be included here if desired.
 *
 * @author mattwigway
 */
public class ProcessSingleFeedJob extends FeedVersionJob {
    private final FeedVersion feedVersion;
    private final boolean isNewVersion;
    private static final Logger LOG = LoggerFactory.getLogger(ProcessSingleFeedJob.class);
    private final FeedSource feedSource;

    /**
     * Create a job for the given feed version.
     */
    public ProcessSingleFeedJob(FeedVersion feedVersion, Auth0UserProfile owner, boolean isNewVersion) {
        super(owner, "Processing GTFS for " + (feedVersion.parentFeedSource() != null ? feedVersion.parentFeedSource().name : "unknown feed source"), JobType.PROCESS_FEED);
        this.feedVersion = feedVersion;
        this.feedSource = feedVersion.parentFeedSource();
        this.isNewVersion = isNewVersion;
        status.update("Waiting...", 0);
        status.uploading = true;
    }

    /**
     * Getter that allows a client to know the ID of the feed version that will be created as soon as the upload is
     * initiated; however, we will not store the FeedVersion in the mongo application database until the upload and
     * processing is completed. This prevents clients from manipulating GTFS data before it is entirely imported.
     */
    @JsonProperty
    public String getFeedVersionId() {
        return feedVersion.id;
    }

    @JsonProperty
    public String getFeedSourceId() {
        return feedSource.id;
    }

    /**
     * The primary logic in this job handles loading (into Postgres) and validating the incoming GTFS file. However,
     * there are important secondary functions that run {@link ArbitraryTransformJob} to modify either the input GTFS
     * zip file or the resulting database representation.
     *
     * There are a few different transformation possibilities:
     * 1. We only have zip transformations. These can either apply directly to the imported zip
     *    or they can apply to a cloned version of the import. In the second case, the retrieval method should be set to
     *    VERSION_CLONE (otherwise, it can be set to any other combination of retrieval methods).
     * 2. We only have db transformations. These will always apply to a cloned snapshot of a feed version to
     *    prevent the feed version's namespace from having different contents from the zip file. Following this, the
     *    snapshot can either end up hanging (i.e., not published) or once all of the transformations have been
     *    applied, we can create a new feed version from the snapshot (retrieval method being produced in house??).
     * 3. Let's say we have zip transformations and db transformations.
     *    a. Zip transformations apply to imported version, db transformations apply to new snapshot or get published as
     *       additional feed version (no big deal).
     *    b. Zip transformations apply to cloned version, db transformations apply to snapshot or get published.
     *       This case is a little trickier, because we could end up with two cloned_versions. Let's say we want
     *       a SQL query to run on the original data, create a new GTFS file, and then replace a GTFS+ file from some
     *       other version.
     */
    @Override
    public void jobLogic() {
        LOG.info("Processing feed for {}", feedVersion.id);
        FeedTransformRules rules = feedSource.getRulesForRetrievalMethod(feedVersion.retrievalMethod);
        boolean shouldTransform = rules != null;
        if (shouldTransform) {
            // Run zip transformations before load to handle any operations that must be applied directly to the zip file.
            List<ZipTransformation> zipTransformations = rules.getActiveTransformations(feedVersion, ZipTransformation.class);
            FeedTransformZipTarget zipTarget = new FeedTransformZipTarget(feedVersion.retrieveGtfsFile());
            for (ZipTransformation transformation : zipTransformations) {
                ArbitraryTransformJob zipTransform = new ArbitraryTransformJob(owner, zipTarget, transformation);
                // Run transform job in line so we can monitor the error status before load/validate begins.
                zipTransform.run();
                // Short circuit the feed load/validate if a pre-load transform fails.
                if (zipTransform.status.error) return;
            }
            // Assign transform result from zip target.
            feedVersion.feedTransformResult = zipTarget.feedTransformResult;
        }

        // First, load the feed into database. During this stage, the GTFS file will be uploaded to S3 (and deleted locally).
        addNextJob(new LoadFeedJob(feedVersion, owner, isNewVersion));

        // Next, validate the feed.
        addNextJob(new ValidateFeedJob(feedVersion, owner, isNewVersion));

        // We only need to snapshot the feed if there are transformations at the database level. In the case that there
        // are, the snapshot namespace will be the target of these modifications. If we were to apply the modifications
        // to the original feed version's namespace, the namespace and the zip file would become out of sync, which
        // violates a core expectation for this application: that each zip file represents exactly its database
        // representation.
        boolean shouldSnapshot = shouldTransform && rules.hasTransformationsOfType(feedVersion, DbTransformation.class);
        if (shouldSnapshot) {
            Snapshot snapshot = new Snapshot("Transform of " + feedVersion.name, feedVersion);
            addNextJob(new CreateSnapshotJob(owner, snapshot));
            // Apply post-load transformations to snapshotted feed version. Post-load transformations will modify only the
            // snapshot (not the original feed version's namespace), so the snapshot must be published (or loaded into the
            // editor) in order to see the results.
            FeedTransformDbTarget dbTarget = new FeedTransformDbTarget(snapshot.id);
            for (DbTransformation transformation : rules.getActiveTransformations(feedVersion, DbTransformation.class)) {
                addNextJob(new ArbitraryTransformJob(owner, dbTarget, transformation));
            }
            // Assign transform result from db target.
            snapshot.feedTransformResult = dbTarget.feedTransformResult;
            // If the user has selected to create a new version from the resulting snapshot, do so here.
            if (rules.createNewVersion) {
                addNextJob(new CreateFeedVersionFromSnapshotJob(feedSource, snapshot, owner));
            }
        }

        // If deployment module is enabled, the feed source is deployable and the project can be auto deployed at
        // this stage, create an auto deploy job. Note: other checks occur within job to ensure appropriate
        // conditions are met.
        Set<AutoDeployType> projectAutoDeployTypes = feedSource.retrieveProject().autoDeployTypes;
        if (
            DataManager.isModuleEnabled("deployment") &&
                feedSource.deployable &&
                (
                    projectAutoDeployTypes.contains(AutoDeployType.ON_PROCESS_FEED) ||
                        (
                            feedVersion.retrievalMethod == FeedRetrievalMethod.FETCHED_AUTOMATICALLY &&
                                projectAutoDeployTypes.contains(AutoDeployType.ON_FEED_FETCH)
                        )
                )
        ) {
            addNextJob(new AutoDeployJob(feedSource.retrieveProject(), owner));
        }
    }

    /**
     * Once the job is complete, notify subscribers and provide feedback on the job creation status.
     */
    @Override
    public void jobFinished() {
        if (!status.error) {
            status.completeSuccessfully("New version saved.");
        } else {
            LOG.warn("Error processing version {} because of {}.", feedVersion.id, getErrorReasonMessage());
        }
        // Send notification to those subscribed to feed version updates.
        NotifyUsersForSubscriptionJob.createNotification(
            "feed-updated",
            feedSource.id,
            this.getNotificationMessage()
        );
    }

    /**
     * Create error reason message based on job status.
     */
    private String getErrorReasonMessage() {
        return status.exceptionType != null
            ? String.format("error due to %s", status.exceptionType)
            : "unknown error";
    }

    /**
     * Create message for new feed version notification based on job status and validation result.
     */
    @JsonIgnore
    public String getNotificationMessage() {
        StringBuilder message = new StringBuilder();
        if (!status.error) {
            message.append(String.format("New feed version created for %s (valid from %s - %s). ",
                feedSource.name,
                feedVersion.validationResult.firstCalendarDate,
                feedVersion.validationResult.lastCalendarDate));
            if (feedVersion.validationResult.errorCount > 0) {
                message.append(String.format("During validation, we found %s issue(s)",
                    feedVersion.validationResult.errorCount));
            } else {
                message.append("The validation check found no issues with this new dataset!");
            }
        } else {
            // Processing did not complete. Depending on which sub-task this occurred in,
            // there may or may not have been a successful load/validation of the feed.
            message.append(
                String.format(
                    "While attempting to process a new feed version for %s, an unrecoverable error was encountered. More details: %s",
                    feedSource.name,
                    getErrorReasonMessage()
                )
            );
        }
        return message.toString();
    }

}
