package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.manager.jobs.AutoDeployType;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.or;

/**
 * Represents a collection of feed sources that can be made into a deployment.
 * Generally, this would represent one agency that is managing the data.
 * For now, there is one Project per instance of GTFS data manager, but
 * we're trying to write the code in such a way that this is not necessary.
 *
 * @author mattwigway
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Project extends Model {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(Project.class);

    /** The name of this project, e.g. NYSDOT. */
    public String name;

    public boolean useCustomOsmBounds;

    public OtpBuildConfig buildConfig;

    public OtpRouterConfig routerConfig;

    public String organizationId;

    /** Last successful auto deploy. **/
    public Date lastAutoDeploy;

    /**
     * A list of servers that are available to deploy project feeds/OSM to. This includes servers assigned to this
     * project as well as those that belong to no project.
     * @return
     */
    public List<OtpServer> availableOtpServers() {
        return Persistence.servers.getFiltered(or(
            eq("projectId", this.id),
            eq("projectId", null)
        ));
    }

    public String defaultTimeZone;
    public boolean autoFetchFeeds;
    public int autoFetchHour, autoFetchMinute;

    public transient Collection<FeedSource> feedSources;

    // Bounds is used for either OSM custom deployment bounds (if useCustomOsmBounds is true)
    // and/or for applying a geographic filter when syncing with external feed registries.
    public Bounds bounds;

    /**
     * Defines when the {@link #pinnedDeploymentId} should be auto-deployed.
     */
    public Set<AutoDeployType> autoDeployTypes = new HashSet<>();

    /**
     * Whether to auto-deploy feeds that have critical errors.
     */
    public boolean autoDeployWithCriticalErrors = false;

    // Identifies a specific "pinned" deployment for the project. This is used in datatools-ui in 2 places:
    // 1. In the list of project deployments, a "pinned" deployment is shown first and highlighted.
    // 2. In the project feed source table, if a "pinned" deployment exists, the status of the versions that were in
    //   the "pinned" deployment are shown and compared to the most recent version in the feed sources.
    public String pinnedDeploymentId;

    /**
     * Feed source in which to store regionally merged GTFS feeds. If specified, during a regional feed merge all feeds
     * in the project will be merged except for the feed source defined here.
     */
    public String regionalFeedSourceId;

    public Project() {
        this.buildConfig = new OtpBuildConfig();
        this.routerConfig = new OtpRouterConfig();
        this.useCustomOsmBounds = false;
    }

    /**
     * Get all the feed sources for this project.
     */
    public Collection<FeedSource> retrieveProjectFeedSources() {
        // TODO: use index, but not important for now because we generally only have one FeedCollection
        return Persistence.feedSources.getAll().stream()
                .filter(fs -> this.id.equals(fs.projectId))
                .collect(Collectors.toList());
    }

    /**
     * Get all the labels for this project.
     */
    public Collection<Label> retrieveProjectLabels() {
        return Persistence.labels.getAll().stream()
                .filter(label -> this.id.equals(label.projectId))
                .collect(Collectors.toList());
    }

    // Keep an empty collection here which is filled dynamically later
    public Collection<Label> labels;


    // Note: Previously a numberOfFeeds() dynamic Jackson JsonProperty was in place here. But when the number of projects
    // in the database grows large, the efficient calculation of this field does not scale.

    /**
     * Get all the deployments for this project.
     */
    public Collection<Deployment> retrieveDeployments() {
        return Persistence.deployments.getFiltered(eq("projectId", this.id));
    }

    // TODO: Does this need to be returned with JSON API response
    public Organization retrieveOrganization() {
        if (organizationId != null) {
            return Persistence.organizations.getById(organizationId);
        } else {
            return null;
        }
    }

    public void delete() {
        // FIXME: Handle this in a Mongo transaction. See https://docs.mongodb.com/master/core/transactions/#transactions-and-mongodb-drivers
        // Delete each feed source in the project (which in turn deletes each feed version).
        retrieveProjectFeedSources().forEach(FeedSource::delete);
        // Delete each deployment in the project.
        retrieveDeployments().forEach(Deployment::delete);
        // Finally, delete the project.
        Persistence.projects.removeById(this.id);
    }

    /**
     * A MixIn to be applied to this project, for returning a single project, so that the list of otpServers is
     * included in the JSON response.
     *
     * Usually a mixin would be used on an external class, but since we are changing one thing about a single class, it
     * seemed unnecessary to define a new view.
     */
    public abstract static class ProjectWithOtpServers {

        @JsonProperty("otpServers")
        public abstract Collection<OtpServer> availableOtpServers ();
    }
}
