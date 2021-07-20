package com.conveyal.datatools.manager.models;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.conveyal.datatools.common.utils.Scheduler;
import com.conveyal.datatools.common.utils.aws.S3Utils;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.gtfs.GTFS;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.mongodb.client.model.Filters.eq;

public class Label extends Model implements Cloneable{
    public static final Logger LOG = LoggerFactory.getLogger(Label.class);

    /**
     * The collection of which this Label is a part
     */
    public String projectId;

    /**
     * Get the Project of which this label is a part
     */
    public Project retrieveProject() {
        return projectId != null ? Persistence.projects.getById(projectId) : null;
    }

    @JsonProperty("organizationId")
    public String organizationId () {
        Project project = retrieveProject();
        return project == null ? null : project.organizationId;
    }

    /** The name of this label, e.g. "Complete" or "Broken" */
    public String name;

    /** A description for the label */
    public String description;

    /** The color of the label. Stored as a string because it will only ever hold a CSS hex value */
    public String color;

    /** Is this label only accessible to admin users? */
    public boolean adminOnly;

    /**
     * Create a new label
     */
    public Label (String name, String description, String color, boolean adminOnly, String projectId) {
        super();
        this.name = name;
        this.description = description != null ? description : "";
        this.color = color != null ? color : "#fff";
        this.adminOnly = adminOnly;

        this.projectId = projectId;
    }

    /**
     * No-arg constructor to yield an uninitialized feed source, for dump/restore.
     * Should not be used in general code.
     */
    public Label () {
        this(null, null, null, false, null);
    }

    /**
     * Delete this label
     */
    public void delete() {
        try {
            Persistence.labels.removeById(this.id);
        } catch (Exception e) {
            LOG.error("Could not delete label", e);
        }
    }

    public int compareTo(Label o) {
        return this.name.compareTo(o.name);
    }

    public String toString () {
        return "<Label " + this.name + " (" + this.id + ")>";
    }
}
