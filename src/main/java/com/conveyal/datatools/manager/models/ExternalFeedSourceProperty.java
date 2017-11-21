package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Collection;

/**
 * Created by demory on 3/30/16.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExternalFeedSourceProperty extends Model {
    private static final long serialVersionUID = 1L;

    // constructor for data dump load
    public ExternalFeedSourceProperty() {}

    public ExternalFeedSourceProperty(FeedSource feedSource, String resourceType, String name, String value) {
        this.id = constructId(feedSource, resourceType, name);
        this.feedSourceId = feedSource.id;
        this.resourceType = resourceType;
        this.name = name;
        this.value = value;
    }

    public static String constructId(FeedSource feedSource, String resourceType, String name) {
        return feedSource.id + "_" + resourceType + "_" + name;
    }

    public String resourceType;

    public String feedSourceId;

    public String name;

    public String value;
}
