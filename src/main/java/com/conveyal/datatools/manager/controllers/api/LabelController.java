package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.manager.auth.Actions;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.models.Label;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static com.conveyal.datatools.common.utils.SparkUtils.getPOJOFromRequestBody;
import static com.conveyal.datatools.common.utils.SparkUtils.logMessageAndHalt;
import static spark.Spark.*;
import static spark.Spark.post;


public class LabelController {
    private static final Logger LOG = LoggerFactory.getLogger(LabelController.class);
    private static JsonManager<Label> json = new JsonManager<>(Label.class, JsonViews.UserInterface.class);


    /**
     * Spark HTTP endpoint to get a single label by ID
     */
    private static Label getLabel(Request req, Response res) {
        return requestLabelById(req, Actions.VIEW);
    }

    /**
     * Spark HTTP endpoint that handles getting all labels for a handful of use cases:
     * - for a single project (if projectId query param provided)
     * - for the entire application
     */
    private static Collection<Label> getAllLabels(Request req, Response res) {
        Collection<Label> labelsToReturn = new ArrayList<>();
        Auth0UserProfile user = req.attribute("user");
        String projectId = req.queryParams("projectId");
        Project project = Persistence.projects.getById(projectId);
        if (project == null) {
            logMessageAndHalt(req, 400, "Must provide valid projectId query param to retrieve labels.");
        }

        Collection<Label> projectLabels = project.retrieveProjectLabels();
        for (Label label: projectLabels) {
            // Only return labels users has access to.
            labelsToReturn.add(checkLabelPermissions(req, label));
        }
        return labelsToReturn;
    }

    /**
     * HTTP endpoint to create a new label
     */
    private static Label createLabel(Request req, Response res) throws IOException {
        Auth0UserProfile userProfile = req.attribute("user");
        Label newLabel = getPOJOFromRequestBody(req, Label.class);
        validate(req, newLabel);

        try {
            Persistence.labels.create(newLabel);
            return newLabel;
        } catch (Exception e) {
            logMessageAndHalt(req, 500, "Unknown error encountered creating label", e);
            return null;
        }
    }
    /**
     * Check that updated or new label object is valid. This method should be called before a label is
     * persisted to the database.
     */
    private static void validate(Request req, Label label) {
        List<String> validationIssues = new ArrayList<>();
        // Label is quite forgiving (sets defaults if null) and the boolean value is type checked,
        // so there is little to validate.
        if (StringUtils.isEmpty(label.name)) {
            validationIssues.add("Name field must not be empty.");
        }
        if (!validationIssues.isEmpty()) {
            logMessageAndHalt(
                    req,
                    HttpStatus.BAD_REQUEST_400,
                    "Request was invalid for the following reasons: " + String.join(", ", validationIssues)
            );
        }
    }

    /**
     * Spark HTTP endpoint to update a label. Requires that the JSON body represent all
     * fields the updated label should contain. See a similar note on FeedSourceController for more info
     */
    private static Label updateLabel(Request req, Response res) throws IOException {
        String labelId = req.params("id");
        Label formerLabel = requestLabelById(req, Actions.MANAGE);
        Label updatedLabel = getPOJOFromRequestBody(req, Label.class);

        // Some things shouldn't be updated
        updatedLabel.projectId = formerLabel.projectId;
        updatedLabel.id = formerLabel.id;

        validate(req, updatedLabel);

        Persistence.labels.replace(labelId, updatedLabel);

        return updatedLabel;
    }
    /**
     * Spark HTTP endpoint to delete a label
     *
     * FIXME: Should this just set a "deleted" flag instead of removing from the database entirely?
     */
    private static Label deleteLabel(Request req, Response res) {
        Label label = requestLabelById(req, Actions.MANAGE);

        // More specific error message if label doesn't exist
        if (label == null) {
            logMessageAndHalt(req, 403, "Label does not exist.");
            return null;
        }

        try {
            // Iterate over feed sources and remove any references to the label
            Persistence.feedSources.getAll().forEach(feedSource -> {
                feedSource.labels = feedSource.labels.stream()
                        .filter(l -> !l.equals(label.id))
                        .collect(Collectors.toList());
            });

            label.delete();
            return label;
        } catch (Exception e) {
            logMessageAndHalt(req, 500, "Unknown error occurred while deleting label.", e);
            return null;
        }
    }

    /**
     * Helper function returns label if user has permission for specified action.
     * @param req spark Request object from API request
     * @param action action type (either "view" or Permission.MANAGE)
     * @return label object for ID
     */
    private static Label requestLabelById(Request req, Actions action) {
        String id = req.params("id");
        if (id == null) {
            logMessageAndHalt(req, 400, "Please specify id param");
        }
        return checkLabelPermissions(req, Persistence.labels.getById(id));
    }

    public static Label checkLabelPermissions(Request req, Label label) {
        Auth0UserProfile userProfile = req.attribute("user");
        // check for null label
        if (label == null) {
            logMessageAndHalt(req, 400, "Label ID does not exist");
            return null;
        }

        if(!label.adminOnly) {
            // Don't need to check for authorization -- anyone may do anything to the label
            return label;
        }

        // The label is privileged, so check if user is admin
        String orgId = label.organizationId();
        boolean authorized = userProfile.canAdministerProject(label.projectId, orgId);

        if (!authorized) {
            // Throw halt if user not authorized.
            logMessageAndHalt(req, 403, "User not admin so cannot view admin-only label");
        }
        // If we make it here, user has permission and the requested label is valid
        return label;
    }

    // FIXME: use generic API controller and return JSON documents via BSON/Mongo
    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/label/:id", LabelController::getLabel, json::write);
        get(apiPrefix + "secure/label", LabelController::getAllLabels, json::write);
        post(apiPrefix + "secure/label", LabelController::createLabel, json::write);
        put(apiPrefix + "secure/label/:id", LabelController::updateLabel, json::write);
        delete(apiPrefix + "secure/label/:id", LabelController::deleteLabel, json::write);
    }
}
