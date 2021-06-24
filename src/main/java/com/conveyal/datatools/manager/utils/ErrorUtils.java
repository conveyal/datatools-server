package com.conveyal.datatools.manager.utils;

import com.bugsnag.Bugsnag;
import com.bugsnag.Report;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;

import static com.conveyal.datatools.manager.DataManager.getBugsnag;

public class ErrorUtils {
    /**
     * Create report to notify bugsnag if configured.
     */
    public static void reportToBugsnag(Exception e, Auth0UserProfile userProfile) {
        Bugsnag bugsnag = getBugsnag();
        if (bugsnag != null && e != null) {
            // create report to send to bugsnag
            Report report = bugsnag.buildReport(e);
            String userEmail = userProfile != null ? userProfile.getEmail() : "no-auth";
            report.setUserEmail(userEmail);
            bugsnag.notify(report);
        }
    }
}
