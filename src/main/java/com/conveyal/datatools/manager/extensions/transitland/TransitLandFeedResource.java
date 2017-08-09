package com.conveyal.datatools.manager.extensions.transitland;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.extensions.ExternalFeedResource;
import com.conveyal.datatools.manager.models.ExternalFeedSourceProperty;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by demory on 3/31/16.
 */

public class TransitLandFeedResource implements ExternalFeedResource {

    public static final Logger LOG = LoggerFactory.getLogger(TransitLandFeedResource.class);

    private String api;

    public TransitLandFeedResource() {
        api = DataManager.getConfigPropertyAsText("extensions.transitland.api");
    }

    @Override
    public String getResourceType() {
        return "TRANSITLAND";
    }

    @Override
    public void importFeedsForProject(Project project, String authHeader) {
        LOG.info("Importing TransitLand feeds");
        URL url = null;
        ObjectMapper mapper = new ObjectMapper();
        int perPage = 10000;
        int count = 0;
        int offset;
        int total = 0;
        String locationFilter = "";
        boolean nextPage = true;

        if (project.north != null && project.south != null && project.east != null && project.west != null)
            locationFilter = "&bbox=" + project.west + "," + + project.south + "," + project.east + "," + project.north;

        do {
            offset = perPage * count;
            try {
                url = new URL(api + "?total=true&per_page=" + perPage + "&offset=" + offset + locationFilter);
            } catch (MalformedURLException ex) {
                LOG.error("Error constructing TransitLand API URL");
            }

            try {
                HttpURLConnection con = (HttpURLConnection) url.openConnection();

                // optional default is GET
                con.setRequestMethod("GET");

                //add request header
                con.setRequestProperty("User-Agent", "User-Agent");

                int responseCode = con.getResponseCode();
                System.out.println("\nSending 'GET' request to URL : " + url);
                System.out.println("Response Code : " + responseCode);

                BufferedReader in = new BufferedReader(
                        new InputStreamReader(con.getInputStream()));
                String inputLine;
                StringBuffer response = new StringBuffer();

                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();

                String json = response.toString();
                JsonNode node = mapper.readTree(json);
                total = node.get("meta").get("total").asInt();
                for (JsonNode feed : node.get("feeds")) {
                    TransitLandFeed tlFeed = new TransitLandFeed(feed);

                    FeedSource source = null;

                    // check if a feed already exists with this id
                    for (FeedSource existingSource : project.getProjectFeedSources()) {
                        ExternalFeedSourceProperty onestopIdProp =
                                ExternalFeedSourceProperty.find(existingSource, this.getResourceType(), "onestop_id");
                        if (onestopIdProp != null && onestopIdProp.value.equals(tlFeed.onestop_id)) {
                            source = existingSource;
                        }
                    }

                    String feedName;
                    feedName = tlFeed.onestop_id;

                    if (source == null) {
                        source = new FeedSource(feedName);
                        LOG.info("Creating new feed source: {}", source.name);
                    }
                    else {
                        source.name = feedName;
                        LOG.info("Syncing properties: {}", source.name);
                    }
                    tlFeed.mapFeedSource(source);

                    source.setName(feedName);

                    source.setProject(project);

                    source.save();

                    // create / update the properties

                    for(Field tlField : tlFeed.getClass().getDeclaredFields()) {
                        String fieldName = tlField.getName();
                        String fieldValue = tlField.get(tlFeed) != null ? tlField.get(tlFeed).toString() : null;

                        ExternalFeedSourceProperty.updateOrCreate(source, this.getResourceType(), fieldName, fieldValue);
                    }
                }
            } catch (Exception ex) {
                LOG.error("Error reading from TransitLand API");
                ex.printStackTrace();
            }
            count++;
        }
        // iterate over results until most recent total exceeds total feeds in TransitLand
        while(offset + perPage < total);

    }

    @Override
    public void feedSourceCreated(FeedSource source, String authHeader) {

    }

    @Override
    public void propertyUpdated(ExternalFeedSourceProperty property, String previousValue, String authHeader) {

    }

    @Override
    public void feedVersionCreated(FeedVersion feedVersion, String authHeader) {

    }
}
