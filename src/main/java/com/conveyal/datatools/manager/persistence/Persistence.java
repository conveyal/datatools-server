package com.conveyal.datatools.manager.persistence;

import com.conveyal.datatools.manager.codec.FetchFrequencyCodec;
import com.conveyal.datatools.manager.codec.IntArrayCodec;
import com.conveyal.datatools.manager.codec.LocalDateCodec;
import com.conveyal.datatools.manager.codec.URLCodec;
import com.conveyal.datatools.manager.models.*;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.conveyal.datatools.manager.DataManager.getConfigPropertyAsText;

/**
 * Groups together a bunch of TypedPersistence abstractions around MongoDB Collections.
 */
public class Persistence {

    private static final Logger LOG = LoggerFactory.getLogger(Persistence.class);
    private static final String MONGO_PROTOCOL = getConfigPropertyAsText("MONGO_PROTOCOL", "mongodb");
    private static final String MONGO_HOST = getConfigPropertyAsText("MONGO_HOST", "localhost:27017");
    private static final String MONGO_USER = getConfigPropertyAsText("MONGO_USER");
    private static final String MONGO_PASSWORD = getConfigPropertyAsText("MONGO_PASSWORD");
    private static final String MONGO_DB_NAME = getConfigPropertyAsText("MONGO_DB_NAME");

    private static MongoClient mongo;
    private static MongoDatabase mongoDatabase;

    // One abstracted Mongo collection for each class of persisted objects
    public static TypedPersistence<FeedSource> feedSources;
    public static TypedPersistence<Deployment> deployments;
    public static TypedPersistence<Project> projects;
    public static TypedPersistence<FeedVersion> feedVersions;
    public static TypedPersistence<Note> notes;
    public static TypedPersistence<Organization> organizations;
    public static TypedPersistence<ExternalFeedSourceProperty> externalFeedSourceProperties;
    public static TypedPersistence<OtpServer> servers;
    public static TypedPersistence<Snapshot> snapshots;
    public static TypedPersistence<FeedDownloadToken> tokens;
    public static TypedPersistence<Label> labels;

    public static void initialize () {

        PojoCodecProvider pojoCodecProvider = PojoCodecProvider.builder()
            .register("com.conveyal.datatools.manager.jobs")
            .register("com.conveyal.datatools.manager.models")
            .register("com.conveyal.gtfs.loader")
            .register("com.conveyal.gtfs.validator")
            .automatic(true)
            .build();

        // Register our custom codecs which cannot be properly auto-built by reflection
        CodecRegistry customRegistry = CodecRegistries.fromCodecs(
            new FetchFrequencyCodec(),
            new IntArrayCodec(),
            new URLCodec(),
            new LocalDateCodec()
        );
        // Provide codec registries in order. Mongo will check each registry for the class codecs in the order they are
        // listed.
        CodecRegistry pojoCodecRegistry = CodecRegistries.fromRegistries(
            // Custom registry must come before getDefaultCodecRegistry because the default registry did not contain a
            // LocalDateCodec when this project was first developed, and we encoded into a string value rather than a
            // milliseconds since epoch value that the new default one does:
            // https://github.com/mongodb/mongo-java-driver/blob/7f6e0c3be351e9d586ca4a6271f79af654b03deb/bson/src/main/org/bson/codecs/jsr310/LocalDateCodec.java#L56-L62
            customRegistry,
            // Default registry must come before package providers so that default codecs (e.g., string) can be located.
            MongoClientSettings.getDefaultCodecRegistry(),
            CodecRegistries.fromProviders(pojoCodecProvider)
            );

        // Construct connection string from configuration values.
        String userAtPassword = MONGO_USER != null && MONGO_PASSWORD != null
            ? String.format("%s:%s@", MONGO_USER, MONGO_PASSWORD)
            : "";
        final String MONGO_URI = String.join("/", MONGO_HOST, MONGO_DB_NAME);
        ConnectionString connectionString = new ConnectionString(
            String.format(
                "%s://%s%s?retryWrites=true&w=majority",
                MONGO_PROTOCOL,
                userAtPassword,
                MONGO_URI
            )
        );
        MongoClientSettings clientSettings = MongoClientSettings.builder()
            .applyConnectionString(connectionString)
            .codecRegistry(pojoCodecRegistry)
            .build();
        LOG.info("Connecting to MongoDB instance at {}://{}", MONGO_PROTOCOL, MONGO_URI);
        mongo = MongoClients.create(clientSettings);
        mongoDatabase = mongo.getDatabase(MONGO_DB_NAME);

        feedSources = new TypedPersistence(mongoDatabase, FeedSource.class);
        projects = new TypedPersistence(mongoDatabase, Project.class);
        feedVersions = new TypedPersistence(mongoDatabase, FeedVersion.class);
        deployments = new TypedPersistence(mongoDatabase, Deployment.class);
        notes = new TypedPersistence(mongoDatabase, Note.class);
        organizations = new TypedPersistence(mongoDatabase, Organization.class);
        externalFeedSourceProperties = new TypedPersistence(mongoDatabase, ExternalFeedSourceProperty.class);
        servers = new TypedPersistence(mongoDatabase, OtpServer.class);
        snapshots = new TypedPersistence(mongoDatabase, Snapshot.class);
        tokens = new TypedPersistence(mongoDatabase, FeedDownloadToken.class);
        labels = new TypedPersistence(mongoDatabase, Label.class);

        // TODO: Set up indexes on feed versions by feedSourceId, version #? deployments, feedSources by projectId.
//        deployments.getMongoCollection().createIndex(Indexes.descending("projectId"));
//        feedSources.getMongoCollection().createIndex(Indexes.descending("projectId"));
//        feedVersions.getMongoCollection().createIndex(Indexes.descending("feedSourceId", "version"));
//        snapshots.getMongoCollection().createIndex(Indexes.descending("feedSourceId", "version"));
    }
    
}
