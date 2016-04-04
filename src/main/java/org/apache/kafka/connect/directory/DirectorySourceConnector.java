package org.apache.kafka.connect.directory;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.apache.kafka.connect.utils.StringUtils;

import java.util.*;


/**
 * DirectorySourceConnector implements the connector interface
 * to write on Kafka file system events (creations, modifications etc)
 *
 * @author Sergio Spinatelli
 */
public class DirectorySourceConnector extends SourceConnector {
    public static final String DIR_PATH = "directories.paths";
    public static final String CHCK_DIR_MS = "check.dir.ms";
    public static final String SCHEMA_NAME = "schema.name";
    public static final String TOPIC = "topic";

    private String directories;
    private String check_dir_ms;
    private String schema_name;
    private String topic;

    /**
     * Get the version of this connector.
     *
     * @return the version, formatted as a String
     */
    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }


    /**
     * Start this Connector. This method will only be called on a clean Connector, i.e. it has
     * either just been instantiated and initialized or {@link #stop()} has been invoked.
     *
     * @param props configuration settings
     */
    @Override
    public void start(Map<String, String> props) {
        schema_name = props.get(SCHEMA_NAME);
        if (schema_name == null || schema_name.isEmpty())
            throw new ConnectException("missing schema.name");

        topic = props.get(TOPIC);
        if (topic == null || topic.isEmpty())
            throw new ConnectException("missing topic");

        directories = props.get(DIR_PATH);
        if (directories == null || directories.isEmpty())
            throw new ConnectException("missing directories");

        check_dir_ms = props.get(CHCK_DIR_MS);
        if (check_dir_ms == null || check_dir_ms.isEmpty())
            check_dir_ms = "1000";
    }


    /**
     * Returns the Task implementation for this Connector.
     *
     * @return tha Task implementation Class
     */
    @Override
    public Class<? extends Task> taskClass() {
        return DirectorySourceTask.class;
    }


    /**
     * Returns a set of configurations for the Tasks based on the current configuration.
     * It always creates a single set of configurations.
     *
     * @param maxTasks maximum number of configurations to generate
     * @return configurations for the Task
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        List<String> dirs = Arrays.asList(directories.split(","));
        for (int i = 0; i < dirs.size(); i++) {
            Map<String, String> config = new HashMap<>();
            config.put(CHCK_DIR_MS, check_dir_ms);
            config.put(SCHEMA_NAME, schema_name);
            config.put(TOPIC, topic);
            config.put(DIR_PATH, dirs.get(i));
            configs.add(config);
        }
        return configs;
    }


    /**
     * Stop this connector.
     */
    @Override
    public void stop() {

    }

}
