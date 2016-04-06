package org.apache.kafka.connect.directory;

import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.utils.DirWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.*;


/**
 * DirectorySourceTask is a Task that reads changes from a directory for storage
 * new binary detected files in Kafka.
 *
 * @author Alex Piermatteo
 */
public class DirectorySourceTask extends SourceTask {
    private final static Logger log = LoggerFactory.getLogger(DirectorySourceTask.class);
    private final static String LAST_UPDATE = "last_update";
    private final static String FILE_UPDATE = "file_update";

    private String tmp_path;

    private TimerTask task;
    private static Schema schema = null;
    private String schemaName;
    private String topic;
    private String check_dir_ms;
    Long offset = null;


    @Override
    public String version() {
        return new DirectorySourceConnector().version();
    }

    /**
     * Start the Task. Handles configuration parsing and one-time setup of the Task.
     *
     * @param props initial configuration
     */
    @Override
    public void start(Map<String, String> props) {
        schemaName = props.get(DirectorySourceConnector.SCHEMA_NAME);
        if (schemaName == null)
            throw new ConnectException("config schema.name null");
        topic = props.get(DirectorySourceConnector.TOPIC);
        if (topic == null)
            throw new ConnectException("config topic null");

        tmp_path = props.get(DirectorySourceConnector.DIR_PATH);
        if (tmp_path == null)
            throw new ConnectException("config tmp.path null");

        check_dir_ms = props.get(DirectorySourceConnector.CHCK_DIR_MS);

        loadOffsets();

        task = new DirWatcher(tmp_path, "", offset == null ? null : FileTime.fromMillis(offset)) {
            protected void onChange(File file, String action) {
                // here we code the action on a change
                System.out.println
                        ("File " + file.getName() + " action: " + action);
            }
        };
        Timer timer = new Timer();
        timer.schedule(task, new Date(), Long.parseLong(check_dir_ms));

        log.trace("Creating schema");
        schema = SchemaBuilder
                .struct()
                .name(schemaName)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("path", Schema.OPTIONAL_STRING_SCHEMA)
                .field("event", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }


    /**
     * Poll this DirectorySourceTask for new records.
     *
     * @return a list of source records
     * @throws InterruptedException
     */
    @Override
    public List<SourceRecord> poll() throws InterruptException {

        List<SourceRecord> records = new ArrayList<>();
        DirWatcher watcher = ((DirWatcher) task);
        //consume here the pool
        if (!watcher.isEmpty()) {
            File file = watcher.poll();
            log.warn("PROCESSING {}", file.getName());
            try {
                RandomAccessFile in = new RandomAccessFile(file, "rw");
                FileLock lock = null;
                try {
                    lock = in.getChannel().lock();
                    SourceRecord record = createUpdateRecord(file);
                    records.add(record);
                    lock.release();
                    watcher.retried(file);
                } catch(Exception ex) {
                    watcher.retry(file);
                    lock.release();
                } finally {
                    in.close();
                }
            } catch(Exception ex) {
                watcher.retry(file);
                // sned pending record
            }
        }

        return records;
    }


    /**
     * Create a new SourceRecord from a File
     *
     * @return a source records
     */
    private SourceRecord createUpdateRecord(File file) {
        // creates the structured message
        Struct messageStruct = new Struct(schema);
        messageStruct.put("name", file.getName());
        messageStruct.put("path", file.getPath());
        try {
            BasicFileAttributes fa = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
            FileTime lastMod = fa.lastModifiedTime();
            FileTime created = fa.lastAccessTime();

            messageStruct.put("event", lastMod.compareTo(created) <= 0 ? "CREATED" : "MODIFIED");
            // creates the record
            // no need to save offsets
            return new SourceRecord(offsetKey(), offsetValue(lastMod.compareTo(created) > 0 ? lastMod : created), topic, messageStruct.schema(), messageStruct);
        } catch (IOException e) {
        }
        return null;
    }

    private Map<String, String> offsetKey() {
        return Collections.singletonMap(FILE_UPDATE, tmp_path);
    }

    private Map<String, Object> offsetValue(FileTime time) {
        offset = time.toMillis();
        return Collections.singletonMap(LAST_UPDATE, offset);
    }

    /**
     * Loads the current saved offsets.
     */
    private void loadOffsets() {
        Map<String, Object> off = context.offsetStorageReader().offset(offsetKey());
        if (off != null)
            offset = (Long) off.get(LAST_UPDATE);
    }

    /**
     * Signal this SourceTask to stop.
     */
    @Override
    public void stop() {
        task.cancel();
    }

}
