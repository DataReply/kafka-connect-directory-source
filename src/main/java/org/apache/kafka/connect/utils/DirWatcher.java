package org.apache.kafka.connect.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;


/**
 * DirWatcher is a TimerTask class that periodically watch changes on a
 * defined directory. Every detected file is putted in a queue waiting
 * to be sent to Kafka.
 *
 * @author Alex Piermatteo
 */
public abstract class DirWatcher extends TimerTask {
    private final static Logger log = LoggerFactory.getLogger(DirWatcher.class);
    private String path;
    private File filesArray[];
    private DirFilterWatcher dfw;
    private ConcurrentLinkedQueue<File> queue_files;
    FileTime lastUpdate = null;
    FileTime maxTimestamp = null;

    /**
     * Constructor of the class.
     *
     * @param path directory path to watch
     * @param filter detect changes only for filtered extensions
     **/
    public DirWatcher(String path, String filter) {
        this.path = path;
        dfw = new DirFilterWatcher(filter);

        queue_files = new ConcurrentLinkedQueue<File>();
    }

    /**
     * Constructor of the class.
     *
     * @param path directory path to watch
     * @param filter detect changes only for filtered extensions
     * @param path directory path to watch
     **/
    public DirWatcher(String path, String filter, FileTime lastUpdate) {
        this(path, filter);
        this.lastUpdate = lastUpdate;
        this.maxTimestamp = lastUpdate;
    }

    private final boolean hasUpdate(Path path, LinkOption... options) {
        if (path.equals(Paths.get(this.path)))
            return false;
        try {
            BasicFileAttributes fa = Files.readAttributes(path, BasicFileAttributes.class, options);
            FileTime changed = Files.readAttributes(path, BasicFileAttributes.class, options).lastAccessTime();

            boolean hasUpdate = (fa.isRegularFile() && (lastUpdate == null || changed.compareTo(lastUpdate) > 0));
            if (maxTimestamp == null || changed.compareTo(maxTimestamp) > 0)
                maxTimestamp = changed;
//            log.warn("====START===");
//            log.warn("File: {}", path.toString());
//            log.warn("Has update {}", hasUpdate);
//            if (lastUpdate != null) {
//                log.warn("last update {}", lastUpdate.toString());
//            }
//            log.warn("====END===");
            return hasUpdate;
        } catch (IOException e) {
        }
        return false;
    }


    /**
     * Run the thread.
     */
    public final void run() {
        try {
            filesArray = Files.walk(Paths.get(path))
                    .sorted((o1, o2) -> {
                        try {
                            FileTime t1 = Files.readAttributes(o1, BasicFileAttributes.class).lastAccessTime();
                            FileTime t2 = Files.readAttributes(o2, BasicFileAttributes.class).lastAccessTime();
                            return t1.compareTo(t2);
                        } catch (IOException e) {
                        }
                        return 0;
                    })
                    .filter(this::hasUpdate).map(Path::toFile)
                    .toArray(File[]::new);
            lastUpdate = maxTimestamp;
            queue_files.addAll(Arrays.asList(filesArray));
            for (File f: filesArray) {
                onChange(f, "NEW OR MODIFIED");
            }
        } catch (IOException e) {
        }
    }


    /**
     * Expose the files queue
     */
    public ConcurrentLinkedQueue<File> getQueueFiles() {
        return queue_files;
    }

    protected abstract void onChange(File file, String action);
}