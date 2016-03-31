package org.apache.kafka.connect.utils;

import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import java.io.File;
import java.io.IOException;


/**
 * @author Sergio Spinatelli
 */
public class DirWatcherTest {

    @Before
    public void setup() throws IOException {
        DirWatcher task = new DirWatcher("D:/workdir/development/man/test/samba", "") {
            protected void onChange(File file, String action) {
                // here we code the action on a change
                System.out.println
                        ("File " + file.getName() + " action: " + action);
            }
        };
    }

    @Test
    public void testNormalLifecycle() {

    }

    private void replay() {
        PowerMock.replayAll();
    }

}