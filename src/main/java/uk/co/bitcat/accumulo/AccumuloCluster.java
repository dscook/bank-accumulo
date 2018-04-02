package uk.co.bitcat.accumulo;

/*
 * MODIFIED 2/4/2018 - Edited lines carry a comment to this effect
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.File;
import java.io.IOException;

import com.google.common.io.Files;

import java.lang.InterruptedException;
import java.lang.Runnable;

import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;

public class AccumuloCluster implements Runnable {

    @Override
    public void run() {
        File tempDir = null;
        MiniAccumuloCluster mac = null;

        try {
            tempDir = Files.createTempDir();
            tempDir.deleteOnExit();

            // MODIFIED 2/4/2018
            final String PASSWORD = "password";

            // MODIFIED 2/4/2018
            MiniAccumuloConfig config = new MiniAccumuloConfig(tempDir, PASSWORD);
            config.setZooKeeperPort(50274);
            mac = new MiniAccumuloCluster(config);

            // MODIFIED 2/4/2018
            System.out.println("MiniAccumuloCluster starting with settings:");
            System.out.println("instance name: " + mac.getInstanceName());
            System.out.println("zookeepers: " + mac.getZooKeepers());
            System.out.println("user: root\npassword: " + PASSWORD);

            mac.start();

            String[] args = new String[] {"-u", "root", "-p", PASSWORD, "-z",
                    mac.getInstanceName(), mac.getZooKeepers()};

            Shell.main(args);

        } catch (InterruptedException e) {
            System.err.println("Error starting MiniAccumuloCluster: " + e.getMessage());
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Error starting MiniAccumuloCluster: " + e.getMessage());
            System.exit(1);
        } finally {
            if (null != tempDir) {
                tempDir.delete();
            }

            if (null != mac) {
                try {
                    mac.stop();
                } catch (InterruptedException e) {
                    System.err.println("Error stopping MiniAccumuloCluster: " + e.getMessage());
                    System.exit(1);
                } catch (IOException e) {
                    System.err.println("Error stopping MiniAccumuloCluster: " + e.getMessage());
                    System.exit(1);
                }
            }
        }
    }

    public static void main(String[] args) {
        // MODIFIED 2/4/2018
        System.out.println("\n   ---- Initializing Accumulo\n");

        AccumuloCluster shell = new AccumuloCluster();
        shell.run();

        System.exit(0);
    }
}
