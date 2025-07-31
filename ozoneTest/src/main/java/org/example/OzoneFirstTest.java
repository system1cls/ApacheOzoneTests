package org.example;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.*;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.checkerframework.checker.units.qual.C;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Objects;
import java.util.Random;


public class OzoneFirstTest {
    public static void main(String[] args) {
        Conter conter = new Conter();
        if (args.length == 0) {
            throw new RuntimeException("not enough args");
        }

        String ozonePath = "." + File.separator;
        if (args.length > 1) ozonePath = args[1];

        String filePath = "." + File.separator;
        if (args.length > 2) ozonePath = args[2];

        int cntThreads = 0;
        try {
            cntThreads = Integer.parseInt(args[0]);
        } catch (RuntimeException e) {
            throw new RuntimeException(e);
        }

        Thread []threads = new Thread[cntThreads];
        for (int i = 0; i < cntThreads; i++) {
            threads[i] = new Thread(new RPCRequster(conter));
        }

        for (int i = 0; i < cntThreads; i++) {
            threads[i].start();
        }


    }

    protected static class Executor implements Runnable {
        private final String pathToFile;
        private final String pathToOzone;
        private final boolean shouldWait;

        Executor(String pathToOzone, String pathToFile, boolean shouldWait) {
            this.pathToFile = pathToFile;
            this.pathToOzone = pathToOzone;
            this.shouldWait = shouldWait;
        }

        @Override
        public void run() {
            Random random = new Random();
            try {
                for (;;) {
                    runProc(random, shouldWait);
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
                throw new RuntimeException(e);
            }
        }


        private void runProc(Random random, boolean shouldWait) {
            ProcessBuilder builder = new ProcessBuilder();
            try {
                Process process = prepareProc(random, builder, pathToOzone, pathToFile).start();
                if (shouldWait) process.waitFor();
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        private ProcessBuilder prepareProc(Random random, ProcessBuilder builder, String pathToOzone, String pathToFile) {

            String key = generateKey(random);

            builder.command(pathToOzone, "sh", "key", "put", "vol2/buck2/" + key, pathToFile);

            System.out.println(Thread.currentThread() + " key = " + key);
            return builder;
        }

    }

    protected static class RPCRequster implements Runnable {
        byte[] data = "data\ndata".getBytes(StandardCharsets.UTF_8);
        Conter conter;

        public RPCRequster(Conter conter) {
            this.conter = conter;
        }

        @Override
        public void run() {
            Random random = new Random();
            try {
                for (;;) {
                    OzoneClient client = connect();
                    ObjectStore store = client.getObjectStore();

                    OzoneVolume vol = checkAndCreateVol(store, "vol1");
                    OzoneBucket buck = checkAndCreateBuck(vol, "buck1");

                    for (int i = 0; i < 10; i++) {
                        addKey(buck, random).close();
                    }
                    conter.inc();
                    client.close();
                }

            } catch (RuntimeException | IOException e) {
                System.out.println(Thread.currentThread() + " error: " + e.getMessage());
                throw new RuntimeException(e);
            }
        }

        private OzoneClient connect() {
            OzoneConfiguration ozConf = new OzoneConfiguration();
            ozConf.set("ozone.security.enabled", "false");
            ozConf.set("ozone.om.address", "192.168.100.103");
            OzoneClient client;
            try {
                client = OzoneClientFactory.getRpcClient(ozConf);
            } catch (IOException e) {
                System.out.println("Ozone client error: " + e.getMessage());
                throw new RuntimeException(e);
            }
            return client;
        }

        private OzoneVolume checkAndCreateVol(ObjectStore store, String volName) {
            try {

                return store.getVolume(volName);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private OzoneBucket checkAndCreateBuck(OzoneVolume volume, String buckName) {
            try {

                return volume.getBucket(buckName);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void readFile(OzoneBucket buck, String fileName) {
            try {
                byte []data = new byte[2048];
              OzoneInputStream stream = buck.readFile(fileName);
              stream.read(data);
              System.out.println(new String(data, StandardCharsets.UTF_8));
            } catch (IOException e) {
                System.out.println("readFile error: " + e.getMessage());
                throw new RuntimeException(e);
            }
        }

        private void addFile(OzoneBucket buck, Random random) {

            try {
                OzoneOutputStream stream = addKey(buck, random);
                stream.write(data);
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private OzoneOutputStream addKey(OzoneBucket buck, Random random) {
            try {
                String key = generateKey(random);
                OzoneOutputStream stream = buck.createKey(key, data.length);
                return stream;
            } catch (IOException e ) {
                throw new RuntimeException(e);
            }
        }
    }

    private static String generateKey(Random random) {
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = 1024*10;

        String key = random.ints(leftLimit, rightLimit + 1).limit(targetStringLength)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();
        return  key;
    }

}