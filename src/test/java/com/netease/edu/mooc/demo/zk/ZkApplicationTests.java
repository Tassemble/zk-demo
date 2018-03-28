package com.netease.edu.mooc.demo.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.client.FourLetterWordMain;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ZkApplicationTests {

    static Logger LOG = LoggerFactory.getLogger(ZkApplicationTests.class);

    static ServerCnxnFactory factory = null;
    static ZooKeeperServer   zks     = null;

    static File dataDir = new File("~/work/spring-boot/zk-demo/");

    static int    CONNECTION_TIMEOUT = 30000;
    static String host               = "127.0.0.1";
    static int    port               = 2181;

    String basePath = "/long-path-000000000-111111111-222222222-333333333-444444444-555555555-666666666-777777777-888888888-999999999";
    static CuratorFramework client = null;

    List<String> paths    = new ArrayList<>();
    int          childNum = 10000;

    @Before
    public void setUp() throws IOException {
    }

    public static boolean waitForServerDown(long timeout) throws InterruptedException {
        long start = System.currentTimeMillis();
        int elapsedTime = 0;
        while (true) {
            try {
                TimeUnit.MILLISECONDS.sleep(200);
                FourLetterWordMain.send4LetterWord(host, port, "stat");
                elapsedTime += 200;
                if (elapsedTime > start + timeout) {
                    break;
                }
            } catch (IOException e) {
                return true;
            }
        }
        return false;
    }

    public static boolean waitForServerUp(int timeout) {
        long start = System.currentTimeMillis();
        int elapsedTime = 0;
        while (true) {
            try {
                TimeUnit.MILLISECONDS.sleep(200);
                String result = FourLetterWordMain.send4LetterWord(host, port, "stat");
                if (result.startsWith("Zookeeper version:") && !result.contains("READ-ONLY")) {
                    return true;
                }
                elapsedTime += 200;
                if (elapsedTime > start + timeout) {
                    break;
                }

            } catch (Exception e) {
                //expected ignore, try again
            }
        }
        return false;
    }

    /**
     * Starting the given server instance
     */
    public static void startServerInstance() throws IOException, InterruptedException {
        LOG.info("STARTING server instance 127.0.0.1:{}", 2181);
        factory = ServerCnxnFactory.createFactory(2181, 0);
        zks = new ZooKeeperServer(dataDir, dataDir, 3000);
        factory.startup(zks);
        Assert.assertTrue("waiting for server up", waitForServerUp(CONNECTION_TIMEOUT));
    }

    static void shutdownServerInstance() throws IOException, InterruptedException {
        if (factory != null) {
            factory.shutdown();
            zks.getZKDatabase().close();
            Assert.assertTrue("waiting for server down", waitForServerDown(CONNECTION_TIMEOUT));
        }
    }

    static void dropZKDB() throws IOException, InterruptedException {
        zks.getZKDatabase().clear();
    }

    @Test
    public void testCreateLargeWatches() {
        try {

            //start zk server
            startServerInstance();

            dropZKDB();

            //start client
            CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder().connectString(host + ":" + port).retryPolicy(
                    new ExponentialBackoffRetry(200, 3)).connectionTimeoutMs(CONNECTION_TIMEOUT).sessionTimeoutMs(60000).namespace(
                    "zk-demo1");

            client = builder.build();
            client.start();

            //创建1000个节点每个节点都设有watcher
            prepareData();

            //shutdown zk server
            shutdownServerInstance();

            awaitDisconnect(CONNECTION_TIMEOUT);

            //restart zk server
            startServerInstance();

            //you can see the result...
            TimeUnit.SECONDS.sleep(300);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void awaitTillConnectted(int connectionTimeout) {

        Assert.assertTrue("server disconnect!", waitTillConnectted(connectionTimeout));

    }

    private boolean waitTillConnectted(int connectionTimeout) {
        long start = System.currentTimeMillis();
        int elapsedTime = 0;
        while (true) {
            try {
                if (client.getZookeeperClient().isConnected()) {
                    return true;
                }
                TimeUnit.MILLISECONDS.sleep(200);
                elapsedTime += 200;
                if (elapsedTime > start + connectionTimeout) {
                    break;
                }
            } catch (Exception e) {
                return false;
            }
        }
        return false;
    }

    void awaitDisconnect(int connectionTimeout) {
        Assert.assertTrue("server disconnect!", waitTillDisconnnect(connectionTimeout));
    }


    void awaitMomentInSeconds(int seconds) {
        int elapsedTime = 0;
        while (true) {
            try {
                TimeUnit.SECONDS.sleep(1);
                elapsedTime++;
                LOG.info("elapsedTime:" + (seconds - elapsedTime));
                if (seconds <= elapsedTime) {
                    return;
                }
            } catch (Exception e) {
                return;
            }
        }
    }


    private boolean waitTillDisconnnect(int connectionTimeout) {

        long start = System.currentTimeMillis();
        int elapsedTime = 0;
        while (true) {
            try {
                if (!client.getZookeeperClient().isConnected()) {
                    return true;
                }
                TimeUnit.MILLISECONDS.sleep(200);
                elapsedTime += 200;
                if (elapsedTime > start + connectionTimeout) {
                    break;
                }
            } catch (Exception e) {
                return false;
            }
        }
        return false;
    }

    private void prepareChildrenData() throws Exception {
        for (String path : paths) {
            client.create().forPath(path + "/1");
        }
    }

    private void prepareData() throws Exception {
        Watcher watcher = new Watcher() {

            @Override
            public void process(WatchedEvent watchedEvent) {
            }
        };

        client.create().forPath(basePath);
        for (int i = 0; i < childNum; i++) {
            String childPath = basePath + i;
            client.create().forPath(childPath);
            client.getData().usingWatcher(watcher).forPath(childPath);
        }
    }

    @Test
    public void testLocker() throws IOException, InterruptedException {

        //start zk server
        startServerInstance();

        dropZKDB();

        //start client
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder().connectString(host + ":" + port).retryPolicy(
                new ExponentialBackoffRetry(200, 3)).connectionTimeoutMs(CONNECTION_TIMEOUT).sessionTimeoutMs(120000).namespace("zk-demo1");

        client = builder.build();
        client.start();


        int threadCount = 100;
        ExecutorService es = Executors.newFixedThreadPool(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final InterProcessMutex ipm = new InterProcessMutex(this.client, "/locks");
            final int seq = i;
            es.submit(new Runnable() {

                @Override
                public void run() {
                    boolean acquire = false;
                    try {
                        //only one can acquire the locker
                        acquire = ipm.acquire(20, TimeUnit.SECONDS);
                        if (acquire) {
                            LOG.info("i am thread No.:" + seq);
                            //waiting
                            TimeUnit.SECONDS.sleep(30);
                        } else {
                            LOG.info("acquired failed!, number:" + seq);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            if (acquire) {
                                LOG.info("i am released, number:" + seq);
                                ipm.release();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            });

        }

        es.shutdown();


        while (!es.awaitTermination(1, TimeUnit.SECONDS)) ;

        TimeUnit.SECONDS.sleep(300);

    }

    @Test
    public void testLocker2() throws Exception {

        //start zk server
        startServerInstance();

        dropZKDB();

        //start client
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder().connectString(host + ":" + port).retryPolicy(
                new ExponentialBackoffRetry(200, 3)).connectionTimeoutMs(CONNECTION_TIMEOUT).sessionTimeoutMs(120000).namespace("zk-demo1");

        client = builder.build();
        client.start();

        prepareData2();


        LOG.info("waiting ...");
        TimeUnit.SECONDS.sleep(20);


        LOG.info("start deleting...");
        deleteChildren();

        TimeUnit.SECONDS.sleep(300);

    }

    private void deleteChildren() throws InterruptedException {
        ExecutorService es = Executors.newFixedThreadPool(100);
        for (int i = 0; i < 100; i++) {

            final int seq = i;
            es.submit(new Runnable() {

                @Override
                public void run() {
                    try {
                        client.delete().forPath("/lock2/" + seq);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        es.shutdown();

        while (!es.awaitTermination(1, TimeUnit.SECONDS)) ;

    }

    private void prepareData2() throws Exception {
        final Watcher watcher = new Watcher() {

            @Override
            public void process(WatchedEvent watchedEvent) {
                LOG.info("received :" + watchedEvent.getPath());
            }
        };

        ExecutorService es = Executors.newFixedThreadPool(100);
        client.create().forPath("/lock2");
        for (int i = 0; i < 100; i++) {

            final int seq = i;
            es.submit(new Runnable() {

                @Override
                public void run() {
                    try {
                        client.create().forPath("/lock2/" + seq);
                        client.checkExists().usingWatcher(watcher).forPath("/lock2/" + seq);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });

        }

        es.shutdown();

        while (!es.awaitTermination(1, TimeUnit.SECONDS)) ;




    }


}
