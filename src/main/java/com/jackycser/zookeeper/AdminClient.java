package com.jackycser.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Date;

/**
 * Created by Jacky on 10/10/2016.
 */
public class AdminClient implements Watcher {
    private ZooKeeper zk;
    private String hostport;

    public AdminClient(String hostport) {
        this.hostport = hostport;
    }

    private void start() throws IOException {
        zk = new ZooKeeper(hostport, 15000, this);
    }

    private void listState() throws KeeperException, InterruptedException {
        try {
            Stat stat = new Stat();
            byte masterData[] = zk.getData("/master", false, stat);
            Date startDate = new Date(stat.getCtime());
            System.out.println("Master: ");
            System.out.println("\tMaster: " + new String(masterData) + " since: " + startDate);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }

        System.out.println("Workers: ");
        for (String w : zk.getChildren("/workers", false)) {
            byte data[] = zk.getData("/workers/" + w, false, null);
            String state = new String(data);
            System.out.println("\t" + w + ": " + state);
        }

        System.out.println("Tasks: ");
        for (String t : zk.getChildren("/assign", false)) {
            System.out.println("\t" + t);
        }
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println("process, " + event);
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        String hostPort = "127.0.0.1:2181";
        AdminClient adminClient = new AdminClient(hostPort);
        adminClient.start();
        adminClient.listState();
    }
}
