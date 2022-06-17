package Zookeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;


public class ZKConnection {
	private ZooKeeper zoo;
	CountDownLatch connectionLatch = new CountDownLatch(1);

	/**
	 * Constructs a Zookeeper instance with a default watcher 
	 * @param host - the host
	 * @return the Zookeeper that was created
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public ZooKeeper connect(String host) throws IOException, InterruptedException {
		zoo = new ZooKeeper(host, 1000, new Watcher() {
			public void process(WatchedEvent we) {
				if (we.getState() == KeeperState.SyncConnected) {
					//connectionLatch.countDown();
				}
			}
		});
		//connectionLatch.await();
		return zoo;
	}
	
	/**
	 * Creates a Zookeeper instance given an host and a watcher
	 * @param host the host
	 * @param watcher the watcher
	 * @return the created zookeeper
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public ZooKeeper connect(String host, Watcher watcher) throws IOException, InterruptedException {
		zoo = new ZooKeeper(host,5000,watcher);
		return zoo;
	}

	/**
	 * Closes the zookeeper
	 * @throws InterruptedException
	 */
	public void close() throws InterruptedException {
		zoo.close();
	}
}