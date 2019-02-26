package luva.xx.kafka.kjm.utils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Hulva Luva.H
 * @date 2019年2月22日
 * @description
 *
 */
public class ZookeeperManager {
	private static Logger LOG = LoggerFactory.getLogger(ZookeeperManager.class);
	private static ZooKeeper zkeeper;
	CountDownLatch connectionLatch = new CountDownLatch(1);

	public ZookeeperManager(String zkHost) throws IOException, InterruptedException {
		connect(zkHost);
	}

	private ZooKeeper connect(String host) throws IOException, InterruptedException {
		zkeeper = new ZooKeeper(host, 2000, new Watcher() {
			public void process(WatchedEvent we) {
				if (we.getState() == KeeperState.SyncConnected) {
					connectionLatch.countDown();
				}
			}
		});

		connectionLatch.await();
		return zkeeper;
	}

	public ZooKeeper getZkeeper() {
		return zkeeper;
	}

	public void closeConnection() {
		try {
			if (zkeeper != null) {
				zkeeper.close();
			}
		} catch (InterruptedException e) {
			LOG.error("ZKUtils close() error! ", e);
		}
	}

	public String getZNodeData(String path) throws KeeperException, InterruptedException, UnsupportedEncodingException {
		byte[] b = null;
		b = zkeeper.getData(path, null, null);
		return new String(b, "UTF-8");
	}
}
