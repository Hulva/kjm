package luva.xx.kafka.kjm.utils;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Hulva Luva.H
 * @date 2019年2月22日
 * @description
 *
 */
public class ZKConnection {
	private static Logger LOG = LoggerFactory.getLogger(ZKConnection.class);
	private ZooKeeper zoo;
	CountDownLatch connectionLatch = new CountDownLatch(1);

	public ZooKeeper connect(String host) throws IOException, InterruptedException {
		zoo = new ZooKeeper(host, 2000, new Watcher() {
			public void process(WatchedEvent we) {
				if (we.getState() == KeeperState.SyncConnected) {
					connectionLatch.countDown();
				}
			}
		});

		connectionLatch.await();
		return zoo;
	}

//	public static List<String> getJMXUrls() throws Exception {
//		List<String> kafkaHosts = new ArrayList<String>();
//		List<String> ids = getChildren("/brokers/ids");
//		for (String id : ids) {
//			try {
//				String brokerInfo = new String(readDataMaybeNull("/brokers/ids/" + id).getData());
//
//				BrokerInfo bi = new BrokerInfo();
//				bi.setBid(Integer.parseInt(id));
//				JSONObject jsonObj = new JSONObject(brokerInfo);
//				if (jsonObj.has("host")) {
//					bi.setHost(jsonObj.get("host").toString());
//				}
//				if (jsonObj.has("port")) {
//					bi.setPort(Integer.parseInt(jsonObj.get("port").toString()));
//				}
//				if (jsonObj.has("jmx_port")) {
//					bi.setJmxPort(Integer.parseInt(jsonObj.get("jmx_port").toString()));
//				}
//				if (jsonObj.has("version")) {
//					bi.setVersion(Integer.parseInt(jsonObj.get("version").toString()));
//				}
//				if (jsonObj.has("timestamp")) {
//					bi.setTimestamp(Long.parseLong(jsonObj.get("timestamp").toString()));
//				}
//				kafkaHosts.add(bi);
//			} catch (Exception e) {
//				LOG.error("Zookeeper borker getting exception {}", e);
//			}
//		}
//		return kafkaHosts;
//	}

	public void close() {
		try {
			if (zoo != null) {
				zoo.close();
			}
		} catch (InterruptedException e) {
			LOG.error("ZKUtils close() error! ", e);
		}
	}
}
