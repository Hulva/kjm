package luva.xx.kafka.kjm;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import luva.xx.kafka.kjm.common.thread.WorkerThreadFactory;
import luva.xx.kafka.kjm.utils.ZookeeperManager;

/**
 * @author Hulva Luva.H
 * @date 2019年2月22日
 * @description
 *
 */
public class JMXCollector {
	public static final int DEFAULT_THREAD_POOL_SIZE = Runtime.getRuntime().availableProcessors();
	private static final ExecutorService kafkaJMXInfoCollectAndSavePool = Executors.newFixedThreadPool(DEFAULT_THREAD_POOL_SIZE,
			new WorkerThreadFactory("JMX-Collector"));

//	private static ScheduledExecutorService scheduler = null;
//
//	scheduler=Executors.newScheduledThreadPool(2,new WorkerThreadFactory("FixedRateSchedule"));

	private JMXCollector() {

	}

	private List<String> getKafkaJMXUrls(String zkHost) {
		List<String> jmxUrls = new ArrayList<String>();
		try {
			ZookeeperManager zkManager = new ZookeeperManager(zkHost);
			List<String> childrend = zkManager.getZkeeper().getChildren("/brokers/ids", false);
			byte[] nodeData = null;
			for (String child : childrend) {
				nodeData = zkManager.getZkeeper().getData("/brokers/ids/" + child, false, null);
				JSONObject jsonBeokerInfo = JSONObject.parseObject(new String(nodeData, "UTF-8"));
				jmxUrls.add("service:jmx:rmi:///jndi/rmi://" + jsonBeokerInfo.getString("host") + ":"
						+ jsonBeokerInfo.getIntValue("jmx_port") + "/jmxrmi");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return jmxUrls;
	}

	public void doCollect() {
//		scheduler.scheduleAtFixedRate(new Runnable() {
//
//			@Override
//			public void run() {
//				try {
//					List<String> groups = og.getGroups();
//					groups.addAll(SystemManager.og.getGroupsCommittedToBroker());sta
//					groups.forEach(group -> {
//						kafkaInfoCollectAndSavePool.submit(new GenerateKafkaInfoTask(group));
//					});
//				} catch (Exception e) {
//					LOG.warn("Ops..." + e.getMessage());
//				}
//			}
//		}, 1000, config.getDataCollectFrequency(), TimeUnit.SECONDS);
	}
}
