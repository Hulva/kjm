package luva.xx.kafka.kjm;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

	private void getKafkaJMXUrls(String zkHost) {
		try {
			ZookeeperManager zkManager = new ZookeeperManager(zkHost);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void doCollect() {
//		scheduler.scheduleAtFixedRate(new Runnable() {
//
//			@Override
//			public void run() {
//				try {
//					List<String> groups = og.getGroups();
//					groups.addAll(SystemManager.og.getGroupsCommittedToBroker());
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
