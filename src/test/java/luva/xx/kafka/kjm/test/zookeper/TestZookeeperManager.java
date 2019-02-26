package luva.xx.kafka.kjm.test.zookeper;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.junit.Test;

import com.alibaba.fastjson.JSONObject;

import luva.xx.kafka.kjm.utils.ZookeeperManager;

/**
 * @author Hulva Luva.H
 * @date 2019年2月26日
 * @description
 *
 */
public class TestZookeeperManager {

	@Test
	public void test() throws IOException, InterruptedException, KeeperException {
		ZookeeperManager zkManager = new ZookeeperManager("10.16.238.101:8181,10.16.238.102:8181,10.16.238.103:8181");
		List<String> childrend = zkManager.getZkeeper().getChildren("/brokers/ids", false);
		System.out.println(childrend);
		for (String child : childrend) {
			byte[] nodeData = zkManager.getZkeeper().getData("/brokers/ids/" + child, false, null);
			System.out.println(nodeData);
			System.out.println(new String(nodeData, "UTF-8"));
		}
	}

	@Test
	public void getJMXUrls() {
		List<String> jmxUrls = new ArrayList<String>();
		try {
			ZookeeperManager zkManager = new ZookeeperManager("10.16.238.101:8181,10.16.238.102:8181,10.16.238.103:8181");
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
		System.out.println(jmxUrls);
	}

}
