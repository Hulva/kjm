package luva.xx.kafka.kjm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

/**
 * 
 * @author Hulva Luva.H
 * @since 2017-07-25
 *
 */
public class ObjectNameManagerTest {

	public static void main(String[] args) throws InterruptedException, IOException {
		List<String> jmxUrls = new ArrayList<String>();
		jmxUrls.add("service:jmx:rmi:///jndi/rmi://10.16.238.101:8888/jmxrmi");
		jmxUrls.add("service:jmx:rmi:///jndi/rmi://10.16.238.102:8888/jmxrmi");
		jmxUrls.add("service:jmx:rmi:///jndi/rmi://10.16.238.103:8888/jmxrmi");
		jmxUrls.add("service:jmx:rmi:///jndi/rmi://10.16.238.104:8888/jmxrmi");

		for (String jmxUrl : jmxUrls) {
			JMXServiceURL url = new JMXServiceURL(jmxUrl);
			JMXConnector jmxc = null;
			MBeanServerConnection mbsc = null;
			int retries = 0;
			int maxNumRetries = 10;
			boolean connected = false;
			while (retries < maxNumRetries && !connected) {
				try {
					System.err.println("Trying to connect to JMX url: " + url);
					jmxc = JMXConnectorFactory.connect(url, null);
					mbsc = jmxc.getMBeanServerConnection();
					connected = true;
				} catch (Exception e) {
					System.err.println("Could not connect to JMX url: " + url + ". Exception " + e.getMessage());
					e.printStackTrace();
					retries += 1;
					Thread.sleep(500);
				}
			}

			if (!connected) {
				System.err.println("Could not connect to JMX url " + url + " after " + maxNumRetries + " retries.");
				System.err.println("Exiting.");
				System.exit(1);
			}
			List<ObjectName> names = new ArrayList<>();
			names.addAll(mbsc.queryNames(null, null));
			System.out.println(jmxUrl);
			System.out.println(names.size());
			names.forEach(name -> {
//				String objectNameValue = name.toString();
				System.out.println(name.getDomain() + " = " + name.getKeyPropertyList());
			});
//			System.out.println();
			System.out.println("==============================================");
		}
	}

}
