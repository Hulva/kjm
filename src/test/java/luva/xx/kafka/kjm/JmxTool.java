package luva.xx.kafka.kjm;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.management.Attribute;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpecBuilder;

/**
 * @author Hulva Luva.H
 * @date 2019年2月21日
 * @description
 *
 */
public class JmxTool {
	public static void main(String[] args) throws IOException, InterruptedException {
		OptionParser parser = new OptionParser();
		ArgumentAcceptingOptionSpec<String> objectNameOpt = parser.accepts("object-name",
				"A JMX object name to use as a query. This can contain wild cards, and this option "
						+ "can be given multiple times to specify more than one query. If no objects are specified "
						+ "all objects will be queried.")
				.withRequiredArg().describedAs("name").ofType(String.class);
		ArgumentAcceptingOptionSpec<String> attributesOpt = parser
				.accepts("attributes",
						"The whitelist of attributes to query. This is a comma-separated list. If no "
								+ "attributes are specified all objects will be queried.")
				.withRequiredArg().describedAs("name").ofType(String.class);
		ArgumentAcceptingOptionSpec<Integer> reportingIntervalOpt = parser
				.accepts("reporting-interval", "Interval in MS with which to poll jmx stats.").withRequiredArg()
				.describedAs("ms").ofType(Integer.class).defaultsTo(2000);
		OptionSpecBuilder helpOpt = parser.accepts("help", "Print usage information.");
		ArgumentAcceptingOptionSpec<String> dateFormatOpt = parser
				.accepts("date-format",
						"The date format to use for formatting the time field. "
								+ "See java.text.SimpleDateFormat for options.")
				.withRequiredArg().describedAs("format").ofType(String.class);
		ArgumentAcceptingOptionSpec<String> jmxServiceUrlOpt = parser
				.accepts("jmx-url",
						"The url to connect to to poll JMX data. See Oracle javadoc for JMXServiceURL for details.")
				.withRequiredArg().describedAs("service-url").ofType(String.class)
				.defaultsTo("service:jmx:rmi:///jndi/rmi://:9999/jmxrmi");
		if (args.length == 0) {
			parser.printHelpOn(System.out);
			System.exit(0);
		}

		OptionSet options = parser.parse(args);

		if (options.has(helpOpt)) {
			parser.printHelpOn(System.out);
			System.exit(0);
		}

		JMXServiceURL url = new JMXServiceURL(options.valueOf(jmxServiceUrlOpt));
		int interval = options.valueOf(reportingIntervalOpt).intValue();
		boolean attributesWhitelistExists = options.has(attributesOpt);
		List<String> attributesWhitelist = new ArrayList<>();
		if (attributesWhitelistExists) {
			attributesWhitelist = Arrays.asList(options.valueOf(attributesOpt).split(","));
		}
		boolean dateFormatExists = options.has(dateFormatOpt);
		SimpleDateFormat dateFormat = null;
		if (dateFormatExists) {
			dateFormat = new SimpleDateFormat(options.valueOf(dateFormatOpt));
		}

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

		List<ObjectName> queries = null;
		if (options.has(objectNameOpt)) {
			queries = options.valuesOf(objectNameOpt).stream().map(on -> {
				try {
					return new ObjectName(on);
				} catch (MalformedObjectNameException e) {
					e.printStackTrace();
				}
				return null;
			}).collect(Collectors.toList());
		}

		List<ObjectName> names = new ArrayList<>();
		if (queries != null) {
			for (ObjectName name : queries) {
				try {
					names.addAll(mbsc.queryNames(name, null));
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		} else {
			try {
				names.addAll(mbsc.queryNames(null, null));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		Map<ObjectName, Integer> numExpectedAttributes = new HashMap<>();
		if (attributesWhitelistExists) {
			for (ObjectName name : queries) {
				numExpectedAttributes.put(name, attributesWhitelist.size());
			}
		} else {
			for (ObjectName name : names) {
				MBeanInfo mbean;
				try {
					mbean = mbsc.getMBeanInfo(name);
					numExpectedAttributes.put(name, mbean.getAttributes().length);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		// print csv header
		Set<String> attributeSet = queryAttributes(mbsc, names, attributesWhitelist).keySet();
		List<String> keys = new ArrayList<String>();
		keys.add("time");
		keys.addAll(attributeSet);

		System.out.println(keys.size() + " " + numExpectedAttributes.values().size());
//		if (keys.size() == numExpectedAttributes.values().size()) {
		System.out.println(keys.stream().map(key -> "\"" + key + "\"").collect(Collectors.joining(",")));
//		}

		while (true) {
			long start = System.currentTimeMillis();
			HashMap<String, Object> attributes = queryAttributes(mbsc, names, attributesWhitelist);
			if (dateFormat != null) {
				attributes.put("time", dateFormat.format(new Date()));
			} else {
				attributes.put("time", System.currentTimeMillis());
			}
//			if (attributes.keySet().size() == numExpectedAttributes.values().size()) {
			System.out
					.println(keys.stream().map(key -> attributes.get(key).toString()).collect(Collectors.joining(",")));
//			}
			long sleep = interval - (System.currentTimeMillis() - start);
			if (sleep < 0) {
				sleep = 0l;
			}
			Thread.sleep(sleep);
		}
	}

	private static HashMap<String, Object> queryAttributes(MBeanServerConnection mbsc, List<ObjectName> names,
			List<String> attributesWhitelist) {
		HashMap<String, Object> attributes = new HashMap<String, Object>();
		for (ObjectName name : names) {
			MBeanInfo mbean;
			try {
				mbean = mbsc.getMBeanInfo(name);
				MBeanAttributeInfo[] mbeanAttributes = mbean.getAttributes();
				List<String> nameList = Arrays.asList(mbeanAttributes).stream()
						.map(mbeanAttribute -> mbeanAttribute.getName()).collect(Collectors.toList());
				String[] nameArray = new String[nameList.size()];
				List<Attribute> attributeList = mbsc.getAttributes(name, nameList.toArray(nameArray)).asList();
				attributeList.forEach(attribute -> {
					if (attributesWhitelist != null && attributesWhitelist.size() > 0) {
						if (attributesWhitelist.contains(attribute.getName())) {
							attributes.put(name + ":" + attribute.getName(), attribute.getValue());
						}
					} else {
						attributes.put(name + ":" + attribute.getName(), attribute.getValue());
					}
					System.out.println(name + ":" + attribute.getName() + " -> " + attribute.getValue());
				});
			} catch (Exception exception) {
			}
		}
		return attributes;
	}
}
