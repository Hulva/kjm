package luva.xx.kafka.kjm.test.elasticsearch;

import static org.junit.Assert.fail;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.client.RestTemplate;

import luva.xx.kafka.kjm.KjmApplication;
import luva.xx.kafka.kjm.elasticsearch.ESDocAPIs;
import luva.xx.kafka.kjm.elasticsearch.ResponseEntityHandler;

/**
 * @author Hulva Luva.H
 * @date 2019年2月22日
 * @description
 *
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = KjmApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class TestESDocAPIs {
	private static final String ES_HOST = "http://10.16.238.103:8200";
	@Autowired
	RestTemplate restTemplate;

	/**
	 * Test method for
	 * {@link luva.xx.kafka.kjm.elasticsearch.ESDocAPIs#insertDoc(org.springframework.web.client.RestTemplate, java.lang.String, java.lang.String, java.lang.String, java.lang.String, luva.xx.kafka.kjm.elasticsearch.ResponseEntityHandler)}.
	 */
	@Test
	public void testInsertDocWithSepecificID() {
		try {
			ESDocAPIs.insertDoc(restTemplate, ES_HOST, "test",
					"{\"user\" : \"kimchy\",\"post_date\" : \"2009-11-15T14:12:12\",\"message\" : \"trying out Elasticsearch\"}",
					"1", new ResponseEntityHandler<String>() {

						@Override
						public void doWithResponseEntity(ResponseEntity<String> response) {
							System.out.println(response.getBody());
						}
					});
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Test method for
	 * {@link luva.xx.kafka.kjm.elasticsearch.ESDocAPIs#insertDoc(org.springframework.web.client.RestTemplate, java.lang.String, java.lang.String, java.lang.String, luva.xx.kafka.kjm.elasticsearch.ResponseEntityHandler)}.
	 */
	@Test
	public void testInsertDoc() {
		fail("Not yet implemented");
	}

	/**
	 * Test method for
	 * {@link luva.xx.kafka.kjm.elasticsearch.ESDocAPIs#bulkDoc(org.springframework.web.client.RestTemplate, java.lang.String, java.lang.String, java.util.Set, luva.xx.kafka.kjm.elasticsearch.ResponseEntityHandler)}.
	 */
	@Test
	public void testBulkDoc() {
		fail("Not yet implemented");
	}

}
