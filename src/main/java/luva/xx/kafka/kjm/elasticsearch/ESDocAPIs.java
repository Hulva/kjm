package luva.xx.kafka.kjm.elasticsearch;

import java.util.Set;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import luva.xx.kafka.kjm.config.RestTemplateConfig;

/**
 * @author Hulva Luva.H
 * @date 2019年2月22日
 * @description
 *
 */
public class ESDocAPIs {

	// Single document APIs
	// 1. Index API
	/**
	 * offer a specific document id
	 * 
	 * @param restTemplate
	 * @param esHost
	 * @param index
	 * @param document
	 * @param docID
	 * @param responseEntityHandler
	 */
	public static void insertDoc(RestTemplate restTemplate, String esHost, String index, String document, String docID,
			ResponseEntityHandler<String> responseEntityHandler) throws Exception {
		try {
			ResponseEntity<String> response = restTemplate.exchange(esHost + "/" + index + "/_doc/" + docID,
					HttpMethod.PUT, new HttpEntity<String>(document), String.class);
			responseEntityHandler.doWithResponseEntity(response);
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage());
		}
	}

	/**
	 * automatically generated document id
	 * 
	 * @param restTemplate
	 * @param esHost
	 * @param index
	 * @param document
	 * @param responseEntityHandler
	 */
	public static void insertDoc(RestTemplate restTemplate, String esHost, String index, String document,
			ResponseEntityHandler<String> responseEntityHandler) throws Exception {
		try {
			ResponseEntity<String> response = restTemplate.exchange(esHost + "/" + index + "/_doc", HttpMethod.POST,
					new HttpEntity<String>(document, RestTemplateConfig.HEADERS), String.class);
			responseEntityHandler.doWithResponseEntity(response);
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage());
		}
	}

	// 2. Get API

	// 3. Delete API

	// 4. Update API

	// Multi-document APIs
	// 1. Multi Get API

	// 2. Bulk API
	public static void bulkDoc(RestTemplate restTemplate, String esHost, String index, Set<String> documents,
			ResponseEntityHandler<String> responseEntityHandler) throws Exception {
		try {
			StringBuilder bulkData = new StringBuilder();
			for (String document : documents) {
				bulkData.append("{\"index\": {\"_index\":\"" + index + "\",\"_type\":\"_doc\"}}").append("\n");
				bulkData.append(document).append("\n");
			}
			ResponseEntity<String> response = restTemplate.exchange(esHost + "/_bulk", HttpMethod.POST,
					new HttpEntity<String>(bulkData.toString(), RestTemplateConfig.HEADERS), String.class);
			responseEntityHandler.doWithResponseEntity(response);
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage());
		}
	}
}
