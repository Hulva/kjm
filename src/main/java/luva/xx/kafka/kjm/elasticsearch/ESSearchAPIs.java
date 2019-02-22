package luva.xx.kafka.kjm.elasticsearch;

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
public class ESSearchAPIs {

	public void search(RestTemplate restTemplate, String esHost, String index, String queryBody,
			ResponseEntityHandler<String> responseEntityHandler) throws Exception {
		try {
			ResponseEntity<String> response = restTemplate.exchange(esHost + "/" + index + "/_search", HttpMethod.POST,
					new HttpEntity<String>(queryBody, RestTemplateConfig.HEADERS), String.class);
			responseEntityHandler.doWithResponseEntity(response);
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage());
		}
	}
}
