package luva.xx.kafka.kjm.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

/**
 * @author Hulva Luva.H
 * @date 2019年2月22日
 * @description
 *
 */
@Configuration
public class RestTemplateConfig {

	public static HttpHeaders HEADERS = null;
	static {
		HEADERS = new HttpHeaders();
		HEADERS.add("Content-Type", "application/json");
		HEADERS.add("Accept", "*/*");
	}

	@Bean
	public RestTemplate restTemplate(ClientHttpRequestFactory factory) {
		return new RestTemplate(factory);
	}

	@Bean
	public ClientHttpRequestFactory simpleClientHttpRequestFactory() {
		SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
		factory.setReadTimeout(5000); // ms
		factory.setConnectTimeout(15000); // ms
		return factory;
	}
}
