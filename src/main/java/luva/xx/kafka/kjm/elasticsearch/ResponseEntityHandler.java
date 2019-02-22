package luva.xx.kafka.kjm.elasticsearch;

import org.springframework.http.ResponseEntity;

/**
 * @author Hulva Luva.H
 * @date 2019年2月22日
 * @description
 *
 */
public interface ResponseEntityHandler<T> {

	void doWithResponseEntity(ResponseEntity<T> response);

}
