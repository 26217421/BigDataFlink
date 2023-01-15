package cn.skyhor.gmalllogger.web;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @author wbw
 * @date 2023-1-13 15:11
 */
@Controller
@Slf4j
public class LoggerController {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public LoggerController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }


    @ResponseBody
    @GetMapping(path = "applog")
    public ResponseEntity<String> getLogger(@RequestParam("param")String jsonStr) {
        log.info(jsonStr);
        kafkaTemplate.send("ods_base_log",jsonStr);
        return ResponseEntity.ok("Success");
    }
}
