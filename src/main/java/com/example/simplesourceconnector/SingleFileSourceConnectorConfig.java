package com.example.simplesourceconnector;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

/**
 * 커넥터 실행 시 받을 설정값들을 정의한다.
 */
public class SingleFileSourceConnectorConfig extends AbstractConfig {

    // 파일 소스 커넥터는 어떤 파일을 읽을 것인지 정지정해야 한다. ➡ 옵션명 file로 파일 위치와 이름을 값으로 받는다.
    public static final String DIR_FILE_NAME = "file";
    private static final String DIR_FILE_NAME_DEFAULT_VALUE = "/tmp/kafka.txt";
    private static final String DIR_FILE_NAME_DOC = "읽을 파일 경로와 이름";

    // 읽은 파일을 어느 토픽으로 보낼 것인지 지정하기 위해 옵션명 topic으로 1개의 토픽값을 받는다.
    public static final String TOPIC_NAME = "topic";
    private static final String TOPIC_DEFAULT_VALUE = "test";
    private static final String TOPIC_DOC = "보낼 토픽 이름";

    /**
     * ConfigDef는 커넥터에서 사용할 옵션값들에 대한 정의를 표현하는 데 사용된다.
     * define() 메서드를 플루언트 스타일로 옵션 값을 지정할 수 있는데, 각 옵션값의 이름, 설명, 기본값, 중요도를 지정할 수 있다.
     * .
     * Importance를 HIGH< MEDIUM, LOW로 정하는 명확한 기준은 없다.
     * 단지 사용자로 하여금 이 옵션이 중요하다는 것을 명시적으로 표시하기 위한 문서로 사용할 뿐이다.
     */
    public static ConfigDef CONFIG = new ConfigDef().define(
            DIR_FILE_NAME,
            ConfigDef.Type.STRING,
            DIR_FILE_NAME_DEFAULT_VALUE,
            ConfigDef.Importance.HIGH,
            DIR_FILE_NAME_DOC
    ).define(
            TOPIC_NAME,
            ConfigDef.Type.STRING,
            TOPIC_DEFAULT_VALUE,
            ConfigDef.Importance.HIGH,
            TOPIC_DOC
    );


    public SingleFileSourceConnectorConfig(Map<String, String> originals) {
        super(CONFIG, originals);
    }
}
