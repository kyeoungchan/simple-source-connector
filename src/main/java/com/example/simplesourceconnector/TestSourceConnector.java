package com.example.simplesourceconnector;

import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

/**
 * 어떻게 사용되는 커넥터인지 알 수 있게 이름을 적어야 한다.
 * ex) MongoDbSourceConnector
 */
public class TestSourceConnector extends SourceConnector {

    /**
     * 사용자가 JSON 또는 config 파일 형태로 입력한 설정 값을 초기화 하는 메서드다.
     * 만약 올바른 값이 아니라면 여기서 ConnectException()을 호출하여 커넥터를 종료할 수 있다.
     */
    @Override
    public void start(Map<String, String> map) {

    }

    /**
     * 이 커넥터가 사용할 태스크 클래스를 지정한다.
     */
    @Override
    public Class<? extends Task> taskClass() {
        return null;
    }

    /**
     * 태스크 개수가 2개 이상일 경우 태스크마다 각기 다른 옵션을 설정할 때 사용한다.
     */
    @Override
    public List<Map<String, String>> taskConfigs(int i) {
        return List.of();
    }

    /**
     * 커넥터가 사용할 설정값에 대한 정보를 받는다.
     */
    @Override
    public ConfigDef config() {
        return null;
    }

    /**
     * 커넥터가 종료될 때 필요한 로직을 작성한다.
     */
    @Override
    public void stop() {

    }

    /**
     * @return 커넥터의 버전을 리턴한다.
     */
    @Override
    public String version() {
        return "";
    }
}
