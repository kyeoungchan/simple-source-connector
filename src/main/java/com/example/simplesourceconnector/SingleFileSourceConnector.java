package com.example.simplesourceconnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

/**
 * SingleFileSourceConnector는 커넥트에서 사용할 커넥터 이름이 된다.
 * 플러그인으로 추가하여 사용 시에는 패키지 이름과 함께 붙여서 사용된다.
 */
public class SingleFileSourceConnector extends SourceConnector {

    private Map<String, String> configProperties;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        this.configProperties = props;
        try {
            // 커넥트에서 SingleFileSourceConnector 커넥터를 생성할 때 받은 설정값들을 초기화한다.
            new SingleFileSourceConnectorConfig(props);
        } catch (ConfigException e) {
            // 설정을 초기화할 때 필수 설정값이 빠져있다면 ConnectException 발생
            throw new ConnectException(e.getMessage(), e);
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SingleFileSourceTask.class;
    }

    /**
     * 태스크가 2개 이상인 경우 태스크마다 다른 설정값을 줄 때 사용한다.
     * 여기서는 태스크가 2개 이상이더라도 동일한 설정값을 받도록 ArrayList에 모두 동일한 설정을 담았다.
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        Map<String, String> taskProps = new HashMap<>();
        taskProps.putAll(configProperties);

        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    /**
     * 커넥터가 사용할 설정값에 대한 정보를 받는다.
     */
    @Override
    public ConfigDef config() {
        return SingleFileSourceConnectorConfig.CONFIG;
    }

    /**
     * 태스크가 종료될 때 필요한 로직을 작성한다.
     * ex) JDBC 소스 커넥터를 구현했다면 이 메서드에서 JDBC ㅓㅋ넥션을 종료하는 로직을 추가하면 된다.
     * 여기서는 해제해야할 리소스가 없으므로 빈칸으로 둔다.
     */
    @Override
    public void stop() {
    }
}
