package com.example.simplesourceconnector;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

@Slf4j
public class SingleFileSourceTask extends SourceTask {

    /* 파일 이름과 해당 파일을 읽은 지점을 오프셋 소토리지에 저장하기 위해 filename과 position 값을 정의한다.
     * 이 2개의 키를 기준으로 오프셋 스토리지에 읽은 위치를 저장한다. */
    public final String FILENAME_FIELD = "filename";
    public final String POSITION_FIELD = "position";

    /* 오프셋 스토리지에 데이터를 저장하고 읽을 때는 Map 자료구조에 담은 데이터를 사용한다.
     * filename이 키, 커넥터가 읽는 파일 이름이 값으로 저장되어 사용된다. */
    private Map<String, String> fileNamePartition;
    private Map<String, Object> offset;

    private String topic;
    private String file;

    /* 읽은 파일의 위치를 커넥터 멤버 변수로 지정하여 사용한다.
     * 커넥터가 최초로 실행될 때 오프셋 스토리지에서 마지막으로 읽은 파일의 위치를 position 변수에 선언하여 중복 적재되지 않도록 할 수 있다.
     * 만약, 처음 읽는 파일이라면 position은 0으로 설정하여 처음부터 읽도록 한다. */
    private long position = -1;

    /**
     * 태스크 버전을 지정한다.
     * 보통 커넥터의 version() 메서드에서 지정한 버전과 동일한 버전으로 작성하는 것이 일반적이다.
     */
    @Override
    public String version() {
        return "1.0";
    }

    /**
     * 태스크가 시작할 때 필요한 로직을 작성한다.
     * 태스크는 실질적으로 데이터를 처리하는 역할을 하므로 데이터 처리에 필요한 모든 리소스를 여기서 초기화하면 좋다.
     * ex) JDBC 소스 커넥터를 구현한다면 이 메서드에서 JDBC 커넥션을 맺는다.
     */
    @Override
    public void start(Map<String, String> props) {
        try {
            /* Init variables
             * 커넥터 실행 시 받은 설정값을 SingleFileSourceConnectorConfig로 선언하여 사용한다.
             * 여기서는 토픽 이름과 읽을 파일 이름 설정값을 사용한다.
             * 토픽 이름과 파일 이름은 SingleFileSourceTask의 멤버 변수로 선언되어 있기 때문에 start() 메서드에서 초기화 이후에 다른 메서드에서 사용할 수 있다. */
            SingleFileSourceConnectorConfig config = new SingleFileSourceConnectorConfig(props);
            topic = config.getString(SingleFileSourceConnectorConfig.TOPIC_NAME);
            file = config.getString(SingleFileSourceConnectorConfig.DIR_FILE_NAME);

            fileNamePartition = Collections.singletonMap(FILENAME_FIELD, file);
            offset = context.offsetStorageReader().offset(fileNamePartition);

            /* Get file offset from offsetStorageReader
             * 오프셋 스토리지에서 현재 읽고자 하는 파일 정보를 가져온다.
             * 오프셋 스토리지는 실제로 데이터가 저장되는 곳으로,
             * - 단일 모드 커넥트: 로컬 파일로 저장
             * - 분산 모드 커넥트: 내부 토픽에 저장
             * 만약 오프셋 스토리지에서 데이터를 읽었을 때 null이 반환된다면 읽고자 하는 데이터가 없다는 뜻이다. */
            if (offset != null) {
                Object lastReadFileOffset = offset.get(POSITION_FIELD);
                if (lastReadFileOffset != null) {
                    // 오프셋 스토리지에서 가져온 마지막으로 처리한 지점을 position 변수에 할당한다.
                    position = (Long) lastReadFileOffset;
                }
            } else {
                /* 오프셋 스토리지에서 가져온 데이터가 null이라면 파일을 처리한 적이 없다는 뜻이므로 따로 position 변수에 0을 할당
                 * position이 0이면 파일의 첫째 줄부터 처리하여 토픽으로 데이터를 보낸다. */
                position = 0;
            }
        } catch (Exception e) {
            throw new ConnectException(e.getMessage(), e);
        }
    }

    /**
     * 소스 애플리케이션 또는 소스 파일로부터 데이터를 읽어오는 로직을 작성한다.
     * 데이터를 읽어오면 토픽으로 보낼 데이터를 SourceRecord로 정의한다.
     * SourceRecord는 토픽으로 데이터를 정의하기 위해 사용한다.
     * 태스크가 시작한 이후로 지속적으로 데이터를 가져오기 위해 반복적으로 호출되는 메서드다.
     */
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> results = new ArrayList<>();
        try {
            Thread.sleep(1000);

            List<String> lines = getLines(position);

            if (!lines.isEmpty()) {
                /* 토픽으로 보내기 전에 우선 파일에서 한 줄씩 읽어오는 과정을 진행해야 한다.
                 * 마지막으로 읽었던 지점 이후로 파일의 마지막 지점까지 읽어서 리턴하는 getLines() 메서드로 데이터를 받는다.
                 * SourceRecord 인스턴스를 만들 때는 마지막으로 전송한 데이터의 위치를 오프셋 스토리지에 저장하기 위해 앞서 선언한 fileNamePartition과 현재 토픽으로 보내는 줄의 위치를 기록한 sourceOffset를 파라미터로 넣어 선언하다. */
                lines.forEach(line -> {
                    Map<String, Long> sourceOffset = Collections.singletonMap(POSITION_FIELD, ++position);
                    SourceRecord sourceRecord = new SourceRecord(fileNamePartition, sourceOffset, topic, Schema.STRING_SCHEMA, line);
                    results.add(sourceRecord);
                });
            }
            return results;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new ConnectException(e.getMessage(), e);
        }
    }

    private List<String> getLines(long readLine) throws Exception {
        BufferedReader reader = Files.newBufferedReader(Paths.get(file));
        return reader.lines().skip(readLine).collect(Collectors.toList());
    }

    /**
     * 태스크가 종료될 때 필요한 로직을 작성한다.
     * ex) JDBC 소스 커넥터를 구현했다면 이 메서드에서 JDBC 커넥션을 종료하는 로직을 추가하면 된다.
     */
    @Override
    public void stop() {

    }
}
