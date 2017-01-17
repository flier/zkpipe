package zkpipe;

public interface KafkaBrokerMBean {
    String getUri();

    String getTopic();

    void close();
}
