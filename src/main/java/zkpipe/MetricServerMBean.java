package zkpipe;

public interface MetricServerMBean {
    String getUri();

    Boolean isHttpMetrics();

    void close();
}
