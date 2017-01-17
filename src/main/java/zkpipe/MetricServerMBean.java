package zkpipe;

public interface MetricServerMBean {
    String getUri();

    boolean isHttpMetrics();

    void close();
}
