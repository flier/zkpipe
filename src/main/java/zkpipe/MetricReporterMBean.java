package zkpipe;

public interface MetricReporterMBean {
    String getUri();

    long getInterval();

    void close();
}
