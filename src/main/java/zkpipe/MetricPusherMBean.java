package zkpipe;

public interface MetricPusherMBean {
    String getAddress();

    long getInterval();

    void flush();

    void close();
}
