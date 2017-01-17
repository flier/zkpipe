package zkpipe;

public interface ConfigMBean {
    String getMode();

    String getLogFiles();

    String getLogDirectory();

    String getZxidRange();

    boolean isFromLatest();

    String getPathPrefix();

    String getKafkaUri();

    String getMetricServerUri();

    String getReportUri();

    long getPushInterval();

    boolean isCheckCrc();

    boolean isJvmMetrics();

    boolean isHttpMetrics();

    String getMessageFormat();
}

