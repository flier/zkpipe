package zkpipe;

public interface ConfigMBean {
    String getMode();

    String getLogFiles();

    String getLogDirectory();

    String getZxidRange();

    String getPathPrefix();

    String getMatchPattern();

    boolean isFromLatest();

    String getKafkaUri();

    String getMetricServerUri();

    String getReportUri();

    long getPushInterval();

    boolean isCheckCrc();

    boolean isJvmMetrics();

    boolean isHttpMetrics();

    String getMessageFormat();
}

