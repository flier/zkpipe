package zkpipe;

public interface ConfigMBean {
    String getMode();

    String getLogFiles();

    String getLogDirectory();

    String getZxidRange();

    Boolean isFromLatest();

    String getPathPrefix();

    String getKafkaUri();

    String getMetricServerUri();

    String getReportUri();

    long getPushInterval();

    Boolean isCheckCrc();

    Boolean isJvmMetrics();

    Boolean isHttpMetrics();

    String getMessageFormat();
}

