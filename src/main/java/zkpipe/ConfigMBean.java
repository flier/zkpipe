package zkpipe;

public interface ConfigMBean {
    String getMode();

    String getLoggingLevel();

    String getLogFiles();

    String getLogDirectory();

    String getZxidRange();

    String getPathPrefix();

    String getMatchPattern();

    long getSessionId();

    boolean isIgnoreSession();

    boolean isFromLatest();

    String getKafkaUri();

    String getMessageFormat();

    int getSendQueueSize();

    String getMetricServerUri();

    String getReportUri();

    long getPushInterval();

    boolean isCheckCrc();

    boolean isJvmMetrics();

    boolean isHttpMetrics();
}

