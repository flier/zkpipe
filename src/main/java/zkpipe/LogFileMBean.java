package zkpipe;

public interface LogFileMBean {
    String getFilename();

    long getOffset();

    boolean isLog();

    boolean isSnapshot();

    boolean isClosed();

    long getPosition();

    long getFirstZxid();

    long getLastZxid();
}
