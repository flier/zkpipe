package zkpipe;

public interface LogFileMBean {
    String getFilename();

    long getOffset();

    boolean isValid();

    boolean isClosed();

    long getPosition();

    long getFirstZxid();

    long getLastZxid();
}
