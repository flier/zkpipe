package zkpipe;

public interface LogFileMBean {
    String getFilename();

    Boolean isValid();

    Boolean isClosed();

    long getPosition();

    long getFirstZxid();

    long getLastZxid();
}
