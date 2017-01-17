package zkpipe;

public interface LogFileMBean {
    String getFilename();

    Boolean isValidated();

    Boolean isClosed();

    long getPosition();

    long getFirstZxid();

    long getLastZxid();
}
