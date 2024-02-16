package org.apache.sis.cloud.aws.s3;

import software.amazon.awssdk.services.s3.S3Client;

import java.util.Objects;

/**
 * File System Key stored in {@link FileService#fileSystems},
 *
 * @author  Quentin Bialota (Geomatys)
 */
public class ClientFileSystemKey {

    /**
     * The S3 access key
     */
    private final String accessKey;

    /**
     * The S3 host (if not stored on Amazon Infrastructure)
     */
    private final String host;

    /**
     * The S3 port (if not stored on Amazon Infrastructure)
     */
    private final int port;

    /**
     * Creates a new file system key for the {@link FileService} with access key, host and port
     *
     * @param accessKey   the S3 access key for this file system.
     * @param host        the host or {@code null} for aws request
     * @param port        the port or {@code -1} for aws request
     */
    public ClientFileSystemKey(String accessKey, String host, int port) {
        this.accessKey = accessKey;
        this.host = host;
        this.port = port;
    }

    /**
     * Returns the access key as a string
     */
    public String getAccessKey() {
        return accessKey;
    }

    /**
     * Returns the host as a string
     */
    public String getHost() {
        return host;
    }

    /**
     * Returns the port as an integer
     */
    public int getPort() {
        return port;
    }

    /**
     * Indicates whether some other object is "equal to" this one.
     *
     * @param o The reference object with which to compare.
     * @return {@code true} if this object is the same as the o argument; {@code false} otherwise.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClientFileSystemKey that = (ClientFileSystemKey) o;
        return Objects.equals(accessKey, that.accessKey) && Objects.equals(host, that.host) && Objects.equals(port, that.port);
    }

    /**
     * Returns a hash code value for the object.
     *
     * @return A hash code value for this object.
     */
    @Override
    public int hashCode() {
        return Objects.hash(accessKey, host, port);
    }
}
