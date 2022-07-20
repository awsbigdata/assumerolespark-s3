package software.zip.s3;

/**
 * Abstraction of system time function for testing purpose
 */
interface TimeProvider {

    /**
     * See {@link System#currentTimeMillis}
     */
    long currentTimeMillis();
}