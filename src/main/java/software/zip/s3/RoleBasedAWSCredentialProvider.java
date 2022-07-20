package software.zip.s3;

import com.amazonaws.auth.*;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;

import java.net.URI;
import java.util.Date;
import java.util.Map;


/**
 * An AWS credential provider that allows hadoop cluster to assume a role when read/write
 * from S3 buckets.
 * <p>
 * The credential provider extracts from the URI the AWS role ARN that it should assume.
 * To encode the role ARN, we use a custom URI query key-value pair. The key is defined as
 * a constant {@link RoleBasedAWSCredentialProvider#AWS_ROLE_ARN_KEY}, the value is encoded
 * AWS role ARN string.
 * If the ARN string does not present, then fallback to default AWS credentials provider
 * </p>
 */
public class RoleBasedAWSCredentialProvider implements AWSCredentialsProvider, Configurable {


    private String accessKey;
    private String secretKey;
    private String sessionToken;

    /**
     * URI query key for the ARN string
     */
    public final static String AWS_ROLE_ARN_KEY = "amz-assume-role-arn";


    /**
     * URI query key for the ARN string
     */
    public final static String S3_BUCKET_URI = "s3_bucket_uri";


    /**
     * Time before expiry within which credentials will be renewed.
     */
    private static final int EXPIRY_TIME_MILLIS = 60 * 1000;

    /**
     * Life span of the temporary credential requested from STS
     */
    private static final int DURATION_TIME_SECONDS = 3600;

    /**
     * The original url containing a AWS role ARN in the query parameter.
     */
    private final URI uri;



    /**
     * Abstraction of System time function for testing purpose
     */
    private final TimeProvider timeProvider;

    /**
     * The expiration time for the current session credentials.
     */
    private Date sessionCredentialsExpiration;

    /**
     * The current session credentials.
     */
    private AWSCredentials sessionCredentials;

    /**
     * The arn of the role to be assumed.
     */
    private String roleArn;

    /**
     * Environment variable that may contain role to assume
     */
    private final String ROLE_ENV_VAR = "AWS_ROLE_ARN_KEY";

    // the S3 bucket URI for the other account
    private  String bucket_URI;

    /**
     * AWS authentication environment variables
     */
    private final String AWS_SECRET_KEY_ID = "AWS_SECRET_KEY_ID";
    private final String AWS_ACCESS_KEY  = "AWS_ACCESS_KEY";
    private final String AWS_SESSION_TOKEN  = "AWS_SESSION_TOKEN";

    private static InstanceProfileCredentialsProvider creds;

    private final Logger logger = LogManager.getLogger(RoleBasedAWSCredentialProvider.class);



    /**
     * Create a {@link AWSCredentialsProvider} from an URI. The URI must contain a query parameter specifying
     * an AWS role ARN. The role is assumed to provide credentials for downstream operations.
     * <p>
     * The constructor signature must conform to hadoop calling convention exactly.
     * </p>
     *
     * @param uri           An URI containing role ARN parameter
     * @param configuration Hadoop configuration data
     */
    public RoleBasedAWSCredentialProvider(URI uri, Configuration configuration) {
        this.uri = uri;

        // This constructor is called by hadoop on EMR. The EMR instance must
        // have permission to access AWS security token service.
        // TODO - consider allow user to supply long-live credentials through hadoop configuration
        this.accessKey = configuration.get( AWS_ACCESS_KEY, null);
        this.secretKey = configuration.get(AWS_SECRET_KEY_ID, null);
        this.sessionToken = configuration.get(AWS_SESSION_TOKEN, null);
        this.bucket_URI = configuration.get(S3_BUCKET_URI, "");

        this.timeProvider = System::currentTimeMillis;
        this.roleArn = configuration.get(AWS_ROLE_ARN_KEY);
        if(this.roleArn == null) {
            logger.warn("RoleBasedAWSCredentialProvider: No role provided via " +
                    "Hadoop configuration. Checking environment variable " + this.ROLE_ENV_VAR + "...");

            Map<String, String> env = System.getenv();
            this.roleArn = env.get(this.ROLE_ENV_VAR);

            if (this.roleArn == null) {
                logger.warn("RoleBasedAWSCredentialProvider: Environment variable " + this.ROLE_ENV_VAR +
                        " not found. Not assuming a role.");
            } else {
                // This level is too high, but I want more visibility.
                // Eventually change to an info.
                logger.warn("RoleBasedAWSCredentialProvider: Using role ARN " + this.roleArn);
            }
        }
    }

    @Override
    public AWSCredentials getCredentials() {
        this.logger.debug("get credential called");

        if(this.roleArn == null) {
            this.logger.warn("assume role not provided");
            return null;
        }
        if (!StringUtils.isEmpty(bucket_URI) && uri.toString().startsWith(bucket_URI)) {
            if(needsNewSession()) {
                startSession();
            }
        }else{
            this.logger.warn("reading from instance profile "+bucket_URI);
            if (creds == null) {
                creds = new InstanceProfileCredentialsProvider
                        (true);
            }
            sessionCredentials = creds.getCredentials();
        }
        return sessionCredentials;
    }

    @Override
    public void refresh() {
        logger.debug("refresh called");
        startSession();
    }

    @Override
    public void setConf(Configuration configuration) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }

    protected Credentials getAssumeRoleSessionCredentialsProvider(final AWSCredentials longLivedCredentials,final String roleArn, final String roleSession) {
        AWSSecurityTokenServiceClient stsClient = new
                AWSSecurityTokenServiceClient(longLivedCredentials);
        AssumeRoleRequest assumeRequest = new AssumeRoleRequest()
                .withRoleArn(roleArn)
                .withDurationSeconds(DURATION_TIME_SECONDS)
                .withRoleSessionName(roleSession);
        AssumeRoleResult assumeResult = stsClient.assumeRole(assumeRequest);
        return assumeResult.getCredentials();
    }

    private AWSCredentials startSession() {
        try {
                logger.warn("getting custom credential for : "+uri);
                String sessionName = "custom-credential-provider" + this.timeProvider.currentTimeMillis();
                Credentials stsCredentials = getAssumeRoleSessionCredentialsProvider(new BasicSessionCredentials(accessKey, secretKey, sessionToken),
                        roleArn, sessionName);
                sessionCredentials = new BasicSessionCredentials(stsCredentials.getAccessKeyId(),
                        stsCredentials.getSecretAccessKey(), stsCredentials.getSessionToken());
                sessionCredentialsExpiration = stsCredentials.getExpiration();
            }
        catch (Exception ex) {
            logger.warn("Unable to start a new session. Will use old session credential or fallback credential", ex);
        }

        return sessionCredentials;
    }

    private boolean needsNewSession() {
        if (sessionCredentials == null) {
            // Increased log level from debug to warn
            logger.warn("Session credentials do not exist. Needs new session");
            return true;
        }

        long timeRemaining = sessionCredentialsExpiration.getTime() - timeProvider.currentTimeMillis();
        if(timeRemaining < EXPIRY_TIME_MILLIS) {
            // Increased log level from debug to warn
            logger.warn("Session credential exist but expired. Needs new session");
            return true;
        } else {
            // Increased log level from debug to warn
            logger.warn("Session credential exist and not expired. No need to create new session");
            return false;
        }
    }
}