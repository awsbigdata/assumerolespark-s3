package software.zip.s3;



import com.amazonaws.auth.*;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;
import org.apache.commons.lang.StringUtils;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.CredentialInitializationException;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.security.ProviderUtils;
import static org.apache.hadoop.fs.s3a.Constants.*;


/**
 * Support session credentials for authenticating with AWS.
 *
 * Please note that users may reference this class name from configuration
 * property fs.s3a.aws.credentials.provider.  Therefore, changing the class name
 * would be a backward-incompatible change.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class MyappsCustomCredentialsProvider implements AWSCredentialsProvider {

    public static final String NAME
            = "software.zip.s3a.MyappsCustomCredentialsProvider";
    private String accessKey;
    private String secretKey;
    private String sessionToken;
    private String assumeRoleArn;
    private IOException lookupIOE;
    // the S3 bucket URI for the other account
    private  String bucket_URI;
    private AWSCredentials credentials;
    private static Credentials stsCredentials;
    private static InstanceProfileCredentialsProvider creds;


    private URI uri;

    static String lookupPassword(Configuration conf, String key, String defVal) throws IOException {
        try {
            char[] pass = conf.getPassword(key);
            return pass != null ? (new String(pass)).trim() : defVal;
        } catch (IOException var4) {
            throw new IOException("Cannot find password option " + key, var4);
        }
    }
    protected Credentials getAssumeRoleSessionCredentialsProvider(final AWSCredentials longLivedCredentials,final String roleArn, final String roleSession) {
        AWSSecurityTokenServiceClient   stsClient = new
                AWSSecurityTokenServiceClient(longLivedCredentials);
        AssumeRoleRequest assumeRequest = new AssumeRoleRequest()
                .withRoleArn(roleArn)
                .withDurationSeconds(3600)
                .withRoleSessionName(roleSession);
        AssumeRoleResult assumeResult = stsClient.assumeRole(assumeRequest);
        Credentials stsCredentials = assumeResult.getCredentials();
        return stsCredentials;
    }

    public MyappsCustomCredentialsProvider(){

    }

    public MyappsCustomCredentialsProvider(URI uri, Configuration conf) {
        try {
            Configuration c = ProviderUtils.excludeIncompatibleCredentialProviders(
                    conf, S3AFileSystem.class);
            this.accessKey = lookupPassword(c, ACCESS_KEY, null);
            this.secretKey = lookupPassword(c, SECRET_KEY, null);
            this.sessionToken = lookupPassword(c, SESSION_TOKEN, null);
            this.assumeRoleArn=lookupPassword(c, "fs.s3a.myapp.assumerole", null);
            this.bucket_URI=lookupPassword(c, "fs.s3a.myapp.uri", null);
            this.uri=uri;
        } catch (IOException e) {
            lookupIOE = e;
        }
    }

    public AWSCredentials getCredentials() {
        if (lookupIOE != null) {
            // propagate any initialization problem
            throw new CredentialInitializationException(lookupIOE.toString(),
                    lookupIOE);
        }
        if (!StringUtils.isEmpty(accessKey) && !StringUtils.isEmpty(secretKey)
                && !StringUtils.isEmpty(sessionToken)) {
            if (!StringUtils.isEmpty(bucket_URI) && uri.toString().startsWith(bucket_URI)) {
                if (!StringUtils.isEmpty(assumeRoleArn)) {
                    System.out.println("reading from myapp role : "+uri);
                    if (stsCredentials == null ||
                            (stsCredentials.getExpiration().getTime() - System.currentTimeMillis() < 60)) {
                        stsCredentials=getAssumeRoleSessionCredentialsProvider(new BasicSessionCredentials(accessKey, secretKey, sessionToken),
                                assumeRoleArn, "fromspark");
                        BasicSessionCredentials temporaryCredentials =
                                new BasicSessionCredentials(
                                        stsCredentials.getAccessKeyId(),
                                        stsCredentials.getSecretAccessKey(),
                                        stsCredentials.getSessionToken());
                        credentials=temporaryCredentials;
                    }
                        return credentials;

                } else {
                    return new BasicSessionCredentials(accessKey, secretKey, sessionToken);

                }
            }else{
               System.out.println("using instance  role : "+uri);
                Boolean refreshCredentialsAsync = true;
                if (creds == null) {
                    creds = new InstanceProfileCredentialsProvider
                            (refreshCredentialsAsync);
                }
                credentials = creds.getCredentials();
                return credentials;
            }

        }
        else{
            //System.out.println("using instance  role : "+uri);

            //Extracting the credentials from EC2 metadata service
            Boolean refreshCredentialsAsync = true;
            if (creds == null) {
                creds = new InstanceProfileCredentialsProvider
                        (refreshCredentialsAsync);
            }
            credentials = creds.getCredentials();
            return credentials;
        }

    }

    @Override
    public void refresh() {}

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

}