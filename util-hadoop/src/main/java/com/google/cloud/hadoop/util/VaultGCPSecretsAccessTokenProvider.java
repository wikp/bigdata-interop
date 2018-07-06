package com.google.cloud.hadoop.util;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.apache.ApacheHttpTransport;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.ExponentialBackOff;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class VaultGCPSecretsAccessTokenProvider implements AccessTokenProvider {

    private static final String VAULT_GCP_PREFIX = "vault.";
    private static final String VAULT_GCP_BACKOFF_PREFIX = VAULT_GCP_PREFIX + "backoff.";

    private static final String VAULT_GCP_ADDRESS_URI_KEY = VAULT_GCP_PREFIX + "address.uri";
    private static final String VAULT_GCP_TOKEN_KEY = VAULT_GCP_PREFIX + "token";
    private static final String VAULT_GCP_SERVICE_ACCOUNT_KEY = VAULT_GCP_PREFIX + "service-account";

    private static final String VAULT_GCP_BACKOFF_INIT_KEY = VAULT_GCP_BACKOFF_PREFIX + "initial";
    private static final int VAULT_GCP_BACKOFF_INIT_DEFAULT = 100;
    private static final String VAULT_GCP_BACKOFF_MAX_KEY = VAULT_GCP_BACKOFF_PREFIX + "max";
    private static final int VAULT_GCP_BACKOFF_MAX_DEFAULT = 10000;
    private static final String VAULT_GCP_BACKOFF_MULTIPLIER_KEY = VAULT_GCP_BACKOFF_PREFIX + "multiplier";
    private static final double VAULT_GCP_BACKOFF_MULTIPLIER_DEFAULT = 2.0;
    private static final String VAULT_GCP_BACKOFF_RANDOMIZATION_FACTOR_KEY = VAULT_GCP_BACKOFF_PREFIX + "randomization-factor";
    private static final double VAULT_GCP_BACKOFF_RANDOMIZATION_FACTOR_DEFAULT = 0.1d;
    private static final long VAULT_TOKEN_EXPIRATION_TIME_MILLIS = 3600000L;
    private static final String VAULT_GCP_ADDRESS_PATH_KEY = VAULT_GCP_PREFIX + "address.path";


    private Configuration conf;
    private BackOff backOff;
    private ApacheHttpTransport httpTransport;
    private HttpRequestFactory requestFactory;

    private AccessToken currentToken;

    public VaultGCPSecretsAccessTokenProvider() {
        httpTransport = new ApacheHttpTransport.Builder().build();
        requestFactory = httpTransport.createRequestFactory();
    }

    @Override
    public AccessToken getAccessToken() {
        if (currentToken == null) {
            currentToken = retrieveTokenFromVault();
        }

        return currentToken;
    }

    private AccessToken retrieveTokenFromVault() {
        final HttpRequest httpRequest = prepareRequest();

        try {
            httpRequest.execute();
        } catch (IOException e) {
            throw new RuntimeException("Failed to acquire token", e);
        }
        return new AccessToken("token", VAULT_TOKEN_EXPIRATION_TIME_MILLIS);
    }

    private HttpRequest prepareRequest() {
        String url = extractVaultUrl();
        String vaultToken = extractVaultToken();
        final HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put("X-Vault-Token", vaultToken);

        try {
            return requestFactory.buildGetRequest(new GenericUrl(url))
                .setHeaders(httpHeaders)
                .setUnsuccessfulResponseHandler(new HttpBackOffUnsuccessfulResponseHandler(backOff));
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to prepare request", e);
        }
    }

    private String extractVaultToken() {
        String vaultToken = conf.get(VAULT_GCP_TOKEN_KEY);
        Preconditions.checkNotNull(vaultToken, "Vault token is not provided in the configuration. Use vault.token property");
        return vaultToken;
    }

    private String extractVaultUrl() {
        String root = conf.get(VAULT_GCP_ADDRESS_URI_KEY);
        Preconditions.checkNotNull(root, "Configuration does not provide vault.address property")
        if (!root.endsWith("/")) {
            root = root + "/";
        }

        String path = conf.get(VAULT_GCP_ADDRESS_PATH_KEY, "v1/gcp/token/");
        String serviceAccount = conf.get(VAULT_GCP_SERVICE_ACCOUNT_KEY);
        Preconditions.checkNotNull(serviceAccount, "Service account is not provided")
        return root + path + serviceAccount;
    }

    @Override
    public void refresh() throws IOException {
        currentToken = retrieveTokenFromVault();
    }

    @Override
    public void setConf(Configuration configuration) {
        conf = configuration;
        backOff = new ExponentialBackOff.Builder()
                .setInitialIntervalMillis(conf.getInt(VAULT_GCP_BACKOFF_INIT_KEY, VAULT_GCP_BACKOFF_INIT_DEFAULT))
                .setMaxElapsedTimeMillis(conf.getInt(VAULT_GCP_BACKOFF_MAX_KEY, VAULT_GCP_BACKOFF_MAX_DEFAULT))
                .setMultiplier(conf.getDouble(VAULT_GCP_BACKOFF_MULTIPLIER_KEY, VAULT_GCP_BACKOFF_MULTIPLIER_DEFAULT))
                .setRandomizationFactor(conf.getDouble(VAULT_GCP_BACKOFF_RANDOMIZATION_FACTOR_KEY, VAULT_GCP_BACKOFF_RANDOMIZATION_FACTOR_DEFAULT))
                .build();
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
