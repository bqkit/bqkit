package io.github.bqkit.core;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Objects;

/** Application Default Credentials, optionally impersonating a target service account. */
public final class CredentialsFactory {

    private static final List<String> SCOPES = List.of("https://www.googleapis.com/auth/cloud-platform");
    private static final int LIFETIME_SECONDS = 3600;

    private CredentialsFactory() {
    }

    public static Credentials create(String impersonateServiceAccount) {
        return create(GoogleCredentials::getApplicationDefault, impersonateServiceAccount);
    }

    static Credentials create(Loader loader, String impersonateServiceAccount) {
        try {
            return create(loader.load(), impersonateServiceAccount);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static Credentials create(GoogleCredentials source, String impersonateServiceAccount) {
        GoogleCredentials scoped = source.createScoped(SCOPES);
        if (Objects.isNull(impersonateServiceAccount) || impersonateServiceAccount.isBlank()) {
            return scoped;
        }
        return ImpersonatedCredentials.create(scoped, impersonateServiceAccount, null, SCOPES, LIFETIME_SECONDS);
    }

    @FunctionalInterface
    interface Loader {
        GoogleCredentials load() throws IOException;
    }
}
