package io.github.bqkit.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import java.util.Date;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class CredentialsFactoryTest {

    private static final GoogleCredentials SOURCE =
            GoogleCredentials.create(new AccessToken("token", new Date(Long.MAX_VALUE)));

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = " ")
    void uses_the_source_credentials_without_a_service_account(String serviceAccount) {
        Credentials credentials = CredentialsFactory.create(SOURCE, serviceAccount);

        assertThat(credentials).isNotInstanceOf(ImpersonatedCredentials.class).isInstanceOf(GoogleCredentials.class);
    }

    @Test
    void impersonates_the_service_account_when_one_is_set() {
        Credentials credentials = CredentialsFactory.create(SOURCE, "reader@my-project.iam.gserviceaccount.com");

        assertThat(credentials).isInstanceOfSatisfying(ImpersonatedCredentials.class, impersonated -> {
            assertThat(impersonated.getAccount()).isEqualTo("reader@my-project.iam.gserviceaccount.com");
            assertThat(impersonated.getSourceCredentials()).isNotNull();
        });
    }
}
