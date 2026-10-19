package io.github.bqkit.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Date;
import java.util.List;
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
    void scopes_the_source_credentials_to_cloud_platform() {
        assertThat(CredentialsFactory.create(scopedTo(SOURCE), null)).isSameAs(SOURCE);
    }

    @Test
    void impersonates_the_service_account_from_the_scoped_source_credentials() {
        Credentials credentials = CredentialsFactory.create(
                scopedTo(SOURCE), "reader@my-project.iam.gserviceaccount.com");

        assertThat(credentials).isInstanceOfSatisfying(ImpersonatedCredentials.class, impersonated -> {
            assertThat(impersonated.getAccount()).isEqualTo("reader@my-project.iam.gserviceaccount.com");
            assertThat(impersonated.getSourceCredentials()).isSameAs(SOURCE);
        });
    }

    @Test
    void loads_the_source_credentials_before_scoping_them() {
        assertThat(CredentialsFactory.create(() -> scopedTo(SOURCE), null)).isSameAs(SOURCE);
    }

    @Test
    void reports_credentials_that_cannot_be_loaded_as_unchecked() {
        IOException failure = new IOException("no application default credentials");

        assertThatThrownBy(() -> CredentialsFactory.create(() -> {
            throw failure;
        }, null)).isInstanceOf(UncheckedIOException.class).hasCause(failure);
    }

    private static GoogleCredentials scopedTo(GoogleCredentials scoped) {
        GoogleCredentials source = mock(GoogleCredentials.class);
        when(source.createScoped(List.of("https://www.googleapis.com/auth/cloud-platform"))).thenReturn(scoped);
        return source;
    }
}
