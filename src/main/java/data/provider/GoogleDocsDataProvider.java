package data.provider;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.auth.oauth2.StoredCredential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.DataStore;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;

public class GoogleDocsDataProvider {
    private String clientSecretsPath = "/google-sheets-client-secret.json";
    private String storedCredentialsPath = ".\\";
    private String docId;
    private Credential credential;

    public void setDocId(String docId) {
        this.docId = docId;
    }

    public String getDocId() {
        return docId;
    }

    /**
     * Authorize credential.
     *
     * @return the credential
     * @throws IOException              the io exception
     * @throws GeneralSecurityException the general security exception
     */
    public Credential authorize() throws IOException, GeneralSecurityException {
        InputStream is = GoogleDocsDataProvider.class.getResourceAsStream(clientSecretsPath);
        GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(
                JacksonFactory.getDefaultInstance(),
                new InputStreamReader(is));
        FileDataStoreFactory fileDataStoreFactory = new FileDataStoreFactory(new File(storedCredentialsPath));
        DataStore<StoredCredential> dataStore = fileDataStoreFactory.getDataStore("user");
        String refreshToken = "1/TQaSlcngNc4EmdAyVZBOCqCNb3eFKxSnUQFmzVGhzAo"; //do not change
        StoredCredential storedCredential = new StoredCredential();
        storedCredential.setRefreshToken(refreshToken);
        dataStore.set("user", storedCredential);

        List<String> scopes = Collections.singletonList(SheetsScopes.SPREADSHEETS_READONLY);
        GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
                GoogleNetHttpTransport.newTrustedTransport(),
                JacksonFactory.getDefaultInstance(),
                clientSecrets,
                scopes)
                .setCredentialDataStore(dataStore)
                .build();
        //DO NOT CHANGE
        credential = new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver()).authorize("user");
        credential.refreshToken();
        return credential;
    }

    /**
     * Gets sheets service.
     *
     * @return the sheets service
     * @throws IOException              the io exception
     * @throws GeneralSecurityException the general security exception
     */
    public Sheets getSheetsService() throws IOException, GeneralSecurityException {
        if (credential == null || credential.getExpirationTimeMilliseconds() < System.currentTimeMillis()) {
            authorize();
        }
        return new Sheets
                .Builder(GoogleNetHttpTransport.newTrustedTransport(), JacksonFactory.getDefaultInstance(), credential)
                .setApplicationName("Google Sheets Example")
                .build();
    }
}
