package sg.edu.nus.iss.edgp.workflow.management.jwt;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.security.interfaces.RSAPublicKey;
import java.util.Date;

import io.jsonwebtoken.Claims;
import sg.edu.nus.iss.edgp.workflow.management.configuration.JWTConfig;
import sg.edu.nus.iss.edgp.workflow.management.pojo.User;
import sg.edu.nus.iss.edgp.workflow.management.utility.JSONReader;

import org.json.simple.JSONObject;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test; 
import org.mockito.Mockito;
import org.springframework.security.core.userdetails.UserDetails;

import java.security.KeyPair;
import java.security.KeyPairGenerator;

public class JWTServiceTest {

    private JWTService jWTService;
    private JWTConfig jWTConfig;
    private JSONReader jsonReader;

    private RSAPublicKey publicKey;
    private Claims claims;

    @BeforeEach
    void setUp() throws Exception {
    	jWTConfig = mock(JWTConfig.class);
        jsonReader = mock(JSONReader.class);
        jWTService = new JWTService(jWTConfig, jsonReader);

        KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
        keyGen.initialize(2048);
        KeyPair keyPair = keyGen.generateKeyPair();
        publicKey = (RSAPublicKey) keyPair.getPublic();

        when(jWTConfig.loadPublicKey()).thenReturn(publicKey);
        claims = mock(Claims.class);
    }

    @Test
    void testExtractSubjectReturnsCorrectSubject() throws Exception {
        String token = "dummy.JWT.token";
        String expectedSubject = "user123";

        JWTService spyService = Mockito.spy(jWTService);
        doReturn(claims).when(spyService).extractAllClaims(token);
        when(claims.getSubject()).thenReturn(expectedSubject);

        String result = spyService.extractSubject(token);

        assertEquals(expectedSubject, result);
    }

    @Test
    void testIsTokenExpiredReturnsFalse() throws Exception {
        String token = "valid.token";
        JWTService spyService = Mockito.spy(jWTService);
        Date futureDate = new Date(System.currentTimeMillis() + 100000);

        doReturn(futureDate).when(spyService).extractExpiration(token);

        assertFalse(spyService.isTokenExpired(token));
    }

    @Test
    void testExtractUserNameFromValidToken() throws Exception {
        String token = "valid.token";
        String expectedName = "John Doe";

        JWTService spyService = Mockito.spy(jWTService);
        doReturn(claims).when(spyService).extractAllClaims(token);
        when(claims.get(jWTService.CLAIM_USERNAME, String.class)).thenReturn(expectedName);

        String actualName = spyService.extractUserNameFromToken(token);
        assertEquals(expectedName, actualName);
    }

    @Test
    void testValidateTokenSuccess() throws Exception {
        String token = "valid.token";
        String email = "test@example.com";

        UserDetails userDetails = org.springframework.security.core.userdetails.User
                .withUsername(email).password("pass").roles("USER").build();

        JWTService spyService = Mockito.spy(jWTService);
        doReturn(claims).when(spyService).extractAllClaims(token);
        when(claims.get(JWTService.USER_EMAIL, String.class)).thenReturn(email);
        doReturn(false).when(spyService).isTokenExpired(token);

        assertTrue(spyService.validateToken(token, userDetails));
    }

    @Test
    void testGetUserDetailSuccess() throws Exception {
        String token = "token";
        String authHeader = "Bearer token";
        String userId = "user123";

        User user = new User();
        user.setEmail("user@example.com");
        user.setPassword("securepass");
        user.setRole("Customer");

        JSONObject mockResponse = new JSONObject();
        mockResponse.put("success", true);

        JWTService spyService = Mockito.spy(jWTService);
        doReturn(userId).when(spyService).extractSubject(token);
        when(jsonReader.getSuccessFromResponse(mockResponse)).thenReturn(true);
        when(jsonReader.getUserObject(mockResponse)).thenReturn(user);
        when(jsonReader.getActiveUserInfo(userId, authHeader)).thenReturn(mockResponse);
      

        UserDetails userDetails = spyService.getUserDetail(authHeader, token);

        assertEquals(user.getEmail(), userDetails.getUsername());
    }
}
