package sg.edu.nus.iss.edgp.workflow.management.jwt;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Date;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.security.core.userdetails.UserDetails;
import sg.edu.nus.iss.edgp.workflow.management.configuration.JWTConfig;
import sg.edu.nus.iss.edgp.workflow.management.enums.AuditLogInvalidUser;
import sg.edu.nus.iss.edgp.workflow.management.pojo.User;
import sg.edu.nus.iss.edgp.workflow.management.utility.JSONReader;

class JWTServiceTest {

    private JWTService service;
    private JWTConfig jwtConfig;
    private JSONReader jsonReader;

    private Claims claims;

    @BeforeEach
    void setUp() {
        jwtConfig = mock(JWTConfig.class);
        jsonReader = mock(JSONReader.class);
        service = new JWTService(jwtConfig, jsonReader);
        claims = mock(Claims.class);
    }


    @Test
    void extractSubject_returnsSubject() throws Exception {
        String token = "t";
        String expected = "user123";
        JWTService spy = Mockito.spy(service);
        doReturn(claims).when(spy).extractAllClaims(token);
        when(claims.getSubject()).thenReturn(expected);

        assertEquals(expected, spy.extractSubject(token));
    }

    @Test
    void extractClaim_genericResolver_works() throws Exception {
        String token = "t";
        Date exp = new Date(System.currentTimeMillis() + 3600_000);
        JWTService spy = Mockito.spy(service);
        doReturn(claims).when(spy).extractAllClaims(token);
        when(claims.getExpiration()).thenReturn(exp);

        Date got = spy.extractClaim(token, Claims::getExpiration);
        assertEquals(exp, got);
    }


    @Test
    void isTokenExpired_false_whenFuture() throws Exception {
        String token = "t";
        JWTService spy = Mockito.spy(service);
        Date future = new Date(System.currentTimeMillis() + 100_000);
        doReturn(future).when(spy).extractExpiration(token);

        assertFalse(spy.isTokenExpired(token));
    }

    @Test
    void isTokenExpired_true_whenPast() throws Exception {
        String token = "t";
        JWTService spy = Mockito.spy(service);
        Date past = new Date(System.currentTimeMillis() - 100_000);
        doReturn(past).when(spy).extractExpiration(token);

        assertTrue(spy.isTokenExpired(token));
    }


    @Test
    void validateToken_success_whenEmailMatches_andNotExpired() throws Exception {
        String token = "valid.token";
        String email = "test@example.com";
        UserDetails userDetails = org.springframework.security.core.userdetails.User
                .withUsername(email).password("x").roles("USER").build();

        JWTService spy = Mockito.spy(service);
        doReturn(claims).when(spy).extractAllClaims(token);
        when(claims.get(JWTService.USER_EMAIL, String.class)).thenReturn(email);
        doReturn(false).when(spy).isTokenExpired(token);

        assertTrue(spy.validateToken(token, userDetails));
    }

    @Test
    void validateToken_false_whenEmailMismatch() throws Exception {
        String token = "t";
        UserDetails ud = org.springframework.security.core.userdetails.User
                .withUsername("alice@example.com").password("x").roles("USER").build();

        JWTService spy = Mockito.spy(service);
        doReturn(claims).when(spy).extractAllClaims(token);
        when(claims.get(JWTService.USER_EMAIL, String.class)).thenReturn("bob@example.com");
        doReturn(false).when(spy).isTokenExpired(token);

        assertFalse(spy.validateToken(token, ud));
    }

  

    @Test
    void extractUserNameFromToken_ok() throws Exception {
        String token = "t";
        JWTService spy = Mockito.spy(service);
        doReturn(claims).when(spy).extractAllClaims(token);
        when(claims.get(JWTService.CLAIM_USERNAME, String.class)).thenReturn("John");

        assertEquals("John", spy.extractUserNameFromToken(token));
    }

    @Test
    void extractUserNameFromToken_usesClaimsFromExpired() throws Exception {
        String token = "expired.t";
        JWTService spy = Mockito.spy(service);
        // simulate ExpiredJwtException with claims containing username
        when(claims.get(JWTService.CLAIM_USERNAME, String.class)).thenReturn("Jane");
        doThrow(new ExpiredJwtException(null, claims, "expired")).when(spy).extractAllClaims(token);

        assertEquals("Jane", spy.extractUserNameFromToken(token));
    }

    @Test
    void extractUserNameFromToken_invalid_returnsFallback() throws Exception {
        String token = "bad.t";
        JWTService spy = Mockito.spy(service);
        doThrow(new RuntimeException("parse error")).when(spy).extractAllClaims(token);

        assertEquals("Invalid Username", spy.extractUserNameFromToken(token));
    }

    @Test
    void extractUserIdFromToken_ok() throws Exception {
        String token = "t";
        JWTService spy = Mockito.spy(service);
        doReturn(claims).when(spy).extractAllClaims(token);
        when(claims.getSubject()).thenReturn("user-1");

        assertEquals("user-1", spy.extractUserIdFromToken(token));
    }

    @Test
    void extractUserIdFromToken_expired_usesExpiredClaims() throws Exception {
        String token = "expired";
        JWTService spy = Mockito.spy(service);
        when(claims.getSubject()).thenReturn("user-2");
        doThrow(new ExpiredJwtException(null, claims, "expired")).when(spy).extractAllClaims(token);

        assertEquals("user-2", spy.extractUserIdFromToken(token));
    }

    @Test
    void extractUserIdFromToken_invalid_returnsFallback() throws Exception {
        String token = "x";
        JWTService spy = Mockito.spy(service);
        doThrow(new RuntimeException("bad")).when(spy).extractAllClaims(token);

        assertEquals("Invalid UserID", spy.extractUserIdFromToken(token));
    }

    @Test
    void extractOrgIdFromToken_ok() throws Exception {
        String token = "t";
        JWTService spy = Mockito.spy(service);
        doReturn(claims).when(spy).extractAllClaims(token);
        when(claims.get(JWTService.ORG_ID, String.class)).thenReturn("org-123");

        assertEquals("org-123", spy.extractOrgIdFromToken(token));
    }

    @Test
    void extractOrgIdFromToken_invalid_returnsFallback() throws Exception {
        String token = "t";
        JWTService spy = Mockito.spy(service);
        doThrow(new RuntimeException("bad")).when(spy).extractAllClaims(token);

        assertEquals("Invalid OrgId", spy.extractOrgIdFromToken(token));
    }

    @Test
    void extractScopeFromToken_ok() throws Exception {
        String token = "t";
        JWTService spy = Mockito.spy(service);
        doReturn(claims).when(spy).extractAllClaims(token);
        when(claims.get(JWTService.SCOPE, String.class)).thenReturn("read:all");

        assertEquals("read:all", spy.extractScopeFromToken(token));
    }

    @Test
    void extractScopeFromToken_invalid_returnsFallback() throws Exception {
        String token = "t";
        JWTService spy = Mockito.spy(service);
        doThrow(new RuntimeException("bad")).when(spy).extractAllClaims(token);

        assertEquals("Invalid Scope", spy.extractScopeFromToken(token));
    }

    @Test
    void extractUserIdAllowExpiredToken_ok() throws Exception {
        String token = "t";
        JWTService spy = Mockito.spy(service);
        doReturn(claims).when(spy).extractAllClaims(token);
        when(claims.getSubject()).thenReturn("user-1");

        assertEquals("user-1", spy.extractUserIdAllowExpiredToken(token));
    }

    @Test
    void extractUserIdAllowExpiredToken_fromExpiredClaims() throws Exception {
        String token = "expired";
        JWTService spy = Mockito.spy(service);
        when(claims.getSubject()).thenReturn("user-2");
        doThrow(new ExpiredJwtException(null, claims, "expired")).when(spy).extractAllClaims(token);

        assertEquals("user-2", spy.extractUserIdAllowExpiredToken(token));
    }

    @Test
    void extractUserIdAllowExpiredToken_onError_returnsInvalidUserIdEnum() throws Exception {
        String token = "bad";
        JWTService spy = Mockito.spy(service);
        doThrow(new RuntimeException("boom")).when(spy).extractAllClaims(token);

        assertEquals(AuditLogInvalidUser.INVALID_USER_ID.toString(), spy.extractUserIdAllowExpiredToken(token));
    }

    @Test
    void retrieveUserName_ok() throws Exception {
        String token = "t";
        JWTService spy = Mockito.spy(service);
        doReturn(claims).when(spy).extractAllClaims(token);
        when(claims.get("userName", String.class)).thenReturn("Alice");

        assertEquals("Alice", spy.retrieveUserName(token));
    }

    @Test
    void retrieveUserName_expired_usesExpiredClaims() throws Exception {
        String token = "expired";
        JWTService spy = Mockito.spy(service);
        when(claims.get("userName", String.class)).thenReturn("Bob");
        doThrow(new ExpiredJwtException(null, claims, "expired")).when(spy).extractAllClaims(token);

        assertEquals("Bob", spy.retrieveUserName(token));
    }

    @Test
    void extractUserNameAllowExpiredToken_ok() throws Exception {
        String token = "t";
        JWTService spy = Mockito.spy(service);
        doReturn(claims).when(spy).extractAllClaims(token);
        when(claims.get(JWTService.CLAIM_USERNAME, String.class)).thenReturn("Carol");

        assertEquals("Carol", spy.extractUserNameAllowExpiredToken(token));
    }

    @Test
    void extractUserNameAllowExpiredToken_onError_returnsInvalidUserNameEnum() throws Exception {
        String token = "bad";
        JWTService spy = Mockito.spy(service);
        doThrow(new RuntimeException("boom")).when(spy).extractAllClaims(token);

        assertEquals(AuditLogInvalidUser.INVALID_USER_NAME.toString(), spy.extractUserNameAllowExpiredToken(token));
    }

    

    @Test
    void getUserDetail_success_buildsSpringUser() throws Exception {
        String token = "token";
        String authHeader = "Bearer token";
        String userId = "user123";

        User user = new User();
        user.setEmail("user@example.com");
        user.setPassword("securepass");
        user.setRole("Customer");

        JSONObject mockResp = new JSONObject();
        // service uses jsonReader.getSuccessFromResponse(mockResp) so we don't rely on the map
        JWTService spy = Mockito.spy(service);

        doReturn(userId).when(spy).extractSubject(token);
        when(jsonReader.getActiveUserInfo(userId, authHeader)).thenReturn(mockResp);
        when(jsonReader.getSuccessFromResponse(mockResp)).thenReturn(true);
        when(jsonReader.getUserObject(mockResp)).thenReturn(user);

        UserDetails ud = spy.getUserDetail(authHeader, token);
        assertEquals(user.getEmail(), ud.getUsername());
        assertTrue(ud.getAuthorities().stream().anyMatch(a -> a.getAuthority().equals("ROLE_" + user.getRole())));
    }

    @Test
    void getUserDetail_failure_throwsWithMessageFromResponse() throws Exception {
        String token = "token";
        String authHeader = "Bearer token";
        String userId = "user123";
        String errMsg = "User not active";

        JSONObject mockResp = new JSONObject();

        JWTService spy = Mockito.spy(service);
        doReturn(userId).when(spy).extractSubject(token);
        when(jsonReader.getActiveUserInfo(userId, authHeader)).thenReturn(mockResp);
        when(jsonReader.getSuccessFromResponse(mockResp)).thenReturn(false);
        when(jsonReader.getMessageFromResponse(mockResp)).thenReturn(errMsg);

        Exception ex = assertThrows(Exception.class, () -> spy.getUserDetail(authHeader, token));
        assertEquals(errMsg, ex.getMessage());
    }
}
