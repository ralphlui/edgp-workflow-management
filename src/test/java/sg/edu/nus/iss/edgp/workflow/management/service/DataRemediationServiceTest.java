package sg.edu.nus.iss.edgp.workflow.management.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import sg.edu.nus.iss.edgp.workflow.management.exception.WorkflowServiceException;
import sg.edu.nus.iss.edgp.workflow.management.repository.DynamicSQLRepository;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.DataRemediationService;
import sg.edu.nus.iss.edgp.workflow.management.utility.Mode;

@ExtendWith(MockitoExtension.class)
class DataRemediationServiceTest {

    @Mock
    private DynamicSQLRepository dynamicSQLRepository;

    @InjectMocks
    private DataRemediationService service;


    @Test
    @DisplayName("Null payload -> no-op")
    void updateDataRemediationResponse_null_noop() {
        assertDoesNotThrow(() -> service.updateDataRemediationResponse(null));
        verifyNoInteractions(dynamicSQLRepository);
    }

    @Test
    @DisplayName("Empty payload -> no-op")
    void updateDataRemediationResponse_empty_noop() {
        assertDoesNotThrow(() -> service.updateDataRemediationResponse(Map.of()));
        verifyNoInteractions(dynamicSQLRepository);
    }

    @Test
    @DisplayName("Non-AUTO mode -> no-op")
    void updateDataRemediationResponse_nonAutoMode_noop() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("mode", "MANUAL"); // anything not AUTO
        raw.put("data", Map.of("action", "delete"));
        assertDoesNotThrow(() -> service.updateDataRemediationResponse(raw));
        verifyNoInteractions(dynamicSQLRepository);
    }

    @Test
    @DisplayName("Missing 'data' field -> no-op")
    void updateDataRemediationResponse_missingData_noop() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("mode", Mode.auto.toString()); // "auto"
        // no "data"
        assertDoesNotThrow(() -> service.updateDataRemediationResponse(raw));
        verifyNoInteractions(dynamicSQLRepository);
    }

    @Test
    @DisplayName("'data' is not a map -> no-op")
    void updateDataRemediationResponse_dataNotMap_noop() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("mode", "AUTO");
        raw.put("data", "string-instead-of-map");
        assertDoesNotThrow(() -> service.updateDataRemediationResponse(raw));
        verifyNoInteractions(dynamicSQLRepository);
    }

    // ----------- delete action -----------

    @Test
    @DisplayName("delete: happy path -> calls insertArchiveData")
    void updateDataRemediationResponse_delete_happy() throws SQLException {
        Map<String, Object> data = new HashMap<>();
        data.put("action", "delete");
        data.put("domain_name", "customer");
        data.put("id", "ID-001");
        data.put("message", "archived by remediation");

        Map<String, Object> raw = new HashMap<>();
        raw.put("mode", "AUTO");      // case-insensitive handled in service
        raw.put("data", data);

        assertDoesNotThrow(() -> service.updateDataRemediationResponse(raw));

        verify(dynamicSQLRepository, times(1))
                .insertArchiveData("customer", "ID-001", "archived by remediation");
        verifyNoMoreInteractions(dynamicSQLRepository);
    }

    @Test
    @DisplayName("delete: missing id -> wrapped as WorkflowServiceException")
    void updateDataRemediationResponse_delete_missingId_wraps() {
        Map<String, Object> data = new HashMap<>();
        data.put("action", "delete");
        data.put("domain_name", "customer");
       

        Map<String, Object> raw = new HashMap<>();
        raw.put("mode", "AUTO");
        raw.put("data", data);

        WorkflowServiceException ex = assertThrows(
                WorkflowServiceException.class,
                () -> service.updateDataRemediationResponse(raw)
        );
        assertTrue(ex.getMessage().contains("An error occurred while updating data remediation response"));
        verifyNoInteractions(dynamicSQLRepository);
    }
 

    @Test
    @DisplayName("update: happy path -> calls updateColumnValue")
    void updateDataRemediationResponse_update_happy() throws SQLException {
        Map<String, Object> data = new HashMap<>();
        data.put("action", "update");
        data.put("domain_name", "orders");
        data.put("id", "OID-9");
        data.put("from_value", "PENDING");
        data.put("to_value", "APPROVED");
        data.put("field_name", "status");
        data.put("message", "auto-fix");

        Map<String, Object> raw = new HashMap<>();
        raw.put("mode", "auto");
        raw.put("data", data);

        assertDoesNotThrow(() -> service.updateDataRemediationResponse(raw));

        verify(dynamicSQLRepository, times(1))
                .updateColumnValue("orders", "status", "OID-9", "PENDING", "APPROVED");
        verifyNoMoreInteractions(dynamicSQLRepository);
    }

    @Test
    @DisplayName("update: missing to_value -> wrapped as WorkflowServiceException")
    void updateDataRemediationResponse_update_missingField_wraps() {
        Map<String, Object> data = new HashMap<>();
        data.put("action", "update");
        data.put("domain_name", "orders");
        data.put("id", "OID-9");
        data.put("from_value", "PENDING");
        // missing to_value
        data.put("field_name", "status");

        Map<String, Object> raw = new HashMap<>();
        raw.put("mode", "AUTO");
        raw.put("data", data);

        WorkflowServiceException ex = assertThrows(
                WorkflowServiceException.class,
                () -> service.updateDataRemediationResponse(raw)
        );
        assertTrue(ex.getMessage().contains("An error occurred while updating data remediation response"));
        verifyNoInteractions(dynamicSQLRepository);
    }


    @Test
    @DisplayName("Repository throws -> wrapped as WorkflowServiceException")
    void updateDataRemediationResponse_repoThrows_wraps() throws SQLException {
        Map<String, Object> data = new HashMap<>();
        data.put("action", "delete");
        data.put("domain_name", "customer");
        data.put("id", "ID-001");
        data.put("message", "archive");

        Map<String, Object> raw = new HashMap<>();
        raw.put("mode", "AUTO");
        raw.put("data", data);

        doThrow(new RuntimeException("DB down"))
                .when(dynamicSQLRepository)
                .insertArchiveData("customer", "ID-001", "archive");

        WorkflowServiceException ex = assertThrows(
                WorkflowServiceException.class,
                () -> service.updateDataRemediationResponse(raw)
        );
        assertTrue(ex.getMessage().contains("An error occurred while updating data remediation response"));
    }
}
