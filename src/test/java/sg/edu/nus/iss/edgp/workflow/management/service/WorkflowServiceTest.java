package sg.edu.nus.iss.edgp.workflow.management.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import java.util.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import sg.edu.nus.iss.edgp.workflow.management.aws.service.SQSDataQualityRequestService;
import sg.edu.nus.iss.edgp.workflow.management.dto.SearchRequest;
import sg.edu.nus.iss.edgp.workflow.management.dto.WorkflowStatus;
import sg.edu.nus.iss.edgp.workflow.management.exception.WorkflowServiceException;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.DynamicDynamoService;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.DynamicSQLService;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.PayloadBuilderService;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.WorkflowService;
import software.amazon.awssdk.core.SdkBytes;
// If you use AWS SDK v1, switch import to com.amazonaws.services.dynamodbv2.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

@ExtendWith(MockitoExtension.class)
class WorkflowServiceTest {

	@Mock
	private DynamicDynamoService dynamoService;

	@Mock
	private DynamicSQLService dynamicSQLService;

	@Spy
	@InjectMocks
	private WorkflowService service;

	private static final String TABLE = "master_task_tracker";
	private static final String TRACKER_TABLE = "master_task_tracker";
	private static final String HEADER_TABLE = "master_header";
	
	@Mock private PayloadBuilderService payloadBuilderService;
    @Mock private SQSDataQualityRequestService sqsDataQualityRequestService;


	@BeforeEach
	void setUp() throws Exception {
		// Set the table name if it's a private field
		Field f = WorkflowService.class.getDeclaredField("masterDataTaskTrackerTableName");
		f.setAccessible(true);
		f.set(service, TABLE);
		setPrivateField(service, "masterDataTaskTrackerTableName", TRACKER_TABLE);
		setPrivateField(service, "masterDataHeaderTableName", HEADER_TABLE);
	}

	@Test
	void updateWorkflowStatus_success_insertsCleanData() {
		// Arrange
		Map<String, Object> data = Map.of("id", "wf-123");
		List<Map<String, Object>> failedValidations = List.of(Map.of("code", "X1", "msg", "oops"));

		Map<String, Object> raw = new HashMap<>();
		raw.put("status", "SUCCESS"); // case-insensitive match in insetCleanMasterData
		raw.put("data", data);
		raw.put("failed_validations", failedValidations);
		raw.put("domain_name", "my_domain_tbl");

		when(dynamoService.tableExists(TABLE)).thenReturn(true);

		Map<String, AttributeValue> stored = new HashMap<>();
		stored.put("id", AttributeValue.builder().s("wf-123").build());
		stored.put("created_date", AttributeValue.builder().s("2025-08-01T12:00:00Z").build());
		stored.put("final_status", AttributeValue.builder().s("PENDING").build());
		stored.put("dataquality_status", AttributeValue.builder().s("PENDING").build());
		stored.put("business_key", AttributeValue.builder().s("BK-9").build());
		stored.put("amount", AttributeValue.builder().n("42").build());
		when(dynamoService.getDataByWorkflowStatusId(TABLE, "wf-123")).thenReturn(stored);

		// Stub the conversion helper to return a mutable map we can assert on later
		Map<String, Object> converted = new LinkedHashMap<>();
		converted.put("id", "wf-123");
		converted.put("created_date", "2025-08-01T12:00:00Z");
		converted.put("final_status", "PENDING");
		converted.put("rule_status", "PENDING");
		converted.put("business_key", "BK-9");
		converted.put("amount", 42);
		doReturn(converted).when(service).dynamoItemToJavaMap(stored);

		ArgumentCaptor<WorkflowStatus> wsCaptor = ArgumentCaptor.forClass(WorkflowStatus.class);
		ArgumentCaptor<Map<String, Object>> cleanMapCaptor = ArgumentCaptor.forClass(Map.class);
		ArgumentCaptor<String> domainTableCaptor = ArgumentCaptor.forClass(String.class);

		// Act
		service.updateDataQualityWorkflowStatus(raw);

		// Assert: workflow status persisted with expected fields
		verify(dynamoService).updateWorkflowStatus(eq(TABLE), wsCaptor.capture());
		WorkflowStatus saved = wsCaptor.getValue();
		assertEquals("wf-123", saved.getId());
		assertEquals("SUCCESS", saved.getFinalStatus());
		assertEquals("SUCCESS", saved.getDataQualityStatus());
		assertEquals(List.copyOf(failedValidations), saved.getFailedValidations());

		// Assert: dynamicSQLService called with cleaned map (keys removed)
		verify(dynamicSQLService).buildCreateTableSQL(cleanMapCaptor.capture(), domainTableCaptor.capture());
		Map<String, Object> cleaned = cleanMapCaptor.getValue();
		assertFalse(cleaned.containsKey("id"), "id should be removed");
		assertFalse(cleaned.containsKey("created_date"), "created_date should be removed");
		assertFalse(cleaned.containsKey("final_status"), "final_status should be removed");
		assertFalse(cleaned.containsKey("dataquality_status"), "rule_status should be removed");
		// business fields remain
		assertEquals("BK-9", cleaned.get("business_key"));
		assertEquals(42, cleaned.get("amount"));
		assertEquals("my_domain_tbl", domainTableCaptor.getValue());
	}

	@Test
	@DisplayName("Creates table when it doesn't exist")
	void updateWorkflowStatus_createsTableIfMissing() {
		// Arrange
		Map<String, Object> data = Map.of("id", "wf-123");
		Map<String, Object> raw = new HashMap<>();
		raw.put("status", "SUCCESS");
		raw.put("data", data);
		raw.put("failed_validations", List.of());
		raw.put("domain_name", "my_domain_tbl");

		when(dynamoService.tableExists(TABLE)).thenReturn(false);
		when(dynamoService.getDataByWorkflowStatusId(TABLE, "wf-123"))
				.thenReturn(Map.of("id", AttributeValue.builder().s("wf-123").build()));

		// conversion stub to keep insetCleanMasterData happy
		doReturn(new HashMap<>(Map.of("business_key", "BK-1"))).when(service).dynamoItemToJavaMap(anyMap());

		// Act
		service.updateDataQualityWorkflowStatus(raw);

		// Assert
		verify(dynamoService).createTable(TABLE);
	}

	@Test
	void updateWorkflowStatus_missingExistingData_throws() {
		// Arrange
		Map<String, Object> raw = new HashMap<>();
		raw.put("status", "SUCCESS");
		raw.put("data", Map.of("id", "wf-404"));
		raw.put("failed_validations", List.of());
		raw.put("domain_name", "domain");

		when(dynamoService.tableExists(TABLE)).thenReturn(true);
		when(dynamoService.getDataByWorkflowStatusId(TABLE, "wf-404")).thenReturn(Collections.emptyMap());

		// Act & Assert
		WorkflowServiceException ex = assertThrows(WorkflowServiceException.class,
				() -> service.updateDataQualityWorkflowStatus(raw));
		assertTrue(ex.getMessage().contains("error"));
	}

	@Test
	void updateWorkflowStatus_noInsertWhenNotSuccessOrNoDomain() {
		// Arrange
		Map<String, Object> raw = new HashMap<>();
		raw.put("status", "FAILED"); // not success
		raw.put("data", Map.of("id", "wf-123"));
		raw.put("failed_validations", List.of());
		raw.put("domain_name", ""); // empty

		when(dynamoService.tableExists(TABLE)).thenReturn(true);
		when(dynamoService.getDataByWorkflowStatusId(TABLE, "wf-123"))
				.thenReturn(Map.of("id", AttributeValue.builder().s("wf-123").build()));

		// Act
		service.updateDataQualityWorkflowStatus(raw);

		// Assert
		verify(dynamicSQLService, never()).buildCreateTableSQL(anyMap(), anyString());
	}

	@Test
	void updateWorkflowStatus_wrapsExceptions() {
		// Arrange
		Map<String, Object> raw = new HashMap<>();
		raw.put("status", "SUCCESS");
		raw.put("data", Map.of("id", "wf-123"));
		raw.put("failed_validations", List.of());
		raw.put("domain_name", "domain");

		when(dynamoService.tableExists(TABLE)).thenReturn(true);
		when(dynamoService.getDataByWorkflowStatusId(TABLE, "wf-123"))
				.thenReturn(Map.of("id", AttributeValue.builder().s("wf-123").build()));
		doThrow(new RuntimeException("boom")).when(dynamoService).updateWorkflowStatus(eq(TABLE),
				any(WorkflowStatus.class));

		// Act & Assert
		WorkflowServiceException ex = assertThrows(WorkflowServiceException.class,
				() -> service.updateDataQualityWorkflowStatus(raw));
		assertTrue(ex.getMessage().contains("An error occurred while updating workflow status"));
	}

	private static void setPrivateField(Object target, String field, Object value) throws Exception {
		Field f = target.getClass().getDeclaredField(field);
		f.setAccessible(true);
		f.set(target, value);
	}

	@Test
	void retrieveDataList_happyPath() {
		// Arrange
		String fileId = "file-123";
		String orgId = "org-9";
		SearchRequest searchRequest = new SearchRequest();

		Map<String, AttributeValue> item1 = new HashMap<>();
		item1.put("id", AttributeValue.builder().s("r1").build());
		item1.put("final_status", AttributeValue.builder().s("SUCCESS").build());

		Map<String, AttributeValue> item2 = new HashMap<>();
		item2.put("id", AttributeValue.builder().s("r2").build());
		item2.put("final_status", AttributeValue.builder().s("FAIL").build());

		Map<String, Object> retrieveResult = new HashMap<>();
		retrieveResult.put("items", List.of(item1, item2));
		retrieveResult.put("totalCount", 2);
		when(dynamoService.retrieveDataList(TRACKER_TABLE, fileId, searchRequest, orgId)).thenReturn(retrieveResult);

		Map<String, AttributeValue> headerRecord = new HashMap<>();
		headerRecord.put("file_name", AttributeValue.builder().s("invoices_aug.csv").build());
		when(dynamoService.getFileDataByFileId(HEADER_TABLE, fileId)).thenReturn(headerRecord);

		// Spy conversion from Dynamo item to Java map
		Map<String, Object> converted1 = new LinkedHashMap<>(
				Map.of("id", "r1", "final_status", "SUCCESS", "amount", 100));
		Map<String, Object> converted2 = new LinkedHashMap<>(Map.of("id", "r2", "final_status", "FAIL", "amount", 200));
		doReturn(converted1).when(service).dynamoItemToJavaMap(item1);
		doReturn(converted2).when(service).dynamoItemToJavaMap(item2);

		// Act
		List<Map<String, Object>> out = service.retrieveDataList(fileId, searchRequest, orgId);

		// Assert: 3 elements (2 items + 1 summary at end)
		assertEquals(3, out.size());

		Map<String, Object> out1 = out.get(0);
		Map<String, Object> out2 = out.get(1);
		Map<String, Object> summary = out.get(2);

		// fileName added to each item
		assertEquals("invoices_aug.csv", out1.get("fileName"));
		assertEquals("invoices_aug.csv", out2.get("fileName"));

		// Original fields preserved
		assertEquals("r1", out1.get("id"));
		assertEquals("SUCCESS", out1.get("final_status"));
		assertEquals(100, out1.get("amount"));

		assertEquals("r2", out2.get("id"));
		assertEquals("FAIL", out2.get("final_status"));
		assertEquals(200, out2.get("amount"));

		// Summary map contains counts and totalCount; is last element
		assertEquals(2, summary.get("totalCount"));
		assertEquals(1, summary.get("successRecords"));
		assertEquals(1, summary.get("failedRecords"));
	}

	@Test
	void retrieveDataList_blankFileId_skipsHeaderLookup() {
		// Arrange
		String fileId = "   "; // blank after trim check in caller
		SearchRequest searchRequest = new SearchRequest();

		Map<String, AttributeValue> item = Map.of("id", AttributeValue.builder().s("r1").build(), "final_status",
				AttributeValue.builder().s("SUCCESS").build());
		Map<String, Object> retrieveResult = new HashMap<>();
		retrieveResult.put("items", List.of(item));
		retrieveResult.put("totalCount", 1);
		when(dynamoService.retrieveDataList(eq(TRACKER_TABLE), anyString(), any(), anyString()))
				.thenReturn(retrieveResult);

		doReturn(new LinkedHashMap<>(Map.of("id", "r1", "final_status", "SUCCESS"))).when(service)
				.dynamoItemToJavaMap(anyMap());

		// Act
		List<Map<String, Object>> out = service.retrieveDataList(fileId, searchRequest, "org");

		// Assert
		verify(dynamoService, never()).getFileDataByFileId(anyString(), anyString());
		assertNull(out.get(0).get("fileName"), "fileName should not be present when fileId is blank");
		Map<String, Object> summary = out.get(1);
		assertEquals(1, summary.get("totalCount"));
		assertEquals(1, summary.get("successRecords"));
		assertEquals(0, summary.get("failedRecords"));
	}

	@Test
	void retrieveDataList_wrapsExceptions() {
		// Arrange
		when(dynamoService.retrieveDataList(anyString(), anyString(), any(), anyString()))
				.thenThrow(new RuntimeException("boom"));

		// Act & Assert
		WorkflowServiceException ex = assertThrows(WorkflowServiceException.class,
				() -> service.retrieveDataList("file", new SearchRequest(), "org"));
		assertTrue(ex.getMessage().contains("An error occurred while retireving data list"));
	}
	
	
	@Test
    @DisplayName("Happy path: returns converted map when data exists")
    void retrieveDataRecordDetailbyWorkflowId_success() {
        // Arrange
        String workflowStatusId = "wf-123";
        Map<String, AttributeValue> record = new HashMap<>();
        record.put("id", AttributeValue.builder().s("wf-123").build());
        record.put("final_status", AttributeValue.builder().s("SUCCESS").build());

        when(dynamoService.getDataByWorkflowStatusId(TRACKER_TABLE, workflowStatusId)).thenReturn(record);

        Map<String, Object> converted = new HashMap<>();
        converted.put("id", "wf-123");
        converted.put("final_status", "SUCCESS");
        doReturn(converted).when(service).dynamoItemToJavaMap(record);

        // Act
        Map<String, Object> out = service.retrieveDataRecordDetailbyWorkflowId(workflowStatusId);

        // Assert
        assertEquals("wf-123", out.get("id"));
        assertEquals("SUCCESS", out.get("final_status"));
    }

    @Test
    @DisplayName("Throws WorkflowServiceException when no data found")
    void retrieveDataRecordDetailbyWorkflowId_noData_throws() {
        // Arrange
        when(dynamoService.getDataByWorkflowStatusId(TRACKER_TABLE, "wf-404"))
                .thenReturn(Collections.emptyMap());

        // Act & Assert
        WorkflowServiceException ex = assertThrows(
                WorkflowServiceException.class,
                () -> service.retrieveDataRecordDetailbyWorkflowId("wf-404")
        );
        assertTrue(ex.getMessage().contains("error"));
    }

    @Test
    @DisplayName("Wraps unexpected exceptions into WorkflowServiceException")
    void retrieveDataRecordDetailbyWorkflowId_wrapsException() {
    
        when(dynamoService.getDataByWorkflowStatusId(TRACKER_TABLE, "wf-err"))
                .thenThrow(new RuntimeException("boom"));
 
        WorkflowServiceException ex = assertThrows(
                WorkflowServiceException.class,
                () -> service.retrieveDataRecordDetailbyWorkflowId("wf-err")
        );
        assertTrue(ex.getMessage().contains("An error occurred while retireving workflow data record by id"));
    }
    
    @Test
    @DisplayName("Rule flow: SUCCESS -> updates ruleStatus, builds payload, sends SQS")
    void updateRuleWorkflowStatus_success_buildsPayloadAndSends() {
       
        Map<String, Object> data = Map.of("id", "wf-123");
        List<Map<String, Object>> failedValidations = List.of(Map.of("f", "v"));

        Map<String, Object> raw = new HashMap<>();
        raw.put("status", "SUCCESS");
        raw.put("data", data);
        raw.put("failed_validations", failedValidations);

        when(dynamoService.tableExists(TABLE)).thenReturn(true);

        Map<String, AttributeValue> stored = new HashMap<>();
        stored.put("id", AttributeValue.builder().s("wf-123").build());
        when(dynamoService.getDataByWorkflowStatusId(TABLE, "wf-123")).thenReturn(stored);

        Map<String, Object> converted = new LinkedHashMap<>();
        converted.put("id", "wf-123");
        doReturn(converted).when(service).dynamoItemToJavaMap(stored);

        Map<String, Object> payloadOut = Map.of("data_entry", Map.of("x", 1));
        try {
            when(payloadBuilderService.buildDataQualityPayLoad(raw, converted)).thenReturn(payloadOut);
        } catch (Exception e) {
            fail(e);
        }

        ArgumentCaptor<WorkflowStatus> wsCaptor = ArgumentCaptor.forClass(WorkflowStatus.class);
 
        service.updateRuleWorkflowStatus(raw);

        verify(dynamoService).updateWorkflowStatus(eq(TABLE), wsCaptor.capture());
        WorkflowStatus saved = wsCaptor.getValue();
        assertEquals("wf-123", saved.getId());
        assertEquals("SUCCESS", saved.getRuleStatus());
       
        assertEquals(List.copyOf(failedValidations), saved.getFailedValidations());

        try {
            verify(payloadBuilderService, times(1)).buildDataQualityPayLoad(raw, converted);
        } catch (Exception e) {
            fail(e);
        }
        verify(sqsDataQualityRequestService, times(1)).forwardToDataQualityRequestQueue(payloadOut);
    }

    @Test
    @DisplayName("Rule flow: FAIL -> sets finalStatus=FAIL and does not send SQS")
    void updateRuleWorkflowStatus_fail_setsFinalStatus_noSqs() {
         
        Map<String, Object> raw = new HashMap<>();
        raw.put("status", "FAIL");
        raw.put("data", Map.of("id", "wf-9"));
        raw.put("failed_validations", List.of(Map.of("code", "E1")));

        when(dynamoService.tableExists(TABLE)).thenReturn(true);
        when(dynamoService.getDataByWorkflowStatusId(TABLE, "wf-9"))
                .thenReturn(Map.of("id", AttributeValue.builder().s("wf-9").build()));

        ArgumentCaptor<sg.edu.nus.iss.edgp.workflow.management.dto.WorkflowStatus> wsCaptor =
                ArgumentCaptor.forClass(sg.edu.nus.iss.edgp.workflow.management.dto.WorkflowStatus.class);
 
        service.updateRuleWorkflowStatus(raw);
 
        verify(dynamoService).updateWorkflowStatus(eq(TABLE), wsCaptor.capture());
        var saved = wsCaptor.getValue();
        assertEquals("FAIL", saved.getRuleStatus());
        assertEquals("FAIL", saved.getFinalStatus(), "finalStatus should be set to FAIL");
        
        verifyNoInteractions(payloadBuilderService);
        verifyNoInteractions(sqsDataQualityRequestService);
    }

    @Test
    @DisplayName("Creates table if missing before proceeding")
    void updateRuleWorkflowStatus_createsTableIfMissing() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("status", "SUCCESS");
        raw.put("data", Map.of("id", "wf-123"));

        when(dynamoService.tableExists(TABLE)).thenReturn(false);
        when(dynamoService.getDataByWorkflowStatusId(TABLE, "wf-123"))
                .thenReturn(Map.of("id", AttributeValue.builder().s("wf-123").build()));
        doReturn(Map.of("id", "wf-123")).when(service).dynamoItemToJavaMap(anyMap());

        service.updateRuleWorkflowStatus(raw);

        verify(dynamoService).createTable(TABLE);
        verify(dynamoService).updateWorkflowStatus(eq(TABLE), any());
    }

    @Test
    void updateRuleWorkflowStatus_missingExistingData_throws() {
      
        Map<String, Object> raw = new HashMap<>();
        raw.put("status", "SUCCESS");
        raw.put("data", Map.of("id", "wf-404"));

        when(dynamoService.tableExists(TABLE)).thenReturn(true);
        when(dynamoService.getDataByWorkflowStatusId(TABLE, "wf-404"))
                .thenReturn(Collections.emptyMap());
 
        WorkflowServiceException ex = assertThrows(
                WorkflowServiceException.class,
                () -> service.updateRuleWorkflowStatus(raw)
        );
 
        assertTrue(ex.getMessage().contains("An error occurred while updating workflow status"));
 
        assertNotNull(ex.getCause());
        assertTrue(ex.getCause().getMessage()
                .contains("Workflow status update aborted: existing workflow status data not found."));
    }


    @Test
    @DisplayName("Wraps unexpected exceptions into WorkflowServiceException")
    void updateRuleWorkflowStatus_wrapsException() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("status", "SUCCESS");
        raw.put("data", Map.of("id", "wf-err"));

        when(dynamoService.tableExists(TABLE)).thenReturn(true);
        when(dynamoService.getDataByWorkflowStatusId(TABLE, "wf-err"))
                .thenReturn(Map.of("id", AttributeValue.builder().s("wf-err").build()));
        
        doThrow(new RuntimeException("boom")).when(dynamoService)
                .updateWorkflowStatus(eq(TABLE), any(sg.edu.nus.iss.edgp.workflow.management.dto.WorkflowStatus.class));

        WorkflowServiceException ex = assertThrows(
                WorkflowServiceException.class,
                () -> service.updateRuleWorkflowStatus(raw)
        );
        assertTrue(ex.getMessage().contains("An error occurred while updating workflow status"));
    }
    
    @Test
    void dynamoItemToJavaMap_convertsScalarsCollectionsAndSets() {
        // Scalars
        AttributeValue sAttr = AttributeValue.builder().s("abc").build();
        AttributeValue nAttr = AttributeValue.builder().n("123.45").build();   // stays String per code
        AttributeValue bAttr = AttributeValue.builder().bool(true).build();

        // Sets
        AttributeValue ssAttr = AttributeValue.builder().ss("a", "b").build();
        AttributeValue nsAttr = AttributeValue.builder().ns("1", "2", "3").build();
        AttributeValue bsAttr = AttributeValue.builder()
                .bs(SdkBytes.fromByteArray(new byte[]{1,2}), SdkBytes.fromByteArray(new byte[]{3,4}))
                .build();

        // List (with mixed types)
        AttributeValue listAttr = AttributeValue.builder()
                .l(
                    AttributeValue.builder().s("x").build(),
                    AttributeValue.builder().n("1").build(),
                    AttributeValue.builder().bool(false).build()
                )
                .build();

        // Map (with nested map + list)
        AttributeValue nestedMapAttr = AttributeValue.builder()
                .m(Map.of(
                        "k1", AttributeValue.builder().s("v1").build(),
                        "k2", AttributeValue.builder().n("7").build(),
                        "innerList", AttributeValue.builder()
                                .l(
                                    AttributeValue.builder().s("y").build(),
                                    AttributeValue.builder().n("2").build()
                                ).build()
                ))
                .build();

        // Null-ish (DynamoDB NULL type). Your code returns null for anything not matched explicitly.
        AttributeValue nullAttr = AttributeValue.builder().nul(true).build();

        Map<String, AttributeValue> input = new LinkedHashMap<>();
        input.put("s", sAttr);
        input.put("n", nAttr);
        input.put("b", bAttr);
        input.put("ss", ssAttr);
        input.put("ns", nsAttr);
        input.put("bs", bsAttr);
        input.put("lst", listAttr);
        input.put("mp", nestedMapAttr);
        input.put("nil", nullAttr);

        Map<String, Object> out = service.dynamoItemToJavaMap(input);

      
        assertEquals("abc", out.get("s"));
        assertEquals("123.45", out.get("n"));
        assertEquals(true, out.get("b"));

      
        assertEquals(Arrays.asList("a", "b"), out.get("ss"));
        assertEquals(Arrays.asList("1", "2", "3"), out.get("ns"));

        @SuppressWarnings("unchecked")
        List<Object> bsOut = (List<Object>) out.get("bs");
        assertNotNull(bsOut);
        assertEquals(2, bsOut.size());
        assertTrue(bsOut.get(0) instanceof SdkBytes);
        assertTrue(bsOut.get(1) instanceof SdkBytes);

   
        @SuppressWarnings("unchecked")
        List<Object> lst = (List<Object>) out.get("lst");
        assertEquals(Arrays.asList("x", "1", false), lst);

     
        @SuppressWarnings("unchecked")
        Map<String, Object> mp = (Map<String, Object>) out.get("mp");
        assertNotNull(mp);
        assertEquals("v1", mp.get("k1"));
        assertEquals("7", mp.get("k2"));
        @SuppressWarnings("unchecked")
        List<Object> innerList = (List<Object>) mp.get("innerList");
        assertEquals(Arrays.asList("y", "2"), innerList);

       
        assertNull(out.get("nil"));
    }

    @Test
    void dynamoItemToJavaMap_handlesDeeplyNestedStructuresRecursively() {
        
        AttributeValue deep = AttributeValue.builder()
                .m(Map.of(
                        "level1", AttributeValue.builder().l(
                                AttributeValue.builder().m(Map.of(
                                        "level2_key", AttributeValue.builder().s("level2_value").build(),
                                        "level2_list", AttributeValue.builder().l(
                                                AttributeValue.builder().n("99").build(),
                                                AttributeValue.builder().bool(true).build()
                                        ).build()
                                )).build()
                        ).build()
                ))
                .build();

        Map<String, AttributeValue> input = Map.of("deep", deep);

        Map<String, Object> out = service.dynamoItemToJavaMap(input);

        @SuppressWarnings("unchecked")
        Map<String, Object> deepMap = (Map<String, Object>) out.get("deep");
        assertNotNull(deepMap);

        @SuppressWarnings("unchecked")
        List<Object> level1 = (List<Object>) deepMap.get("level1");
        assertEquals(1, level1.size());

        @SuppressWarnings("unchecked")
        Map<String, Object> level2 = (Map<String, Object>) level1.get(0);
        assertEquals("level2_value", level2.get("level2_key"));

        @SuppressWarnings("unchecked")
        List<Object> level2List = (List<Object>) level2.get("level2_list");
        assertEquals(Arrays.asList("99", true), level2List);
    }
}
