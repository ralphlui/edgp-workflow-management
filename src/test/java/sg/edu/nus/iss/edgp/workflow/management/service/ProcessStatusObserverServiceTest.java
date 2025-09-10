package sg.edu.nus.iss.edgp.workflow.management.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.paginators.ScanIterable;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import sg.edu.nus.iss.edgp.workflow.management.service.impl.ProcessStatusObserverService;
import sg.edu.nus.iss.edgp.workflow.management.utility.Status;
import sg.edu.nus.iss.edgp.workflow.management.enums.FileProcessStage;

@ExtendWith(MockitoExtension.class)
class ProcessStatusObserverServiceTest {

	@Mock
	private DynamoDbClient dynamoDbClient;

	private ProcessStatusObserverService service;

	@BeforeEach
	void setUp() {
		service = new ProcessStatusObserverService(dynamoDbClient);

		ReflectionTestUtils.setField(service, "masterDataHeaderTableName", " header_tbl ");
		ReflectionTestUtils.setField(service, "masterDataTaskTrackerTableName", " task_tbl ");
	}

	@Test
	void fetchOldestIdByProcessStage_picksOldestUploadedDate_andSkipsInvalid() {

		Map<String, AttributeValue> invalidNoId = Map.of("file_name", AttributeValue.builder().s("A.csv").build(),
				"uploaded_date", AttributeValue.builder().s("2025-09-08T10:00:00Z").build());
		Map<String, AttributeValue> invalidNoDate = Map.of("id", AttributeValue.builder().s("id-should-skip").build(),
				"file_name", AttributeValue.builder().s("B.csv").build());
		Map<String, AttributeValue> newer = Map.of("id", AttributeValue.builder().s("id-newer").build(), "file_name",
				AttributeValue.builder().s("C.csv").build(), "uploaded_date",
				AttributeValue.builder().s("2025-09-09T10:00:00Z").build());
		Map<String, AttributeValue> oldest = Map.of("id", AttributeValue.builder().s("id-oldest").build(), "file_name",
				AttributeValue.builder().s("D.csv").build(), "uploaded_date",
				AttributeValue.builder().s("2025-09-01T00:00:00Z").build());

		ScanResponse page = ScanResponse.builder().items(invalidNoId, invalidNoDate, newer, oldest).build();

		ScanIterable paginator = mock(ScanIterable.class);
		when(paginator.iterator()).thenReturn(List.of(page).iterator());
		when(dynamoDbClient.scanPaginator(any(ScanRequest.class))).thenReturn(paginator);

		HashMap<String, String> result = service.fetchOldestIdByProcessStage(FileProcessStage.PROCESSING);

		assertEquals("id-oldest", result.get("id"));
		assertEquals("D.csv", result.get("name"));

		org.mockito.ArgumentCaptor<ScanRequest> cap = ArgumentCaptor.forClass(ScanRequest.class);
		verify(dynamoDbClient).scanPaginator(cap.capture());
		assertEquals("header_tbl", cap.getValue().tableName());
		assertTrue(cap.getValue().filterExpression().contains("#ps = :ps"));
		assertEquals("process_stage", cap.getValue().expressionAttributeNames().get("#ps"));
		assertNotNull(cap.getValue().expressionAttributeValues().get(":ps"));
	}

	@Test
	void fetchOldestIdByProcessStage_emptyPages_returnsEmptyMap() {
		ScanResponse emptyPage = ScanResponse.builder().items(Collections.emptyList()).build();
		ScanIterable paginator = mock(ScanIterable.class);
		when(paginator.iterator()).thenReturn(List.of(emptyPage).iterator());
		when(dynamoDbClient.scanPaginator(any(ScanRequest.class))).thenReturn(paginator);

		HashMap<String, String> result = service.fetchOldestIdByProcessStage(FileProcessStage.UNPROCESSED);
		assertTrue(result.isEmpty());
	}

	@Test
	void isAllDataProcessed_trueWhenAllFinalStatusNonBlank() {

		Map<String, AttributeValue> i1 = Map.of("final_status", AttributeValue.builder().s("success").build());
		Map<String, AttributeValue> i2 = Map.of("final_status", AttributeValue.builder().s("FAIL").build());

		ScanResponse resp = ScanResponse.builder().items(i1, i2).build();
		when(dynamoDbClient.scan(any(ScanRequest.class))).thenReturn(resp);

		boolean allDone = service.isAllDataProcessed("file-1");
		assertTrue(allDone);

		org.mockito.ArgumentCaptor<ScanRequest> cap = ArgumentCaptor.forClass(ScanRequest.class);
		verify(dynamoDbClient).scan(cap.capture());
		assertEquals("task_tbl", cap.getValue().tableName());
		assertTrue(cap.getValue().filterExpression().contains("file_id = :fid"));
		assertEquals("file-1", cap.getValue().expressionAttributeValues().get(":fid").s());
	}

	@Test
	void isAllDataProcessed_falseIfAnyFinalStatusBlankOrMissing() {
		Map<String, AttributeValue> ok = Map.of("final_status", AttributeValue.builder().s("success").build());
		Map<String, AttributeValue> missing = Map.of(); // missing final_status

		ScanResponse resp = ScanResponse.builder().items(ok, missing).build();
		when(dynamoDbClient.scan(any(ScanRequest.class))).thenReturn(resp);

		assertFalse(service.isAllDataProcessed("file-2"));
	}

	@Test
	void getAllStatusForFile_returnsFailIfAnyPageHasFail() {
		Map<String, AttributeValue> ok = Map.of("final_status", AttributeValue.builder().s("success").build());
		Map<String, AttributeValue> fail = Map.of("final_status", AttributeValue.builder().s("fail").build());

		ScanResponse page1 = ScanResponse.builder().items(ok).build();
		ScanResponse page2 = ScanResponse.builder().items(fail).build();

		ScanIterable paginator = mock(ScanIterable.class);
		when(paginator.iterator()).thenReturn(List.of(page1, page2).iterator());
		when(dynamoDbClient.scanPaginator(any(ScanRequest.class))).thenReturn(paginator);

		String status = service.getAllStatusForFile("file-3");
		assertEquals(Status.fail.toString(), status);

		org.mockito.ArgumentCaptor<ScanRequest> cap = ArgumentCaptor.forClass(ScanRequest.class);
		verify(dynamoDbClient).scanPaginator(cap.capture());
		assertEquals("task_tbl", cap.getValue().tableName());
		assertTrue(cap.getValue().filterExpression().contains("#fid = :fid"));
		assertEquals("file_id", cap.getValue().expressionAttributeNames().get("#fid"));
	}

	@Test
	void getAllStatusForFile_returnsSuccessIfNoFailFound() {
		Map<String, AttributeValue> ok1 = Map.of("final_status", AttributeValue.builder().s("SUCCESS").build());
		Map<String, AttributeValue> ok2 = Map.of("final_status", AttributeValue.builder().s("done").build());

		ScanResponse page = ScanResponse.builder().items(ok1, ok2).build();

		ScanIterable paginator = mock(ScanIterable.class);
		when(paginator.iterator()).thenReturn(List.of(page).iterator());
		when(dynamoDbClient.scanPaginator(any(ScanRequest.class))).thenReturn(paginator);

		String status = service.getAllStatusForFile("file-4");
		assertEquals(Status.success.toString(), status);
	}

	@Test
	void updateFileStageAndStatus_buildsExpectedUpdateItemRequest() {

		service.updateFileStageAndStatus("fid-1", FileProcessStage.PROCESSING, "IN_PROGRESS");

		org.mockito.ArgumentCaptor<UpdateItemRequest> cap = ArgumentCaptor.forClass(UpdateItemRequest.class);
		verify(dynamoDbClient).updateItem(cap.capture());
		UpdateItemRequest req = cap.getValue();

		assertEquals("header_tbl", req.tableName());
		assertEquals("fid-1", req.key().get("id").s());
		assertEquals("attribute_exists(id)", req.conditionExpression());
		assertEquals(ReturnValue.UPDATED_NEW, req.returnValues());

		assertEquals("process_stage", req.expressionAttributeNames().get("#stage"));
		assertEquals("file_status", req.expressionAttributeNames().get("#status"));

		assertEquals(FileProcessStage.PROCESSING.name(), req.expressionAttributeValues().get(":stage").s());
		assertEquals("IN_PROGRESS", req.expressionAttributeValues().get(":status").s());
		assertNotNull(req.expressionAttributeValues().get(":now").s());
	}

	private static final class ArgumentCaptor {
		static <T> org.mockito.ArgumentCaptor<T> forClass(Class<T> c) {
			return org.mockito.ArgumentCaptor.forClass(c);
		}

		static org.mockito.ArgumentCaptor<UpdateItemRequest> UpdateItemRequest = org.mockito.ArgumentCaptor
				.forClass(UpdateItemRequest.class);
	}
}
