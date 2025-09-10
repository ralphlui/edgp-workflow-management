package sg.edu.nus.iss.edgp.workflow.management.observer;


import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import sg.edu.nus.iss.edgp.workflow.management.enums.FileProcessStage;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.DataIngestionNotifierService;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.DynamicDynamoService;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.ProcessStatusObserverService;
import sg.edu.nus.iss.edgp.workflow.management.service.impl.WorkflowService;

@ExtendWith(MockitoExtension.class)
class ProcessStatusObserverSchedulerTest {

    @Mock private WorkflowService workflowService;
    @Mock private ProcessStatusObserverService processStatusObserverService;
    @Mock private DynamicDynamoService dynamoService;
    @Mock private DataIngestionNotifierService workflowNotificationService;

    @InjectMocks
    private ProcessStatusObserverScheduler scheduler;

    private static final String HEADER_TABLE = "mdm_header_tbl";
    private static final String TASK_TBL = "mdm_task_tbl";

    @BeforeEach
    void setUp() {
       
        ReflectionTestUtils.setField(scheduler, "masterDataHeaderTableName", " " + HEADER_TABLE + " ");
        ReflectionTestUtils.setField(scheduler, "masterDataTaskTrackerTableName", " " + TASK_TBL + " ");
    }

    @Test
    void whenTablesMissing() {
        when(dynamoService.tableExists(HEADER_TABLE)).thenReturn(false);

        scheduler.checkWorkflowStatus();

        verify(dynamoService).tableExists(HEADER_TABLE);
        verifyNoInteractions(processStatusObserverService, workflowService, workflowNotificationService);
    }

    @Test
    void whenNoProcessingFiles_null_returns() {
        bothTablesExist();

        when(processStatusObserverService.fetchOldestIdByProcessStage(FileProcessStage.PROCESSING))
                .thenReturn(null);

        scheduler.checkWorkflowStatus();

        verify(processStatusObserverService).fetchOldestIdByProcessStage(FileProcessStage.PROCESSING);
        verifyNoMoreInteractions(processStatusObserverService);
        verifyNoInteractions(workflowService, workflowNotificationService);
    }

    @Test
    void whenNoProcessingFiles_emptyMap_returns() {
        bothTablesExist();

        when(processStatusObserverService.fetchOldestIdByProcessStage(FileProcessStage.PROCESSING))
                .thenReturn(new HashMap<>());

        scheduler.checkWorkflowStatus();

        verify(processStatusObserverService).fetchOldestIdByProcessStage(FileProcessStage.PROCESSING);
        verifyNoMoreInteractions(processStatusObserverService);
        verifyNoInteractions(workflowService, workflowNotificationService);
    }

    @Test
    void whenFileNotFullyProcessed_doesNotUpdateOrEmail() {
        bothTablesExist();

        HashMap<String, String> fileInfo = new HashMap<>();
        fileInfo.put("id", "  file-123  "); // verify .trim() handling
        when(processStatusObserverService.fetchOldestIdByProcessStage(FileProcessStage.PROCESSING))
                .thenReturn(new HashMap<>(fileInfo));

        when(workflowService.isAllDataProcessed("file-123")).thenReturn(false);

        scheduler.checkWorkflowStatus();

        verify(processStatusObserverService).fetchOldestIdByProcessStage(FileProcessStage.PROCESSING);
        verify(workflowService).isAllDataProcessed("file-123");
        verify(processStatusObserverService, never()).updateFileStageAndStatus(anyString(), any(), anyString());
       

    }

    @Test
    void whenFileProcessed_updatesStageThenSendsEmail() {
        bothTablesExist();

        HashMap<String, String> fileInfo = new HashMap<>();
        fileInfo.put("id", " file-999 ");
        when(processStatusObserverService.fetchOldestIdByProcessStage(FileProcessStage.PROCESSING))
                .thenReturn(new HashMap<>(fileInfo));

        when(workflowService.isAllDataProcessed("file-999")).thenReturn(true);
        when(processStatusObserverService.getAllStatusForFile("file-999")).thenReturn("OK");

        scheduler.checkWorkflowStatus();

        InOrder inOrder = inOrder(processStatusObserverService, workflowNotificationService);
        verify(processStatusObserverService).fetchOldestIdByProcessStage(FileProcessStage.PROCESSING);
        verify(workflowService).isAllDataProcessed("file-999");
        verify(processStatusObserverService).getAllStatusForFile("file-999");

        inOrder.verify(processStatusObserverService)
               .updateFileStageAndStatus("file-999", FileProcessStage.COMPLETE, "OK");
        inOrder.verify(workflowNotificationService).sendDataIngestionResult(fileInfo);

        verifyNoMoreInteractions(processStatusObserverService, workflowNotificationService);
    }

    @Test
    void whenDownstreamThrows_schedulerDoesNotCrash() {
        bothTablesExist();

        Map<String, String> fileInfo = Map.of("id", "file-ex");
        when(processStatusObserverService.fetchOldestIdByProcessStage(FileProcessStage.PROCESSING))
                .thenReturn(new HashMap<>(fileInfo));

        when(workflowService.isAllDataProcessed("file-ex"))
                .thenThrow(new RuntimeException("boom"));

       
        scheduler.checkWorkflowStatus();

        verify(processStatusObserverService, never()).getAllStatusForFile(anyString());
        verify(processStatusObserverService, never())
                .updateFileStageAndStatus(anyString(), any(), anyString());
        verifyNoInteractions(workflowNotificationService);
    }


    private void bothTablesExist() {
        when(dynamoService.tableExists(HEADER_TABLE)).thenReturn(true);
        when(dynamoService.tableExists(TASK_TBL)).thenReturn(true);
    }
}

