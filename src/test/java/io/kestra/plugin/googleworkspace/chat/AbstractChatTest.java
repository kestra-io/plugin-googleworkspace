package io.kestra.plugin.googleworkspace.chat;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.runners.TestRunnerUtils;
import io.kestra.core.utils.Await;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.context.ApplicationContext;
import io.micronaut.runtime.server.EmbeddedServer;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static io.kestra.core.tenant.TenantService.MAIN_TENANT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@KestraTest
public class AbstractChatTest {
    @Inject
    protected EmbeddedServer embeddedServer;

    @Inject
    protected ApplicationContext applicationContext;

    @Inject
    @Named(QueueFactoryInterface.EXECUTION_NAMED)
    protected QueueInterface<Execution> executionQueue;

    @Inject
    protected TestRunnerUtils runnerUtils;

    @BeforeAll
    void startServer() {
        embeddedServer = applicationContext.getBean(EmbeddedServer.class);
        embeddedServer.start();
    }

    @AfterAll
    void stopServer() {
        if (embeddedServer != null) {
            embeddedServer.stop();
        }
    }

    @BeforeEach
    void reset() {
        FakeWebhookController.data = null;
    }

    /**
     * waits for a webhook data to return a non-null value.
     *
     * @param dataSupplier supplier function that provides the data to check
     * @param timeoutMs The maximum time to wait in milliseconds.
     * @return The received data string.
     * @throws InterruptedException if the thread is interrupted while sleeping.
     * @throws TimeoutException if the data does not become non-null within the timeout period.
     */
    public static String waitForWebhookData(Supplier<String> dataSupplier, long timeoutMs) throws InterruptedException, TimeoutException {
        try {
            return Await.until(
                dataSupplier::get,
                Duration.ofMillis(100),
                Duration.ofSeconds(5)
            );
        } catch (TimeoutException e) {
            throw new TimeoutException("Webhook data did not arrive within " + timeoutMs + "ms.");
        }
    }

    protected Execution runAndCaptureExecution(String triggeringFlowId, String notificationFlowId) throws Exception {
        CountDownLatch queueCount = new CountDownLatch(1);
        AtomicReference<Execution> last = new AtomicReference<>();

        Flux<Execution> receive = TestsUtils.receive(executionQueue, execution -> {
            if (execution.getLeft().getFlowId().equals(notificationFlowId)) {
                last.set(execution.getLeft());
                queueCount.countDown();
            }
        });

        Execution execution;

        execution = runnerUtils.runOne(
            MAIN_TENANT,
            "io.kestra.tests",
            triggeringFlowId
        );

        boolean await = queueCount.await(20, TimeUnit.SECONDS);
        assertThat(await, is(true));

        Execution triggeredExecution = last.get();
        assertThat(triggeredExecution, notNullValue());
        assertThat(triggeredExecution.getTrigger().getVariables().get("executionId"), is(execution.getId()));

        receive.blockLast();

        return execution;
    }
}