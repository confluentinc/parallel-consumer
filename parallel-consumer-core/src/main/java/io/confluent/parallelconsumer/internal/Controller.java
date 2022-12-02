package io.confluent.parallelconsumer.internal;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.PollContextInternal;
import io.confluent.parallelconsumer.state.WorkManager;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;

import static lombok.AccessLevel.PRIVATE;
import static lombok.AccessLevel.PROTECTED;

/**
 * @author Antony Stubbs
 */
@Slf4j
public class Controller<K, V> implements DrainingCloseable {

    public static final String MDC_INSTANCE_ID = "pcId";

    private final PCModule<K, V> module;

    @Getter(PROTECTED)
    protected final ParallelConsumerOptions<K, V> options;

    // todo decide delegate or not
    @Delegate
    private final StateMachine state;

    private ControlLoop<K, V> controlLoop;

    private Optional<Future<Boolean>> controlThreadFuture = Optional.empty();

    /**
     * Reference to the control thread, used for waking up a blocking poll ({@link BlockingQueue#poll}) against a
     * collection sooner.
     *
     * @see #processWorkCompleteMailBox
     */
    private Thread blockableControlThread;

    // todo make private
    @Getter(PRIVATE)
    private WorkMailbox<K, V> workMailbox;

    // todo make package level
    @Getter(PRIVATE)
    protected final WorkManager<K, V> wm;

    private final BrokerPollSystem<K, V> brokerPollSubsystem;

    // todo make private
    @Getter(PRIVATE)
    private final Optional<ProducerManager<K, V>> producerManager;

    /**
     * Multiple of {@link ParallelConsumerOptions#getMaxConcurrency()} to have in our processing queue, in order to make
     * sure threads always have work to do.
     */
    protected final DynamicLoadFactor dynamicExtraLoadFactor;

    /**
     * Control for stepping loading factor - shouldn't step if work requests can't be fulfilled due to restrictions.
     * (e.g. we may want 10, but maybe there's a single partition and we're in partition mode - stepping up won't
     * help).
     */
    private boolean lastWorkRequestWasFulfilled = false;

    public Controller(@NonNull PCModule<K, V> module) {
        this.brokerPollSubsystem = module.brokerPoller();
        this.module = module;
        this.options = module.options();
        this.state = module.stateMachine();
        this.workMailbox = module.workMailbox();
        this.dynamicExtraLoadFactor = module.dynamicExtraLoadFactor();
        this.wm = module.workManager();

        producerManager = initProducerManager();
    }

    private Optional<ProducerManager<K, V>> initProducerManager() {
        if (options.isProducerSupplied()) {
            return module.producerManager();
        } else {
            return Optional.empty();
        }
    }

    /**
     * Useful when testing with more than one instance
     */
    public static void addInstanceMDC(ParallelConsumerOptions<?, ?> options) {
        options.getMyId().ifPresent(id -> MDC.put(MDC_INSTANCE_ID, id));
    }

    /**
     * Kicks off the control loop in the executor, with supervision and returns.
     *
     * @see #supervisorLoop(Function, Consumer)
     */
    // todo rename
    protected <R> void supervisorLoop(Function<PollContextInternal<K, V>, List<R>> userFunctionWrapped,
                                      Consumer<R> callback) {
        state.transitionToRunning();

        // broker poll subsystem
        brokerPollSubsystem.start(options.getManagedExecutorService());

        // todo shouldn't the worker pool use the same executor service?
        ExecutorService executorService;
        try {
            executorService = InitialContext.doLookup(options.getManagedExecutorService());
        } catch (NamingException e) {
            log.debug("Using Java SE Thread", e);
            executorService = Executors.newSingleThreadExecutor();
        }

        // run main pool loop in thread
        // todo casts
        Callable<Boolean> controlTask = () -> superviseControlLoop((Function) userFunctionWrapped, (Consumer<Object>) callback);
        Future<Boolean> controlTaskFutureResult = executorService.submit(controlTask);
        this.controlThreadFuture = Optional.of(controlTaskFutureResult);
    }

    // todo delete type param
    // todo name
    private <R> boolean superviseControlLoop(Function userFunctionWrapped, Consumer<Object> callback) throws Exception {
        addInstanceMDC(options);
        log.info("Control loop starting up...");

        controlLoop.initWorkerPool(userFunctionWrapped, callback);

        Thread controlThread = Thread.currentThread();
        controlThread.setName("pc-control");
        this.blockableControlThread = controlThread;
        while (state.isOpen()) {
            log.debug("Control loop start");
            try {
                controlLoop.loop();

                // sanity - supervise the poller
                brokerPollSubsystem.supervise();

                wm.logState();
            } catch (InterruptedException e) {
                log.debug("Control loop interrupted, closing");
                state.doClose(DrainingCloseable.DEFAULT_TIMEOUT);
            } catch (Exception e) {
                throw state.handleCrash(e);
            }
        }
        log.info("Control loop ending clean (state:{})...", state);
        return true;
    }

    /**
     * Early notify of work arrived.
     * <p>
     * Only wake up the thread if it's sleeping while polling the mail box.
     *
     * @see #processWorkCompleteMailBox
     * @see #blockableControlThread
     */
    public void notifySomethingToDo() {
        boolean noTransactionInProgress = !producerManager.map(ProducerManager::isTransactionCommittingInProgress).orElse(false);
        if (noTransactionInProgress) {
            log.trace("Interrupting control thread: Knock knock, wake up! You've got mail (tm)!");
            interruptControlThread();
        } else {
            log.trace("Would have interrupted control thread, but TX in progress");
        }
    }

    /**
     * Control thread can be blocked waiting for work, but is interruptible. Interrupting it can be useful to inform
     * that work is available when there was none, to make tests run faster, or to move on to shutting down the
     * {@link BrokerPollSystem} so that fewer messages are downloaded and queued.
     */
    private void interruptControlThread() {
        if (blockableControlThread != null) {
            log.debug("Interrupting {} thread in case it's waiting for work", blockableControlThread.getName());
            blockableControlThread.interrupt();
        }
    }
}