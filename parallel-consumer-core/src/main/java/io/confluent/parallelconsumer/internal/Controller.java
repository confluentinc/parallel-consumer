package io.confluent.parallelconsumer.internal;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.PollContextInternal;
import io.confluent.parallelconsumer.state.WorkManager;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.confluent.csid.utils.BackportUtils.isEmpty;
import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.internal.State.running;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
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

    /**
     * If the system failed with an exception, it is referenced here.
     */
    private Exception failureReason;

    /**
     * The pool which is used for running the users' supplied function
     */
    protected final ThreadPoolExecutor workerThreadPool;

    // todo make private
    @Getter(PRIVATE)
    private WorkMailbox<K, V> workMailbox;

    // todo make package level
    @Getter(PRIVATE)
    protected final WorkManager<K, V> wm;

    private PCWorkerPool<K, V, Object> workerPool;

    private final BrokerPollSystem<K, V> brokerPollSubsystem;

    private final OffsetCommitter committer;

    /**
     * Used to request a commit asap
     */
    private final AtomicBoolean commitCommand = new AtomicBoolean(false);

    @Getter(PROTECTED)
    private final Optional<ProducerManager<K, V>> producerManager;

    /**
     * Multiple of {@link ParallelConsumerOptions#getMaxConcurrency()} to have in our processing queue, in order to make
     * sure threads always have work to do.
     */
    protected final DynamicLoadFactor dynamicExtraLoadFactor;

    /**
     * Time of last successful commit
     */
    private Instant lastCommitTime;

    private Instant lastCommitCheckTime = Instant.now();

    private final RateLimiter queueStatsLimiter = new RateLimiter();

    /**
     * Control for stepping loading factor - shouldn't step if work requests can't be fulfilled due to restrictions.
     * (e.g. we may want 10, but maybe there's a single partition and we're in partition mode - stepping up won't
     * help).
     */
    private boolean lastWorkRequestWasFulfilled = false;

    // todo depends on MicroMeter pr
    private final SimpleMeterRegistry metricsRegistry = new SimpleMeterRegistry();

    private final Timer workRetrievalTimer = metricsRegistry.timer("user.function");


    public Controller(@NonNull PCModule<K, V> module) {
        this.brokerPollSubsystem = module.brokerPoller();
        this.module = module;
        this.options = module.options();
        this.state = module.stateMachine();
        this.consumer = options.getConsumer();
        this.workerThreadPool = createWorkerPool(options.getMaxConcurrency());
        this.workMailbox = module.workMailbox();
        this.dynamicExtraLoadFactor = module.dynamicExtraLoadFactor();
        this.wm = module.workManager();

        producerManager = initProducerManager();
        committer = getCommitter();
    }

    private OffsetCommitter getCommitter() {
        if (options.isUsingTransactionCommitMode())
            return producerManager.get();
        else
            return brokerPollSubsystem;
    }

    private Optional<ProducerManager<K, V>> initProducerManager() {
        if (options.isProducerSupplied()) {
            return Optional.of(module.producerManager());
        } else {
            return Optional.empty();
        }
    }

    protected ThreadPoolExecutor createWorkerPool(int poolSize) {
        ThreadFactory defaultFactory;
        try {
            defaultFactory = InitialContext.doLookup(options.getManagedThreadFactory());
        } catch (NamingException e) {
            log.debug("Using Java SE Thread", e);
            defaultFactory = Executors.defaultThreadFactory();
        }
        ThreadFactory finalDefaultFactory = defaultFactory;
        ThreadFactory namingThreadFactory = r -> {
            Thread thread = finalDefaultFactory.newThread(r);
            String name = thread.getName();
            thread.setName("pc-" + name);
            return thread;
        };
        ThreadPoolExecutor.AbortPolicy rejectionHandler = new ThreadPoolExecutor.AbortPolicy();
        LinkedBlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
        return new ThreadPoolExecutor(poolSize, poolSize, 0L, MILLISECONDS, workQueue,
                namingThreadFactory, rejectionHandler);
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
        if (state == State.unused) {
            state = running;
        } else {
            throw new IllegalStateException(msg("Invalid state - you cannot call the poll* or pollAndProduce* methods " +
                    "more than once (they are asynchronous) (current state is {})", state));
        }

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
        Callable<Boolean> controlTask = () -> superviseControlLoop((Function) userFunctionWrapped, (Consumer<Object>) callback);
        Future<Boolean> controlTaskFutureResult = executorService.submit(controlTask);
        this.controlThreadFuture = Optional.of(controlTaskFutureResult);
    }

    // todo delete type param
    // todo name
    private <R> boolean superviseControlLoop(Function userFunctionWrapped, Consumer<Object> callback) throws Exception {
        addInstanceMDC(options);
        log.info("Control loop starting up...");

        initWorkerPool(userFunctionWrapped, callback);

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
                log.error("Error from poll control thread, will attempt controlled shutdown, then rethrow. Error: " + e.getMessage(), e);
                failureReason = new RuntimeException("Error from poll control thread: " + e.getMessage(), e);
                state.doClose(DrainingCloseable.DEFAULT_TIMEOUT); // attempt to close
                throw failureReason;
            }
        }
        log.info("Control loop ending clean (state:{})...", state);
        return true;
    }

    private void initWorkerPool(Function userFunctionWrapped, Consumer<Object> callback) {
        // todo casts
        Function<PollContextInternal<K, V>, List<Object>> cast = userFunctionWrapped;
        var runner = FunctionRunner.<K, V, Object>builder()
                .userFunctionWrapped(cast)
                .callback(callback)
                .options(options)
                .module(module)
                .workMailbox(workMailbox)
                .workManager(wm)
                .build();
        workerPool = new PCWorkerPool<>(options.getMaxConcurrency(), runner, options);
    }

    /**
     * @return aim to never have the pool queue drop below this
     */
    private int getPoolLoadTarget() {
        return options.getTargetAmountOfRecordsInFlight();
    }

    private int getNumberOfUserFunctionsQueued() {
        return workerThreadPool.getQueue().size();
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

    private boolean isRecordsAwaitingProcessing() {
        boolean isRecordsAwaitingProcessing = wm.isRecordsAwaitingProcessing();
        boolean threadsDone = areMyThreadsDone();
        log.trace("isRecordsAwaitingProcessing {} || threadsDone {}", isRecordsAwaitingProcessing, threadsDone);
        return isRecordsAwaitingProcessing || threadsDone;
    }

    /**
     * Control thread can be blocked waiting for work, but is interruptible. Interrupting it can be useful to inform
     * that work is available when there was none, to make tests run faster, or to move on to shutting down the
     * {@link BrokerPollSystem} so that less messages are downloaded and queued.
     */
    private void interruptControlThread() {
        if (blockableControlThread != null) {
            log.debug("Interrupting {} thread in case it's waiting for work", blockableControlThread.getName());
            blockableControlThread.interrupt();
        }
    }

    private boolean areMyThreadsDone() {
        if (isEmpty(controlThreadFuture)) {
            // not constructed yet, will become alive, unless #poll is never called
            return false;
        } else {
            return controlThreadFuture.get().isDone();
        }
    }

    /**
     * Request a commit as soon as possible (ASAP), overriding other constraints.
     */
    public void requestCommitAsap() {
        log.debug("Registering command to commit next chance");
        synchronized (commitCommand) {
            this.commitCommand.set(true);
        }
        notifySomethingToDo();
    }

    private boolean isCommandedToCommit() {
        synchronized (commitCommand) {
            return this.commitCommand.get();
        }
    }


}