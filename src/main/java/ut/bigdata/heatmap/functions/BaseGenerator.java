package ut.bigdata.heatmap.functions;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Preconditions;

import java.util.SplittableRandom;

public abstract class BaseGenerator<T> extends RichParallelSourceFunction<T> implements CheckpointedFunction {
    private static final long serialVersionUID = 1L;
    protected int maxRecordsPerSecond;
    private volatile boolean running = true;
    private long id = -1;
    private transient ListState<Long> idState;

    protected BaseGenerator() {
        this.maxRecordsPerSecond = -1;
    }

    protected BaseGenerator(int maxRecordsPerSecond) {
        Preconditions.checkArgument(
            maxRecordsPerSecond == -1 || maxRecordsPerSecond > 0,
            "maxRecordsPerSecond must be positive or -1 (infinite)");
        this.maxRecordsPerSecond = maxRecordsPerSecond;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        if (id == -1) {
            id = getRuntimeContext().getIndexOfThisSubtask();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        idState.clear();
        idState.add(id);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        idState = context.getOperatorStateStore().getUnionListState(new ListStateDescriptor<Long>("ids", BasicTypeInfo.LONG_TYPE_INFO));
        if (context.isRestored()) {
            long max = Long.MIN_VALUE;
            for (Long value : idState.get()) {
                max = Math.max(max, value);
            }

            id = max + getRuntimeContext().getIndexOfThisSubtask();
        }
    }

    @Override
    public void run(SourceContext<T> context) throws Exception {
        final int numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
        final Throttler throttler = new Throttler(maxRecordsPerSecond, numberOfParallelSubtasks);
        final SplittableRandom rnd = new SplittableRandom();
        final Object lock = context.getCheckpointLock();

        while (running) {
            T event = randomEvent(rnd, id);

            synchronized (lock) {
                context.collect(event);
                id += numberOfParallelSubtasks;
            }

            throttler.throttle();
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    public abstract T randomEvent(SplittableRandom rnd, long id);

    public int getMaxRecordsPerSecond() {
        return maxRecordsPerSecond;
    }
}
