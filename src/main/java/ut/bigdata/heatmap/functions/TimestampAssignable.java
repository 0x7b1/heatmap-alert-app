package ut.bigdata.heatmap.functions;

public interface TimestampAssignable<T> {
  void assignIngestionTimestamp(T timestamp);
}
