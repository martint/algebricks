package edu.uci.ics.algebricks.compiler.algebra.base;

public enum PhysicalOperatorTag {
    AGGREGATE,
    ASSIGN,
    BROADCAST_EXCHANGE,
    BTREE_SEARCH,
    DATASOURCE_SCAN,
    EMPTY_TUPLE_SOURCE,
    EXTERNAL_GROUP_BY,
    IN_MEMORY_HASH_JOIN,
    HASH_GROUP_BY,
    HASH_PARTITION_EXCHANGE,
    HASH_PARTITION_MERGE_EXCHANGE,
    HYBRID_HASH_JOIN,
    HDFS_READER,
    IN_MEMORY_STABLE_SORT,
    MICRO_PRE_CLUSTERED_GROUP_BY,
    NESTED_LOOP,
    NESTED_TUPLE_SOURCE,
    ONE_TO_ONE_EXCHANGE,
    PRE_SORTED_DISTINCT_BY,
    PRE_CLUSTERED_GROUP_BY,
    RANGE_PARTITION_EXCHANGE,
    RANDOM_MERGE_EXCHANGE,
    RUNNING_AGGREGATE,
    SORT_MERGE_EXCHANGE,
    SINK,
    SINK_WRITE,
    SPLIT,
    STABLE_SORT,
    STREAM_LIMIT,
    STREAM_SELECT,
    STREAM_PROJECT,
    STRING_STREAM_SCRIPT,
    SUBPLAN,
    UNION_ALL,
    UNNEST,
    WRITE_RESULT
}
