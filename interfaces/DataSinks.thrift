// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

namespace cpp impala
namespace java org.apache.impala.thrift

include "Exprs.thrift"
include "Types.thrift"
include "Descriptors.thrift"
include "Partitions.thrift"

enum TDataSinkType {
  DATA_STREAM_SINK,
  TABLE_SINK,
  JOIN_BUILD_SINK,
  PLAN_ROOT_SINK
}

enum TSinkAction {
  INSERT,
  UPDATE,
  UPSERT,
  DELETE
}

enum TTableSinkType {
  HDFS,
  HBASE,
  KUDU
}

// Sink which forwards data to a remote plan fragment,
// according to the given output partition specification
// (ie, the m:1 part of an m:n data stream)
struct TDataStreamSink {
  // destination node id
  1: required Types.TPlanNodeId dest_node_id

  // Specification of how the output of a fragment is partitioned.
  // If the partitioning type is UNPARTITIONED, the output is broadcast
  // to each destination host.
  2: required Partitions.TDataPartition output_partition
}

// Creates a new Hdfs files according to the evaluation of the partitionKeyExprs,
// and materializes all its input RowBatches as a Hdfs file.
struct THdfsTableSink {
  1: required list<Exprs.TExpr> partition_key_exprs
  2: required bool overwrite

  // The 'skip.header.line.count' property of the target Hdfs table. We will insert this
  // many empty lines at the beginning of new text files, which will be skipped by the
  // scanners while reading from the files.
  3: optional i32 skip_header_line_count

  // This property indicates to the table sink whether the input is ordered by the
  // partition keys, meaning partitions can be opened, written, and closed one by one.
  4: required bool input_is_clustered

  // Stores the indices into the list of non-clustering columns of the target table that
  // are stored in the 'sort.columns' table property. This is used in the backend to
  // populate the RowGroup::sorting_columns list in parquet files.
  5: optional list<i32> sort_columns
}

// Structure to encapsulate specific options that are passed down to the KuduTableSink
struct TKuduTableSink {
  // The position in this vector is equal to the position in the output expressions of the
  // sink and holds the index of the corresponsding column in the Kudu schema,
  // e.g. 'exprs[i]' references 'kudu_table.column(referenced_cols[i])'
  1: optional list<i32> referenced_columns

  // Defines if duplicate or not found keys should be ignored
  2: optional bool ignore_not_found_or_duplicate
}

// Sink to create the build side of a JoinNode.
struct TJoinBuildSink {
  1: required Types.TJoinTableId join_table_id

  // only set for hash join build sinks
  2: required list<Exprs.TExpr> build_exprs
}

// Union type of all table sinks.
struct TTableSink {
  1: required Types.TTableId target_table_id
  2: required TTableSinkType type
  3: required TSinkAction action
  4: optional THdfsTableSink hdfs_table_sink
  5: optional TKuduTableSink kudu_table_sink
}

struct TDataSink {
  1: required TDataSinkType type
  2: optional TDataStreamSink stream_sink
  3: optional TTableSink table_sink
  4: optional TJoinBuildSink join_build_sink
}
