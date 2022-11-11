/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package castle;

import datasources.NYCTaxiRideSource;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * Skeleton code for the datastream walkthrough
 */
public class TransactionGeneralizationJob {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		/*DataStream<String> lines = env
				.readTextFile("/Users/Augustin/Desktop/augustin/uni/ROC/project/data/database/part_s.tbl")
				.name("Read data");*/

		// Initialize NYCTaxi DataSource
		DataStream<Tuple> messageStream = env
				.addSource(new NYCTaxiRideSource(60000, 1000)).setParallelism(1); //runtime of -1 means throughput/sec

		TypeInformation[] types = new TypeInformation[11];
		types[0] = Types.LONG;
		types[1] = Types.LONG;
		types[2] = Types.LONG;
		types[3] = Types.BOOLEAN;
		types[4] = Types.LONG;
		types[5] = Types.LONG;
		types[6] = Types.DOUBLE;
		types[7] = Types.DOUBLE;
		types[8] = Types.DOUBLE;
		types[9] = Types.DOUBLE;
		types[10] = Types.SHORT;
		/*DataStream<Tuple> tuples = lines
				.map(new CSVParser(9, types, "|", false, 1000))
				.name("parsing");*/

		DataStream<Tuple2<Tuple, Long>> enrichedTuples = messageStream
				.map(value -> new Tuple2<>(value, System.currentTimeMillis()))
				.returns(Types.TUPLE(Types.TUPLE(types), Types.LONG)) //needed, bc in the lambda function type info gts lost
				.name("Enrich with timestamp");

		int[] keys = new int[3];
		keys[0] = 0;
		keys[1] = 1;
		keys[2] = 5;
		DataStream<Tuple> generalizedTransactions = enrichedTuples
			.process(new Generalizer(10,2500, 10, 15, keys, types))
			.name("Generalizer");

		/*generalizedTransactions
				.addSink(new FinalSink())
				.name("Checking Uniqueness");*/


		env.execute("Transactions Generalization");
	}
}