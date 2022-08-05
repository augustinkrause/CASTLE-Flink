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

package spendreport;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;


/**
 * Skeleton code for the datastream walkthrough
 */
public class TransactionGeneralizationJob {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Transaction> transactions = env
			.addSource(new TransactionSource())
			.name("transactions");

		//map to (Tuple(Transaction), Timestamp)
		DataStream<Tuple2<Tuple, Long>> mappedTransactions = transactions
				.map(value -> new Tuple2<Tuple, Long>(new Tuple3<>(value.getAmount(), value.getTimestamp(), value.getAccountId()), System.currentTimeMillis()))
				.returns(Types.TUPLE(Types.TUPLE(Types.DOUBLE, Types.LONG, Types.LONG), Types.LONG)) //needed, bc in the lambda function type info gts lost
				.name("Mapping");

		int[] keys = new int[1];
		keys[0] = 0;
		DataStream<Tuple> generalizedTransactions = mappedTransactions
			.process(new Generalizer(10,2500, 0.3, keys))
				.returns(Types.TUPLE())
			.name("Generalizer");

		/*alerts
			.addSink(new AlertSink())
			.name("send-alerts");*/


		env.execute("Transactions Generalization");
	}
}