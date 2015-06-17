/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package samza.examples.footrace;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

import java.sql.Time;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RaceBibAggregatorStreamTask implements StreamTask {

    private static SystemStream OUTPUT_STREAM = new SystemStream("kafka", "race-bib-aggregates");

    private Set<Integer> bibs = new HashSet<Integer>(); // Set ensures duplicates DNE
    private Map<Integer, Set<BibMetadata>> metaForBibs = new HashMap<Integer, Set<BibMetadata>>();

    @SuppressWarnings("unchecked")
    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        Map<String, Object> jsonRead = (Map<String, Object>) envelope.getMessage();

        Integer bib = (Integer) jsonRead.get("bib"),
                checkpoint = (Integer) jsonRead.get("checkpoint");
        Time time = (Time) jsonRead.get("time");
        if (bibs.add(bib)) {
            metaForBibs.put(bib, new HashSet<BibMetadata>());
        }

        metaForBibs.get(bib).add(new BibMetadata(checkpoint, time));

        collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM,
                new CheckpointCounterEnvelope(bib, metaForBibs.get(bib))));
    }
}
