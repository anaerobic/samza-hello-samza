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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RaceCheckpointCheckerStreamTask implements StreamTask {

    private static SystemStream OUTPUT_STREAM = new SystemStream("kafka", "race-checkpoint-checks");

    private Set<Integer> bibs = new HashSet<Integer>(); // Set ensures duplicates DNE
    private Map<Integer, Set<CheckpointCounterEnvelope>> counterEnvelopes = new HashMap<Integer, Set<CheckpointCounterEnvelope>>();
    private Map<String, Object> checks = new HashMap<String, Object>();

    @SuppressWarnings("unchecked")
    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        CheckpointCounterEnvelope counterEnvelope = (CheckpointCounterEnvelope) envelope.getMessage();

        if (bibs.add(counterEnvelope.Bib)) {
            counterEnvelopes.put(counterEnvelope.Bib, new HashSet<CheckpointCounterEnvelope>());
        }

        Set<CheckpointCounterEnvelope> counterEnvelopesForBib = counterEnvelopes.get(counterEnvelope.Bib);

        counterEnvelopesForBib.add(counterEnvelope);

        // iterate over all of the envelopes we have received and
        // validate that they are consistent with previous envelopes
        Set<CheckpointCounterEnvelope> invalidEnvelopes = null;
        for (CheckpointCounterEnvelope prevEnv : counterEnvelopesForBib) {

            BibMetadata lastMeta = null;
            for (BibMetadata meta : prevEnv.BibMetadata) {

                // each successive meta should have:
                // the next checkpoint, and
                // a higher time value
                if (lastMeta != null &&
                        ((meta.Checkpoint - lastMeta.Checkpoint != 1) ||
                                meta.Time.before(lastMeta.Time))) {
                    if (invalidEnvelopes == null) {
                        invalidEnvelopes = new HashSet<CheckpointCounterEnvelope>();
                    }
                    invalidEnvelopes.add(prevEnv);
                }

                lastMeta = meta;
            }
        }

        checks.put("bib", counterEnvelope.Bib);
        checks.put("bib-is-valid", invalidEnvelopes == null ? "Yes" : "No");
        checks.put("checkpoints-passed", counterEnvelopesForBib.size());

        if (invalidEnvelopes != null) {
            checks.put("invalid-checkpoints", invalidEnvelopes);
        }

        collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, checks));
    }
}
