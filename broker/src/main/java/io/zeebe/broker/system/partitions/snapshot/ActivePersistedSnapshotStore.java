/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.system.partitions.snapshot;

import io.atomix.raft.snapshot.PersistedSnapshotStore;
import io.atomix.raft.snapshot.TransientSnapshot;
import java.util.Optional;

/** A persisted snapshot store than can create a new snapshot and persists it. */
public interface ActivePersistedSnapshotStore extends PersistedSnapshotStore {
  Optional<TransientSnapshot> newTransientSnapshot(
      long index, long term, long processedPosition, long exportedPosition);
}
