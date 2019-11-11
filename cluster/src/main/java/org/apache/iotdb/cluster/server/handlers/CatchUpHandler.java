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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.cluster.server.handlers;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient.appendEntry_call;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CatchUpHandler implements AsyncMethodCallback<appendEntry_call> {

  private static final Logger logger = LoggerFactory.getLogger(CatchUpHandler.class);

  private Log log;
  private AtomicBoolean aborted;
  private AtomicBoolean appendSucceed;
  private RaftServer raftServer;

  @Override
  public void onComplete(appendEntry_call response) {
    try {
      logger.debug("Received a catch up result of {}", log);
      long resp = response.getResult();
      if (resp == RaftServer.RESPONSE_AGREE) {
        synchronized (aborted) {
          appendSucceed.set(true);
          aborted.notifyAll();
        }
        logger.debug("Succeeded to send log {}", log);
      } else if (resp == RaftServer.RESPONSE_LOG_MISMATCH) {
        // this is not probably possible
        logger.error("Log mismatch occurred when sending log {}", log);
        synchronized (aborted) {
          aborted.set(true);
          aborted.notifyAll();
        }
      } else {
        // the follower'term has updated, which means a new leader is elected
        synchronized (raftServer.getTerm()) {
          long currTerm = raftServer.getTerm().get();
          if (currTerm < resp) {
            logger.debug("Received a rejection because term is stale: {}/{}", currTerm, resp);
            raftServer.setCharacter(NodeCharacter.FOLLOWER);
            raftServer.getTerm().set(currTerm);
          }
        }
        synchronized (aborted) {
          aborted.set(true);
          aborted.notifyAll();
        }
        logger.warn("Catch-up aborted because leadership is lost");
      }
    } catch (TException e) {
      onError(e);
    }
  }

  @Override
  public void onError(Exception exception) {
    synchronized (aborted) {
      aborted.set(true);
      aborted.notifyAll();
    }
    logger.warn("Catchup fails when sending log {}", log, exception);
  }

  public void setLog(Log log) {
    this.log = log;
  }

  public void setAborted(AtomicBoolean aborted) {
    this.aborted = aborted;
  }

  public void setAppendSucceed(AtomicBoolean appendSucceed) {
    this.appendSucceed = appendSucceed;
  }

  public void setRaftServer(RaftServer raftServer) {
    this.raftServer = raftServer;
  }
}
