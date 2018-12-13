/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gobolt

import (
	"time"
)

type channelConnection struct {
	delegate *neo4jConnection
	work     workItem
	workers  *workerPool
}

func newChannelConnection(conn *neo4jConnector, mode AccessMode) (*channelConnection, error) {
	var startTime time.Time
	var delegate *neo4jConnection
	var err error
	var newConnection = &channelConnection{
		delegate: nil,
		work:     nil,
		workers:  conn.workers,
	}

	for {
		if startTime.IsZero() {
			startTime = time.Now()
		} else if time.Since(startTime) > conn.config.ConnAcquisitionTimeout {
			return nil, conn.valueSystem.genericErrorFactory("connection acquisition timed out")

		}

		newConnection.queueSendJob(func() {
			if delegate, err = newDirectConnection(conn, mode); err == nil {
				newConnection.delegate = delegate
			}
		})

		if err != nil {
			if cErr, ok := err.(ConnectorError); ok && cErr.Code() == 0x600 {
				select {
				case <-conn.freeChan:
					continue
				case <-time.After(conn.config.ConnAcquisitionTimeout):
					return nil, conn.valueSystem.genericErrorFactory("connection acquisition timed out")
				}
			}

			return nil, err
		}

		break;
	}

	return newConnection, nil
}

func (c *channelConnection) Close() error {
	var err error
	c.queueRecvJob(func() {
		err = c.delegate.Close()
	})

	select {
	case c.delegate.connector.freeChan <- true:
	default:
	}

	return err
}

func queueJob(c *channelConnection, work workItem, done chan bool) {
	c.workers.queueWorkItem(func() {
		work()
		done <- true
	})
}

func (c *channelConnection) queueSendJob(work workItem) {
	var done = make(chan bool, 1)
	queueJob(c, work, done)
	<-done
}

func (c *channelConnection) queueRecvJob(work workItem) {
	var done = make(chan bool, 1)
	queueJob(c, work, done)
	<-done
}

func (c *channelConnection) Id() string {
	var id = ""
	c.queueRecvJob(func() {
		id = c.delegate.Id()
	})
	return id
}

func (c *channelConnection) RemoteAddress() string {
	var remoteAddress = ""
	c.queueRecvJob(func() {
		remoteAddress = c.delegate.RemoteAddress()
	})
	return remoteAddress
}

func (c *channelConnection) Server() string {
	var server = ""
	c.queueRecvJob(func() {
		server = c.delegate.Server()
	})
	return server
}

func (c *channelConnection) Begin(bookmarks []string, txTimeout time.Duration, txMetadata map[string]interface{}) (RequestHandle, error) {
	var handle RequestHandle
	var err error
	c.queueSendJob(func() {
		handle, err = c.delegate.Begin(bookmarks, txTimeout, txMetadata)
	})
	return handle, err
}

func (c *channelConnection) Commit() (RequestHandle, error) {
	var handle RequestHandle
	var err error
	c.queueSendJob(func() {
		handle, err = c.delegate.Commit()
	})
	return handle, err
}

func (c *channelConnection) Rollback() (RequestHandle, error) {
	var handle RequestHandle
	var err error
	c.queueSendJob(func() {
		handle, err = c.delegate.Rollback()
	})
	return handle, err
}

func (c *channelConnection) Run(cypher string, args map[string]interface{}, bookmarks []string, txTimeout time.Duration, txMetadata map[string]interface{}) (RequestHandle, error) {
	var handle RequestHandle
	var err error
	c.queueSendJob(func() {
		handle, err = c.delegate.Run(cypher, args, bookmarks, txTimeout, txMetadata)
	})
	return handle, err
}

func (c *channelConnection) PullAll() (RequestHandle, error) {
	var handle RequestHandle
	var err error
	c.queueSendJob(func() {
		handle, err = c.delegate.PullAll()
	})
	return handle, err
}

func (c *channelConnection) DiscardAll() (RequestHandle, error) {
	var handle RequestHandle
	var err error
	c.queueSendJob(func() {
		handle, err = c.delegate.DiscardAll()
	})
	return handle, err
}

func (c *channelConnection) Reset() (RequestHandle, error) {
	var handle RequestHandle
	var err error
	c.queueSendJob(func() {
		handle, err = c.delegate.Reset()
	})
	return handle, err
}

func (c *channelConnection) Flush() error {
	var err error
	c.queueSendJob(func() {
		err = c.delegate.Flush()
	})
	return err
}

func (c *channelConnection) Fetch(request RequestHandle) (FetchType, error) {
	var fetched FetchType
	var err error
	c.queueRecvJob(func() {
		fetched, err = c.delegate.Fetch(request)
	})
	return fetched, err
}

func (c *channelConnection) FetchSummary(request RequestHandle) (int, error) {
	var records int
	var err error
	c.queueRecvJob(func() {
		records, err = c.delegate.FetchSummary(request)
	})
	return records, err
}

func (c *channelConnection) LastBookmark() string {
	var bookmark = ""
	c.queueRecvJob(func() {
		bookmark = c.delegate.LastBookmark()
	})
	return bookmark
}

func (c *channelConnection) Fields() ([]string, error) {
	var fields []string
	var err error
	c.queueRecvJob(func() {
		fields, err = c.delegate.Fields()
	})
	return fields, err
}

func (c *channelConnection) Metadata() (map[string]interface{}, error) {
	var metadata map[string]interface{}
	var err error
	c.queueRecvJob(func() {
		metadata, err = c.delegate.Metadata()
	})
	return metadata, err
}

func (c *channelConnection) Data() ([]interface{}, error) {
	var data []interface{}
	var err error
	c.queueRecvJob(func() {
		data, err = c.delegate.Data()
	})
	return data, err
}
