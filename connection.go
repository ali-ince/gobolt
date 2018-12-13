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

import "time"

// Connection represents an active seabolt connection
type Connection interface {
	Id() string
	RemoteAddress() string
	Server() string

	Begin(bookmarks []string, txTimeout time.Duration, txMetadata map[string]interface{}) (RequestHandle, error)
	Commit() (RequestHandle, error)
	Rollback() (RequestHandle, error)
	Run(cypher string, args map[string]interface{}, bookmarks []string, txTimeout time.Duration, txMetadata map[string]interface{}) (RequestHandle, error)
	PullAll() (RequestHandle, error)
	DiscardAll() (RequestHandle, error)
	Reset() (RequestHandle, error)
	Flush() error
	Fetch(request RequestHandle) (FetchType, error)  // return type ?
	FetchSummary(request RequestHandle) (int, error) // return type ?

	LastBookmark() string
	Fields() ([]string, error)
	Metadata() (map[string]interface{}, error)
	Data() ([]interface{}, error)

	Close() error
}
