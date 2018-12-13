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

import "sync"

type workItem func()

type workerPool struct {
	waiter *sync.WaitGroup
	done   chan bool
	queue  chan workItem
}

func newWorkerPool(workerCount int) *workerPool {
	var done = make(chan bool)
	var queue = make(chan workItem, 1000)
	var waiter = &sync.WaitGroup{}

	for i := 0; i < workerCount; i++ {
		waiter.Add(1)

		go func(queue chan workItem, done chan bool) {
			defer waiter.Done()

			for {
				select {
				case job := <-queue:
					if job != nil {
						job()
					}
				case <-done:
					return
				}
			}
		}(queue, done)
	}

	return &workerPool{
		waiter: waiter,
		done:   done,
		queue:  queue,
	}
}

func (pool *workerPool) queueWorkItem(item workItem) {
	pool.queue <- item
}

func (pool *workerPool) stop() {
	close(pool.done)
	pool.waiter.Wait()
}
