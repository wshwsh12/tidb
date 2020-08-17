// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package storage_test

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/sys/storage"
)

func TestT(t *testing.T) {
	TestingT(t)
}

func TestGetTargetDirectoryCapacity(t *testing.T) {
	r, err := storage.GetTargetDirectoryCapacity(".")
	if err != nil {
		t.Fatal(err.Error())
	}
	if r < 1 {
		t.Fatalf("couldn't get capacity")
	}
	//TODO: check the value of r with `df` in linux
}
