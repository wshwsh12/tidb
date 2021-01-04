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

package cgroup

import (
	"os"
	"path"
	"testing"

	. "github.com/pingcap/check"
)

var _ = Suite(&testCGroupSuite{})

func TestT(t *testing.T) {
	TestingT(t)
}

type testCGroupSuite struct {
}

func (s *testCGroupSuite) SetUpSuite(c *C) {
}

func (s *testCGroupSuite) TearDownSuite(c *C) {
}

func (s *testCGroupSuite) TestParseMountPointFromLine(c *C) {
	testCases := []string{
		"1 0 252:0 / / rw,noatime - ext4 /dev/dm-0 rw,errors=remount-ro,data=ordered",
		"31 23 0:24 /docker /sys/fs/cgroup/cpu rw,nosuid,nodev,noexec,relatime shared:1 - cgroup cgroup rw,cpu",
	}

	expectedMps := []mountPoint{
		{
			mountID:        1,
			parentID:       0,
			deviceID:       "252:0",
			root:           "/",
			mountPoint:     "/",
			option:         []string{"rw", "noatime"},
			optionalFields: []string{},
			fsType:         "ext4",
			mountSource:    "/dev/dm-0",
			superOptions:   []string{"rw", "errors=remount-ro", "data=ordered"},
		},
		{
			mountID:        31,
			parentID:       23,
			deviceID:       "0:24",
			root:           "/docker",
			mountPoint:     "/sys/fs/cgroup/cpu",
			option:         []string{"rw", "nosuid", "nodev", "noexec", "relatime"},
			optionalFields: []string{"shared:1"},
			fsType:         "cgroup",
			mountSource:    "cgroup",
			superOptions:   []string{"rw", "cpu"},
		},
	}

	for i := range testCases {
		input := testCases[i]
		expected := expectedMps[i]
		output, err := parseMountPointFromString(input)
		c.Assert(err, IsNil)
		c.Assert(expected, DeepEquals, *output)
	}
}

func (s *testCGroupSuite) TestMountPointTranslate(c *C) {
	mp := mountPoint{
		mountID:        31,
		parentID:       23,
		deviceID:       "0:24",
		root:           "/docker",
		mountPoint:     "/sys/fs/cgroup/cpu",
		option:         []string{"rw", "nosuid", "nodev", "noexec", "relatime"},
		optionalFields: []string{"shared:1"},
		fsType:         "cgroup",
		mountSource:    "cgroup",
		superOptions:   []string{"rw", "cpu"},
	}
	translated, err := mp.translate("/docker")
	c.Assert(err, IsNil)
	c.Assert(translated, Equals, "/sys/fs/cgroup/cpu")
}

func (s testCGroupSuite) TestCGroupSys(c *C) {
	tmpDir := os.TempDir()
	path1 := path.Join(tmpDir, "cgroup")
	f1, err := os.OpenFile(path1, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	c.Assert(err, IsNil)
	defer func() {
		os.Remove(path1)
	}()
	path2 := path.Join(tmpDir, "mountinfo")
	f2, err := os.OpenFile(path2, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	c.Assert(err, IsNil)
	defer func() {
		os.Remove(path2)
	}()
	f1.WriteString("4:memory:/kubepods/burstable/poda2ebe2cd-64c7-11ea-8799-eeeeeeeeeeee/a026c487f1168b7f5442444ac8e35161dfcde87c175ef27d9a806270e267a575\n")
	f1.WriteString("5:cpuacct,cpu:/kubepods/burstable/poda2ebe2cd-64c7-11ea-8799-eeeeeeeeeeee/a026c487f1168b7f5442444ac8e35161dfcde87c175ef27d9a806270e267a575\n")
	f1.Close()

	f2.WriteString("5871 5867 0:26 /kubepods/burstable/poda2ebe2cd-64c7-11ea-8799-eeeeeeeeeeee/a026c487f1168b7f5442444ac8e35161dfcde87c175ef27d9a806270e267a575 /sys/fs/cgroup/memory ro,nosuid,nodev,noexec,relatime master:12 - cgroup cgroup rw,memory\n")
	f2.WriteString("5872 5867 0:27 /kubepods/burstable/poda2ebe2cd-64c7-11ea-8799-eeeeeeeeeeee/a026c487f1168b7f5442444ac8e35161dfcde87c175ef27d9a806270e267a575 /sys/fs/cgroup/cpu,cpuacct ro,nosuid,nodev,noexec,relatime master:13 - cgroup cgroup rw,cpuacct,cpu\n")
	f2.Close()

	cgroup := NewCGroupSys(path1, path2)

	cases := []struct {
		key string
		val *cGroup
	}{
		{"memory", NewCGroup("/sys/fs/cgroup/memory")},
		{"cpu", NewCGroup("/sys/fs/cgroup/cpu,cpuacct")},
		{"cpuacct", NewCGroup("/sys/fs/cgroup/cpu,cpuacct")},
	}

	for _, testCase := range cases {
		val, ok := cgroup.cGroups[testCase.key]
		c.Assert(ok, IsTrue)
		c.Assert(val, DeepEquals, testCase.val)
	}
}
