package log

import (
	"github.com/golang/protobuf/proto"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	api "github.com/neepoo/proglog/api/v1"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, log *Log){
		"append and read a record succeeds": testAppendRead,
		"offset out of range error":         testOutRangeErr,
		"init with existing segments":       testInitExising,
		"reader":                            testReader,
		"truncate":                          testTruncate,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "store test")
			require.NoError(t, err)

			defer os.RemoveAll(dir)
			c := Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, c)
			require.NoError(t, err)
			fn(t, log)
		})
	}
}

func testAppendRead(t *testing.T, log *Log) {
	a := &api.Record{Values: []byte("hello world")}
	off, err := log.Append(a)
	require.NoError(t, err)
	// 偏移从0开始
	require.Equal(t, uint64(0), off)
	read, err := log.Read(off)
	require.NoError(t, err)
	require.Equal(t, a.Values, read.Values)
}

func testOutRangeErr(t *testing.T, log *Log) {
	read, err := log.Read(1)
	require.Nil(t, read)
	apiErr := err.(api.ErrOffsetOutOfRange)
	require.Equal(t, uint64(1), apiErr.Offset)
}

func testInitExising(t *testing.T, log *Log) {
	a := &api.Record{
		Values: []byte("Hello world"),
	}
	for i := 0; i < 3; i++ {
		_, err := log.Append(a)
		require.NoError(t, err)
	}
	require.NoError(t, log.Close())

	//re open
	off, err := log.LowerOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)
	off, err = log.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

	o, err := NewLog(log.Dir, log.Config)
	require.NoError(t, err)

	off, err = o.LowerOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	off, err = o.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

}
//testReader(*testing.T, *log.Log)
//tests that we can read the full, raw log as it’s
//stored on disk so that we can snapshot and restore the logs in Finite-
//State Machine.
func testReader(t *testing.T, log *Log) {
	a := &api.Record{
		Values: []byte("hello world"),
	}

	off, err := log.Append(a)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	reader := log.Reader()
	b, err := ioutil.ReadAll(reader)
	require.NoError(t, err)
	read := &api.Record{}
	err = proto.Unmarshal(b[lenWidth:], read)
	require.NoError(t, err)
	require.Equal(t, a.Values, read.Values)
}

func testTruncate(t *testing.T, log *Log)  {
	a := &api.Record{
		Values: []byte("hello world"),
	}

	for i := 0; i < 3; i++ {
		_, err := log.Append(a)
		require.NoError(t, err)
	}
	err := log.Truncate(1)
	require.NoError(t, err)

	_, err = log.Read(0)
	require.Error(t, err)
}