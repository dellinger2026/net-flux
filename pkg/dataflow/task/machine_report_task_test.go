package task

import (
	"math"
	"testing"

	"github.com/dellinger2023/net-flux/gen"
	"github.com/dellinger2023/net-flux/pkg/dataflow/gather"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func nearlyEqual(a, b float64) bool {
	return math.Abs(a-b) < 1e-9
}

func TestMachineMetricFloatFieldsRoundTrip(t *testing.T) {
	in := &gen.MachineMetric{
		MemUsed:        bytesToMB(11626835968),
		MemTotal:       bytesToMB(17179869184),
		DiskReadBytes:  10.5,
		DiskWriteBytes: 4.25,
		DiskReadTime:   0.12,
		DiskWriteTime:  0.045,
		NetworkIn:      12.5,
		NetworkOut:     3.5,
	}
	fields := in.ProtoReflect().Descriptor().Fields()
	for _, n := range []protoreflect.FieldNumber{4, 5, 8, 9, 10, 11, 23, 24} {
		f := fields.ByNumber(n)
		if f == nil || f.Kind() != protoreflect.DoubleKind {
			t.Fatalf("field %d is not double", n)
		}
	}
	raw, err := proto.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	out := &gen.MachineMetric{}
	if err := proto.Unmarshal(raw, out); err != nil {
		t.Fatal(err)
	}
	if !nearlyEqual(out.MemUsed, in.MemUsed) || !nearlyEqual(out.MemTotal, 16384) ||
		!nearlyEqual(out.DiskReadBytes, 10.5) || !nearlyEqual(out.DiskWriteBytes, 4.25) ||
		!nearlyEqual(out.DiskReadTime, 0.12) || !nearlyEqual(out.DiskWriteTime, 0.045) ||
		!nearlyEqual(out.NetworkIn, 12.5) || !nearlyEqual(out.NetworkOut, 3.5) {
		t.Fatalf("round trip = %+v", out)
	}
}

func TestBytesToMB(t *testing.T) {
	if got := bytesToMB(11626835968); !nearlyEqual(got, 11626835968.0/1024/1024) {
		t.Fatalf("mem used = %v, want %v", got, 11626835968.0/1024/1024)
	}
	if got := bytesToMB(17179869184); !nearlyEqual(got, 16384) {
		t.Fatalf("mem total = %v, want 16384", got)
	}
}

func TestDiskInterval_FirstSampleIsZero(t *testing.T) {
	cur := &gather.DiskInfo{
		ReadBytes:  10 * 1024 * 1024,
		WriteBytes: 20 * 1024 * 1024,
		ReadTime:   5000,
		WriteTime:  8000,
	}
	readBytes, writeBytes, readTime, writeTime := diskInterval(nil, cur, 100)
	if readBytes != 0 || writeBytes != 0 || readTime != 0 || writeTime != 0 {
		t.Fatalf("first sample = (%v, %v, %v, %v), want zeros", readBytes, writeBytes, readTime, writeTime)
	}
}

func TestDiskInterval_Delta(t *testing.T) {
	prev := &diskSample{
		readBytes:  1000 * 1024,
		writeBytes: 2000 * 1024,
		readTime:   1000,
		writeTime:  2000,
		timestamp:  1_000,
	}
	cur := &gather.DiskInfo{
		ReadBytes:  prev.readBytes + 10*1024*30,
		WriteBytes: prev.writeBytes + 4*1024*30,
		ReadTime:   prev.readTime + 120,
		WriteTime:  prev.writeTime + 45,
	}
	readBytes, writeBytes, readTime, writeTime := diskInterval(prev, cur, prev.timestamp+30)
	if !nearlyEqual(readBytes, 10) {
		t.Fatalf("disk_read_bytes = %v, want 10 KB/s", readBytes)
	}
	if !nearlyEqual(writeBytes, 4) {
		t.Fatalf("disk_write_bytes = %v, want 4 KB/s", writeBytes)
	}
	if !nearlyEqual(readTime, 0.12) {
		t.Fatalf("disk_read_time = %v, want 0.12 s", readTime)
	}
	if !nearlyEqual(writeTime, 0.045) {
		t.Fatalf("disk_write_time = %v, want 0.045 s", writeTime)
	}
}

func TestNetInterval_FirstSampleIsZero(t *testing.T) {
	in, out := netInterval(nil, &gather.NetInfo{BytesRecv: 1024 * 1024, BytesSent: 2048 * 1024}, 100)
	if in != 0 || out != 0 {
		t.Fatalf("first sample = (%v, %v), want zeros", in, out)
	}
}

func TestNetInterval_Delta(t *testing.T) {
	prev := &netSample{
		bytesRecv: 1000 * 1024,
		bytesSent: 2000 * 1024,
		timestamp: 1_000,
	}
	cur := &gather.NetInfo{
		BytesRecv: prev.bytesRecv + 12*1024*30,
		BytesSent: prev.bytesSent + 3*1024*30,
	}
	in, out := netInterval(prev, cur, prev.timestamp+30)
	if !nearlyEqual(in, 12) {
		t.Fatalf("network_in = %v, want 12 KB/s", in)
	}
	if !nearlyEqual(out, 3) {
		t.Fatalf("network_out = %v, want 3 KB/s", out)
	}
}

func TestNetInterval_CounterReset(t *testing.T) {
	prev := &netSample{
		bytesRecv: 1000 * 1024,
		bytesSent: 2000 * 1024,
		timestamp: 1_000,
	}
	in, out := netInterval(prev, &gather.NetInfo{BytesRecv: 10 * 1024, BytesSent: 10 * 1024}, prev.timestamp+30)
	if in != 0 || out != 0 {
		t.Fatalf("reset sample = (%v, %v), want zeros", in, out)
	}
}

func TestDiskInterval_CounterReset(t *testing.T) {
	prev := &diskSample{
		readBytes:  1000 * 1024,
		writeBytes: 2000 * 1024,
		readTime:   1000,
		writeTime:  2000,
		timestamp:  1_000,
	}
	cur := &gather.DiskInfo{
		ReadBytes:  10 * 1024,
		WriteBytes: 10 * 1024,
		ReadTime:   10,
		WriteTime:  10,
	}
	readBytes, writeBytes, readTime, writeTime := diskInterval(prev, cur, prev.timestamp+30)
	if readBytes != 0 || writeBytes != 0 || readTime != 0 || writeTime != 0 {
		t.Fatalf("reset sample = (%v, %v, %v, %v), want zeros", readBytes, writeBytes, readTime, writeTime)
	}
}
