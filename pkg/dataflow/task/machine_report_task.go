package task

import (
	"time"

	"github.com/dellinger2023/net-flux/gen"
	"github.com/dellinger2023/net-flux/pkg/dataflow/core"
	"github.com/dellinger2023/net-flux/pkg/dataflow/gather"
	"github.com/dellinger2023/net-flux/pkg/logger"
)

const machineTaskName = "machine"

type diskSample struct {
	readBytes  uint64
	writeBytes uint64
	readTime   uint64
	writeTime  uint64
	timestamp  int64
}

type netSample struct {
	bytesRecv uint64
	bytesSent uint64
	timestamp int64
}

type machineTask struct {
	nodeInfo *core.NodeInfo
	interval time.Duration
	next     time.Time
	channel  core.DataflowChannel
	prevDisk *diskSample
	prevNet  *netSample
}

// Next implements Task.
func (t *machineTask) Next() time.Time {
	return t.next
}

func NewMachineTask(nodeInfo *core.NodeInfo, interval time.Duration, channel core.DataflowChannel) Task {
	return &machineTask{nodeInfo: nodeInfo, interval: interval, channel: channel}
}

func (t *machineTask) ID() string {
	return machineTaskName
}

func (t *machineTask) Run() error {

	defer func() {
		t.next = time.Now().Add(t.interval)
	}()

	cpuInfo, err := gather.CollectCPUInfo()
	if err != nil {
		return err
	}

	memInfo, err := gather.CollectMemInfo()
	if err != nil {
		return err
	}

	diskInfo, err := gather.CollectDiskInfo()
	if err != nil {
		return err
	}

	netInfo, err := gather.CollectNetInfo()
	if err != nil {
		return err
	}

	now := time.Now().Unix()
	readBytes, writeBytes, readTime, writeTime := diskInterval(t.prevDisk, diskInfo, now)
	netIn, netOut := netInterval(t.prevNet, netInfo, now)
	t.prevDisk = &diskSample{
		readBytes:  diskInfo.ReadBytes,
		writeBytes: diskInfo.WriteBytes,
		readTime:   diskInfo.ReadTime,
		writeTime:  diskInfo.WriteTime,
		timestamp:  now,
	}
	t.prevNet = &netSample{
		bytesRecv: netInfo.BytesRecv,
		bytesSent: netInfo.BytesSent,
		timestamp: now,
	}

	metric := &gen.MachineMetric{
		CpuUsage:       cpuInfo.Usage,
		CpuCount:       int32(cpuInfo.Count),
		MemUsed:        bytesToMB(memInfo.Used),
		MemTotal:       bytesToMB(memInfo.Total),
		DiskReadBytes:  readBytes,
		DiskWriteBytes: writeBytes,
		DiskReadTime:   readTime,
		DiskWriteTime:  writeTime,
		DiskReadCount:  int64(diskInfo.ReadCount),
		DiskWriteCount: int64(diskInfo.WriteCount),
		NetworkIn:      netIn,
		NetworkOut:     netOut,
		Timestamp:      now,
	}

	if t.nodeInfo != nil {
		metric.MachineId = t.nodeInfo.ID
		metric.InstanceId = t.nodeInfo.ID
		metric.InstanceName = t.nodeInfo.Name
		metric.NodeId = t.nodeInfo.ID
		metric.NodeName = t.nodeInfo.Name
		metric.NodeStatus = t.nodeInfo.Status
		metric.Status = t.nodeInfo.Status
		metric.Version = t.nodeInfo.Version
		metric.StartTime = t.nodeInfo.StartTime
	}

	if err := t.channel.SendMachineInfo(metric); err != nil {
		logger.Errorf("send machine info failed: %v", err)
		return err
	}

	logger.Debugf("send machine info: %+v", metric)
	return nil
}

func bytesToMB(v uint64) float64 {
	return float64(v) / 1024 / 1024
}

// diskInterval 用相邻两次采集计算上报值。字节换算成 KB/s，耗时由毫秒差值换成秒。
// 没有上一次样本、时间没有前进、或计数回绕时上报 0。
func diskInterval(prev *diskSample, cur *gather.DiskInfo, ts int64) (readBytes, writeBytes, readTime, writeTime float64) {
	if prev == nil || cur == nil || ts <= prev.timestamp {
		return 0, 0, 0, 0
	}
	dt := float64(ts - prev.timestamp)
	return kbPerSec(cur.ReadBytes, prev.readBytes, dt),
		kbPerSec(cur.WriteBytes, prev.writeBytes, dt),
		elapsedSec(cur.ReadTime, prev.readTime),
		elapsedSec(cur.WriteTime, prev.writeTime)
}

// netInterval 用相邻两次累计收发字节计算 KB/s。没有上一次样本、时间没有前进、或计数回绕时上报 0。
func netInterval(prev *netSample, cur *gather.NetInfo, ts int64) (in, out float64) {
	if prev == nil || cur == nil || ts <= prev.timestamp {
		return 0, 0
	}
	dt := float64(ts - prev.timestamp)
	return kbPerSec(cur.BytesRecv, prev.bytesRecv, dt),
		kbPerSec(cur.BytesSent, prev.bytesSent, dt)
}

func kbPerSec(current, previous uint64, deltaSec float64) float64 {
	if deltaSec <= 0 || current < previous {
		return 0
	}
	return float64(current-previous) / 1024 / deltaSec
}

func elapsedSec(current, previous uint64) float64 {
	if current < previous {
		return 0
	}
	return float64(current-previous) / 1000
}
