package gather

import (
	"github.com/dellinger2023/net-flux/pkg/logger"
	"github.com/shirou/gopsutil/v4/net"
)

func CollectNetInfo() (*NetInfo, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		logger.Errorf("get network interfaces failed: %v", err)
		return nil, err
	}
	loopback := map[string]struct{}{}
	for _, iface := range ifaces {
		if isLoopback(iface.Flags) {
			loopback[iface.Name] = struct{}{}
		}
	}

	stats, err := net.IOCounters(true)
	if err != nil {
		logger.Errorf("get network io counters failed: %v", err)
		return nil, err
	}

	info := &NetInfo{}
	for _, stat := range stats {
		if _, skip := loopback[stat.Name]; skip {
			continue
		}
		info.BytesRecv += stat.BytesRecv
		info.BytesSent += stat.BytesSent
	}
	return info, nil
}

func isLoopback(flags []string) bool {
	for _, flag := range flags {
		if flag == "loopback" {
			return true
		}
	}
	return false
}
