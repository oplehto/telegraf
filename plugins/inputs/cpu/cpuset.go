package cpu

import (
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"

	"github.com/grafana/grafana/pkg/cmd/grafana-cli/logger"
)

func parseCpuset(cpu string) (map[int]bool, error) {
	cpus := map[int]bool{}
	chunks := strings.Split(cpu, ",")
	for _, chunk := range chunks {
		if strings.Contains(chunk, "-") {
			// Range
			fields := strings.SplitN(chunk, "-", 2)
			if len(fields) != 2 {
				return nil, fmt.Errorf("Invalid cpuset value: %s", cpu)
			}

			low, err := strconv.Atoi(fields[0])
			if err != nil {
				return nil, fmt.Errorf("Invalid cpuset value: %s", cpu)
			}

			high, err := strconv.Atoi(fields[1])
			if err != nil {
				return nil, fmt.Errorf("Invalid cpuset value: %s", cpu)
			}

			for i := low; i <= high; i++ {
				cpus[i] = true
			}
		} else {
			// Simple entry
			nr, err := strconv.Atoi(chunk)
			if err != nil {
				return nil, fmt.Errorf("Invalid cpuset value: %s", cpu)
			}
			cpus[nr] = true
		}
	}
	return cpus, nil
}

func isolCpus() map[int]bool {
	isolCpuInt := map[int]bool{}
	buf, err := ioutil.ReadFile("/sys/devices/system/cpu/isolated")
	if err != nil {
		fmt.Errorf("Failed to read /sys/devices/system/cpu/isolated")
		return isolCpuInt
	}
	isolCpu := strings.TrimSpace(string(buf))
	if isolCpu != "" {
		isolCpuInt, err = parseCpuset(isolCpu)
		if err != nil {
			logger.Errorf("Error parsing isolated CPU set: %s", string(isolCpu))
			return isolCpuInt
		}
	}
	return isolCpuInt
}