package stream

import "net"

func getLocalIP() string {
	interfaces, err := net.Interfaces()
	if err != nil {
		return ""
	}

	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		if ip := localIPFromAddrs(addrs); ip != "" {
			return ip
		}
	}

	return ""
}

func localIPFromAddrs(addrs []net.Addr) string {
	for _, addr := range addrs {
		var ip net.IP

		switch v := addr.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		}

		if ip == nil || ip.IsLoopback() {
			continue
		}

		ipv4 := ip.To4()
		if ipv4 == nil {
			continue
		}

		return ipv4.String()
	}

	return ""
}
