package utils

import "net"

func GetIP() (string, error) {
	ip := ""
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ip4 := ipnet.IP.To4(); ip4 != nil {
				ip = ip4.String()
				if ip[0:3] == "192" {
					return ip, nil
				}
			}
		}
	}
	return "", nil
}
