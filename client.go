package client

import (
	"log"
	"net"
)

func main() {
	client, err := net.DialTimeout("tcp", "localhost:3000")
	if err != nil {
		log.Fatal(err)
	}
	data := "POST /post HTTP/1.1\r\nHost: localhost:3000\r\nContent-Length: 4096\r\n\r\n"
	data += "f" * 4096
	_, err := client.Write([]byte(data))
	if err != nil {
		log.Println(err)
	}
	buff := make([]byte, 4096)
	n, err := client.Read(buff)
	if err != nil {
		log.Println(err)
	}
	log.Println(n, "bytes received")

}
