package main

import (
	"errors"
	"fmt"
	"log"
	"mass-rpc/pool"
	"net"
	"net/rpc"
)

type Args struct {
	A, B int
}

type Quotient struct {
	Quo, Rem int
}

type Arith int

func (t *Arith) Multiply(args *Args, reply *int) error {
	*reply = args.A * args.B
	return nil
}

func (t *Arith) Divide(args *Args, quo *Quotient) error {
	if args.B == 0 {
		return errors.New("divide by zero")
	}
	fmt.Print("call")
	quo.Quo = args.A / args.B
	quo.Rem = args.A % args.B
	return nil
}

func connectionPoolTest(total int, repeat int) {
	fmt.Print("Create pool")
	var pools = make([]*pool.RpcConnPool, 0)
	for i := 0; i < total; i++ {
		pools = append(pools, pool.NewConnectionPool("localhost:1234", 5, 5, 30, false, false))
	}
	fmt.Print("start sending")
	for j := 0; j < repeat; j++ {
		for i := 0; i < total; i++ {
			pool := pools[i]

			client, _ := pool.Get()
			args := &Args{7, 8}
			quotient := new(Quotient)
			divCall := client.Conn.Go("Arith.Divide", args, quotient, nil)
			pool.Put(client)
			fmt.Printf("%v sent", i)
			<-divCall.Done
			fmt.Printf("Arith: %d*%d=%v", args.A, args.B, quotient.Quo)
		}
	}

}

func individualAsync(total int, repeat int) {
	log.Print("Creating clients")
	var clients = make([]*rpc.Client, 0)
	log.Print("Create client into array")
	for i := 0; i < total; i++ {
		client, err := rpc.Dial("tcp", "localhost"+":1234")
		if err != nil {
			log.Fatal("dialing:", err)
		}
		clients = append(clients, client)
	}
	log.Print("Start sending request")
	for j := 0; j < repeat; j++ {
		for i := 0; i < total; i++ {
			args := &Args{7, 8}
			quotient := new(Quotient)
			divCall := clients[i].Go("Arith.Divide", args, quotient, nil)
			<-divCall.Done
			fmt.Printf("[%v] Arith: %d*%d=%d", i, args.A, args.B, quotient.Quo)
		}
	}

}

func individual(total int, repeat int) {
	log.Print("Creating clients")
	var clients = make([]*rpc.Client, 0)
	for i := 0; i < total; i++ {
		client, err := rpc.Dial("tcp", "localhost"+":1234")
		if err != nil {
			log.Fatal("dialing:", err)
		}
		clients = append(clients, client)
		log.Printf("%v", len(clients))
	}
	for j := 0; j < repeat; j++ {
		for i := 0; i < total; i++ {
			args := &Args{7, 8}
			quotient := new(Quotient)
			err := clients[i].Call("Arith.Divide", args, &quotient)
			if err != nil {
				log.Fatal("arith error:", err)
			}
			fmt.Printf("[%v] Arith: %d*%d=%d", i, args.A, args.B, quotient.Quo)
		}
	}

}

func main() {
	total := 200
	repeat := 10
	fmt.Print("Hello")

	handler := rpc.NewServer()
	arith := new(Arith)
	handler.Register(arith)
	//rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	log.Print("Create Server")
	go func() {
		for {
			cxn, err := l.Accept()
			if err != nil {
				log.Print("Error Accept Request: %s\n", err)
				return
			}
			go handler.ServeConn(cxn)
		}
	}()
	//individual(total, repeat)
	//individualAsync(total, repeat)
	connectionPoolTest(total, repeat)
	fmt.Println("finish")
	fmt.Scanln()
}
