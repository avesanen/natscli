package main

import (
	"fmt"
	"github.com/nats-io/go-nats"
	"github.com/urfave/cli"
	"os"
	"sync"
	"time"
)

var nc *nats.Conn

func main() {
	app := cli.NewApp()

	app.Name = "NatsCLI"
	app.Usage = "Commandline interface for nats."

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "url",
			Value: "nats://localhost:4222",
		},
	}

	app.Commands = []cli.Command{
		{
			Name:    "publish",
			Aliases: []string{"pub", "p"},
			Usage:   "publish",
			Action:  PubAction,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name: "subj",
				},
				cli.StringFlag{
					Name: "data",
				},
			},
		},
		{
			Name:    "subscribe",
			Aliases: []string{"sub", "s"},
			Usage:   "subscribe",
			Action:  SubAction,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name: "subj",
				},
				cli.IntFlag{
					Name:  "num",
					Usage: "Number of messages to receive",
					Value: -1,
				},
			},
		},
		{
			Name:    "request",
			Aliases: []string{"req", "r"},
			Usage:   "request",
			Action:  ReqAction,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name: "subj",
				},
				cli.StringFlag{
					Name: "data",
				},
				cli.Int64Flag{
					Name:  "timeout",
					Value: 1,
				},
			},
		},
	}

	app.Before = func(c *cli.Context) error {
		url := c.String("url")
		conn, err := nats.Connect(url)
		if err != nil {
			return err
		}
		nc = conn
		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		fmt.Printf("%v\n", err.Error())
		os.Exit(1)
	}
}

func PubAction(c *cli.Context) error {
	err := nc.Publish(c.String("subj"), []byte(c.String("data")))
	if err != nil {
		return err
	}
	if err := nc.Flush(); err != nil {
		return err
	}
	return nil
}

func SubAction(c *cli.Context) error {
	n := c.Int("num")
	wg := sync.WaitGroup{}
	if n < 1 {
		wg.Add(1)
	} else {
		wg.Add(n)
	}

	sub, err := nc.Subscribe(c.String("subj"), func(msg *nats.Msg) {
		fmt.Printf("%s\n", msg.Data)
		if n != -1 {
			wg.Done()
		}
	})
	if err != nil {
		fmt.Println("fail")
		return err
	}
	defer sub.Unsubscribe()
	wg.Wait()
	return nil
}

func ReqAction(c *cli.Context) error {
	timeout := time.Second * time.Duration(c.Int64("timeout"))
	msg, err := nc.Request(c.String("subj"), []byte(c.String("data")), timeout)
	if err != nil {
		return err
	}
	fmt.Printf("%s\n", msg.Data)
	return nil
}
