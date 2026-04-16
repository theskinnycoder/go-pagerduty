package main

import (
	"fmt"
	"strings"

	"github.com/mitchellh/cli"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

type IncidentChangeEvents struct {
	Meta
}

func IncidentChangeEventsCommand() (cli.Command, error) {
	return &IncidentChangeEvents{}, nil
}

func (c *IncidentChangeEvents) Help() string {
	helpText := `
	pd incident change-events <ID> List change events related to the specified incident

	` + c.Meta.Help()
	return strings.TrimSpace(helpText)
}

func (c *IncidentChangeEvents) Synopsis() string {
	return "List change events related to the specified incident"
}

func (c *IncidentChangeEvents) Run(args []string) int {
	flags := c.Meta.FlagSet("incident change-events")
	flags.Usage = func() { fmt.Println(c.Help()) }

	if err := flags.Parse(args); err != nil {
		log.Error(err)
		return -1
	}
	if err := c.Meta.Setup(); err != nil {
		log.Error(err)
		return -1
	}
	if len(flags.Args()) != 1 {
		log.Error("Please specify an incident ID")
		return -1
	}

	client := c.Meta.Client()
	id := flags.Arg(0)

	resp, err := client.ListIncidentRelatedChangeEvents(id)
	if err != nil {
		log.Error(err)
		return -1
	}

	for i, ce := range resp.ChangeEvents {
		fmt.Println("Entry: ", i+1)
		data, err := yaml.Marshal(ce)
		if err != nil {
			log.Error(err)
			return -1
		}
		fmt.Println(string(data))
	}
	return 0
}
