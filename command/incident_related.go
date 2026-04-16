package main

import (
	"fmt"
	"strings"

	"github.com/mitchellh/cli"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

type IncidentRelated struct {
	Meta
}

func IncidentRelatedCommand() (cli.Command, error) {
	return &IncidentRelated{}, nil
}

func (c *IncidentRelated) Help() string {
	helpText := `
	pd incident related <ID> Get related incidents for the specified incident

	` + c.Meta.Help()
	return strings.TrimSpace(helpText)
}

func (c *IncidentRelated) Synopsis() string {
	return "Get related incidents for the specified incident"
}

func (c *IncidentRelated) Run(args []string) int {
	flags := c.Meta.FlagSet("incident related")
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

	resp, err := client.GetRelatedIncidents(id)
	if err != nil {
		log.Error(err)
		return -1
	}

	for i, ri := range resp.RelatedIncidents {
		fmt.Println("Entry: ", i+1)
		data, err := yaml.Marshal(ri)
		if err != nil {
			log.Error(err)
			return -1
		}
		fmt.Println(string(data))
	}
	return 0
}
