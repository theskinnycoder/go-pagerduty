package main

import (
	"fmt"
	"strings"

	"github.com/mitchellh/cli"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

type IncidentAlertList struct {
	Meta
}

func IncidentAlertListCommand() (cli.Command, error) {
	return &IncidentAlertList{}, nil
}

func (c *IncidentAlertList) Help() string {
	helpText := `
	pd incident alert list <INCIDENT_ID> List alerts for the specified incident

	` + c.Meta.Help()
	return strings.TrimSpace(helpText)
}

func (c *IncidentAlertList) Synopsis() string {
	return "List alerts for the specified incident"
}

func (c *IncidentAlertList) Run(args []string) int {
	flags := c.Meta.FlagSet("incident alert list")
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

	resp, err := client.ListIncidentAlerts(id)
	if err != nil {
		log.Error(err)
		return -1
	}

	for i, alert := range resp.Alerts {
		fmt.Println("Entry: ", i+1)
		data, err := yaml.Marshal(alert)
		if err != nil {
			log.Error(err)
			return -1
		}
		fmt.Println(string(data))
	}
	return 0
}
