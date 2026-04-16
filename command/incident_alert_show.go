package main

import (
	"fmt"
	"strings"

	"github.com/mitchellh/cli"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

type IncidentAlertShow struct {
	Meta
}

func IncidentAlertShowCommand() (cli.Command, error) {
	return &IncidentAlertShow{}, nil
}

func (c *IncidentAlertShow) Help() string {
	helpText := `
	pd incident alert show Get a specific alert for an incident

	Options:

		-incident-id  Incident ID (required)
		-alert-id     Alert ID (required)
	` + c.Meta.Help()
	return strings.TrimSpace(helpText)
}

func (c *IncidentAlertShow) Synopsis() string {
	return "Get a specific alert for the specified incident"
}

func (c *IncidentAlertShow) Run(args []string) int {
	var incidentID string
	var alertID string

	flags := c.Meta.FlagSet("incident alert show")
	flags.Usage = func() { fmt.Println(c.Help()) }
	flags.StringVar(&incidentID, "incident-id", "", "Incident ID")
	flags.StringVar(&alertID, "alert-id", "", "Alert ID")

	if err := flags.Parse(args); err != nil {
		log.Error(err)
		return -1
	}
	if err := c.Meta.Setup(); err != nil {
		log.Error(err)
		return -1
	}
	if incidentID == "" || alertID == "" {
		log.Error("Please specify both -incident-id and -alert-id")
		return -1
	}

	client := c.Meta.Client()

	resp, err := client.GetIncidentAlert(incidentID, alertID)
	if err != nil {
		log.Error(err)
		return -1
	}

	data, err := yaml.Marshal(resp)
	if err != nil {
		log.Error(err)
		return -1
	}
	fmt.Println(string(data))
	return 0
}
