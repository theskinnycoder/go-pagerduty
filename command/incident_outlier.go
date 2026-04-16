package main

import (
	"fmt"
	"strings"

	"github.com/mitchellh/cli"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

type IncidentOutlier struct {
	Meta
}

func IncidentOutlierCommand() (cli.Command, error) {
	return &IncidentOutlier{}, nil
}

func (c *IncidentOutlier) Help() string {
	helpText := `
	pd incident outlier <ID> Get outlier incident information

	` + c.Meta.Help()
	return strings.TrimSpace(helpText)
}

func (c *IncidentOutlier) Synopsis() string {
	return "Get outlier incident information for the specified incident"
}

func (c *IncidentOutlier) Run(args []string) int {
	flags := c.Meta.FlagSet("incident outlier")
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

	resp, err := client.GetOutlierIncident(id)
	if err != nil {
		log.Error(err)
		return -1
	}

	data, err := yaml.Marshal(resp.OutlierIncident)
	if err != nil {
		log.Error(err)
		return -1
	}
	fmt.Println(string(data))
	return 0
}
