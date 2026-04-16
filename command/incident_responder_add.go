package main

import (
	"fmt"
	"strings"

	"github.com/PagerDuty/go-pagerduty"
	"github.com/mitchellh/cli"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

type IncidentResponderAdd struct {
	Meta
}

func IncidentResponderAddCommand() (cli.Command, error) {
	return &IncidentResponderAdd{}, nil
}

func (c *IncidentResponderAdd) Help() string {
	helpText := `
	pd incident responder add  Add responders to an incident

	Options:

		-incident-id   Incident ID (required)
		-user-id       User ID to page as responder (required, repeatable)
		-message       Message to include with the request
		-requester-id  Requester user ID (required)
		-from          Email of the requester (required)
	` + c.Meta.Help()
	return strings.TrimSpace(helpText)
}

func (c *IncidentResponderAdd) Synopsis() string {
	return "Add responders to an incident"
}

func (c *IncidentResponderAdd) Run(args []string) int {
	var incidentID string
	var requesterID string
	var from string
	var message string
	var userIDs []string

	flags := c.Meta.FlagSet("incident responder add")
	flags.Usage = func() { fmt.Println(c.Help()) }
	flags.StringVar(&incidentID, "incident-id", "", "Incident ID")
	flags.StringVar(&requesterID, "requester-id", "", "Requester user ID")
	flags.StringVar(&from, "from", "", "Email of the requester")
	flags.StringVar(&message, "message", "Please help with this incident", "Message to include")
	flags.Var((*ArrayFlags)(&userIDs), "user-id", "User ID to add as responder (repeatable)")

	if err := flags.Parse(args); err != nil {
		log.Error(err)
		return -1
	}
	if err := c.Meta.Setup(); err != nil {
		log.Error(err)
		return -1
	}
	if incidentID == "" || requesterID == "" || from == "" || len(userIDs) == 0 {
		log.Error("Please specify -incident-id, -requester-id, -from, and at least one -user-id")
		return -1
	}

	client := c.Meta.Client()

	targets := make([]pagerduty.ResponderRequestTargetWrapper, len(userIDs))
	for i, uid := range userIDs {
		targets[i] = pagerduty.ResponderRequestTargetWrapper{
			Target: pagerduty.ResponderRequestTarget{
				APIObject: pagerduty.APIObject{
					ID:   uid,
					Type: "user_reference",
				},
			},
		}
	}

	opts := pagerduty.ResponderRequestOptions{
		From:        from,
		Message:     message,
		RequesterID: requesterID,
		Targets:     targets,
	}

	resp, err := client.ResponderRequest(incidentID, opts)
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
