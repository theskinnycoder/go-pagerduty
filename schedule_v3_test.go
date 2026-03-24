package pagerduty

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
)

// testBodyContains decodes the request body JSON into dest and calls t.Fatal on error.
func testBodyContains(t *testing.T, r *http.Request, dest interface{}) {
	t.Helper()
	if err := json.NewDecoder(r.Body).Decode(dest); err != nil {
		t.Fatalf("failed to decode request body: %v", err)
	}
}

var (
	testScheduleV3ID   = "SCHED01"
	testRotationV3ID   = "ROT01"
	testEventV3ID      = "EVT01"
	testCustomShiftID  = "CSH01"
	testOverrideV3ID   = "OVR01"
)

const (
	mockScheduleV3ListResponse = `{
		"schedules": [
			{
				"id": "SCHED01",
				"type": "schedule_reference",
				"summary": "On-Call Schedule",
				"self": "https://api.pagerduty.com/v3/schedules/SCHED01",
				"html_url": "https://example.pagerduty.com/schedules/SCHED01"
			}
		]
	}`

	mockScheduleV3ListEmptyResponse = `{"schedules": []}`

	// No "events" key in rotation — Go decodes Events as nil.
	mockScheduleV3GetResponse = `{
		"schedule": {
			"id": "SCHED01",
			"type": "schedule",
			"name": "On-Call Schedule",
			"time_zone": "UTC",
			"description": "Test schedule",
			"rotations": [
				{"id": "ROT01", "type": "rotation"}
			]
		}
	}`

	// Reproduces the real API response shape for a schedule associated with an
	// escalation policy. Prior to the fix, this caused:
	// "json: cannot unmarshal object into Go struct field
	//  ScheduleV3.schedule.escalation_policies of type string"
	mockScheduleV3GetWithEscalationPoliciesResponse = `{
		"schedule": {
			"id": "SCHED01",
			"type": "schedule",
			"name": "On-Call Schedule",
			"time_zone": "UTC",
			"escalation_policies": [
				{
					"id": "EP01",
					"type": "escalation_policy",
					"summary": "Default EP",
					"self": "https://api.pagerduty.com/escalation_policies/EP01",
					"html_url": "https://app.pagerduty.com/escalation_policies/EP01"
				}
			]
		}
	}`

	mockScheduleV3GetWithFinalResponse = `{
		"schedule": {
			"id": "SCHED01",
			"type": "schedule",
			"name": "On-Call Schedule",
			"time_zone": "UTC",
			"final_schedule": {
				"type": "final_schedule",
				"rendered_coverage_percentage": 100,
				"computed_shift_assignments": [
					{
						"type": "computed_shift_assignment",
						"start_time": "2026-03-01T09:00:00Z",
						"end_time": "2026-03-08T09:00:00Z",
						"member": {"type": "user_member", "user_id": "USER01"},
						"source": {"type": "schedule_rotation", "rotation_id": "ROT01"}
					}
				]
			}
		}
	}`

	// No rotations — used for create / update responses.
	mockScheduleV3MutateResponse = `{
		"schedule": {
			"id": "SCHED01",
			"type": "schedule",
			"name": "On-Call Schedule",
			"time_zone": "UTC",
			"description": "Test schedule",
			"teams": [
				{"id": "TEAM01", "type": "team_reference"}
			]
		}
	}`

	// "events": [] — Go decodes Events as []EventV3{} (non-nil empty slice).
	mockRotationV3Response = `{
		"rotation": {
			"id": "ROT01",
			"type": "rotation",
			"events": []
		}
	}`

	mockListRotationsV3Response = `{
		"rotations": [
			{"id": "ROT01", "type": "rotation"}
		],
		"limit": 25,
		"offset": 0,
		"more": false
	}`

	mockListEventsV3Response = `{
		"events": [
			{
				"id": "EVT01",
				"type": "schedule_event",
				"name": "On-Call Event",
				"start_time": {"date_time": "2026-02-21T09:00:00Z", "time_zone": "America/New_York"},
				"end_time":   {"date_time": "2026-02-21T17:00:00Z", "time_zone": "America/New_York"},
				"effective_since": "2026-02-21T09:00:00Z",
				"effective_until": null,
				"recurrence": ["RRULE:FREQ=WEEKLY;BYDAY=MO,TU,WE,TH,FR"],
				"assignment_strategy": {
					"type": "rotating_member_assignment_strategy",
					"shifts_per_member": 1,
					"members": [
						{"type": "user_member", "user_id": "USER01"}
					]
				}
			}
		],
		"limit": 25,
		"offset": 0,
		"more": false
	}`

	mockEventV3Response = `{
		"event": {
			"id": "EVT01",
			"type": "schedule_event",
			"name": "On-Call Event",
			"start_time": {"date_time": "2026-02-21T09:00:00Z", "time_zone": "America/New_York"},
			"end_time":   {"date_time": "2026-02-21T17:00:00Z", "time_zone": "America/New_York"},
			"effective_since": "2026-02-21T09:00:00Z",
			"effective_until": null,
			"recurrence": ["RRULE:FREQ=WEEKLY;BYDAY=MO,TU,WE,TH,FR"],
			"assignment_strategy": {
				"type": "rotating_member_assignment_strategy",
				"shifts_per_member": 1,
				"members": [
					{"type": "user_member", "user_id": "USER01"}
				]
			}
		}
	}`

	mockCustomShiftV3Response = `{
		"custom_shift": {
			"id": "CSH01",
			"type": "custom_shift",
			"start_time": "2026-03-15T09:00:00Z",
			"end_time":   "2026-03-15T17:00:00Z",
			"assignments": [
				{"id": "ASN01", "type": "shift_assignment", "member": {"type": "user_member", "user_id": "USER01"}}
			]
		}
	}`

	mockListCustomShiftsV3Response = `{
		"custom_shifts": [
			{
				"id": "CSH01",
				"type": "custom_shift",
				"start_time": "2026-03-15T09:00:00Z",
				"end_time":   "2026-03-15T17:00:00Z",
				"assignments": [
					{"id": "ASN01", "type": "shift_assignment", "member": {"type": "user_member", "user_id": "USER01"}}
				]
			}
		],
		"limit": 25,
		"offset": 0,
		"more": false
	}`

	mockCreateCustomShiftsV3Response = `{
		"custom_shifts": [
			{
				"id": "CSH01",
				"type": "custom_shift",
				"start_time": "2026-03-15T09:00:00Z",
				"end_time":   "2026-03-15T17:00:00Z",
				"assignments": [
					{"id": "ASN01", "type": "shift_assignment", "member": {"type": "user_member", "user_id": "USER01"}}
				]
			}
		]
	}`

	mockOverrideShiftV3Response = `{
		"override": {
			"id": "OVR01",
			"type": "override_shift",
			"rotation_id": "ROT01",
			"start_time": "2026-03-15T09:00:00Z",
			"end_time":   "2026-03-15T17:00:00Z",
			"overridden_member": {"type": "user_member", "user_id": "USER01"},
			"overriding_member": {"type": "user_member", "user_id": "USER02"}
		}
	}`

	mockListOverridesV3Response = `{
		"overrides": [
			{
				"id": "OVR01",
				"type": "override_shift",
				"rotation_id": "ROT01",
				"start_time": "2026-03-15T09:00:00Z",
				"end_time":   "2026-03-15T17:00:00Z",
				"overridden_member": {"type": "user_member", "user_id": "USER01"},
				"overriding_member": {"type": "user_member", "user_id": "USER02"}
			}
		],
		"limit": 25,
		"offset": 0,
		"more": false
	}`

	mockCreateOverridesV3Response = `{
		"overrides": [
			{
				"id": "OVR01",
				"type": "override_shift",
				"rotation_id": "ROT01",
				"start_time": "2026-03-15T09:00:00Z",
				"end_time":   "2026-03-15T17:00:00Z",
				"overridden_member": {"type": "user_member", "user_id": "USER01"},
				"overriding_member": {"type": "user_member", "user_id": "USER02"}
			}
		]
	}`

	mockScheduleV3Error400 = `{
		"error": {
			"code": 2001,
			"message": "Invalid Input Provided",
			"errors": ["name is required"]
		}
	}`

	mockScheduleV3Error404 = `{
		"error": {
			"code": 2100,
			"message": "Not Found",
			"errors": ["The specified resource does not exist"]
		}
	}`

	mockScheduleV3Error500 = `{
		"error": {
			"code": 3001,
			"message": "Internal Server Error",
			"errors": ["An unexpected error occurred"]
		}
	}`
)

// testV3EarlyAccessHeader verifies the X-Early-Access header required by
// every v3 endpoint is present on the outgoing request.
func testV3EarlyAccessHeader(t *testing.T, r *http.Request) {
	t.Helper()
	if got := r.Header.Get("X-Early-Access"); got == "" {
		t.Error("X-Early-Access header is missing from v3 request")
	}
}

// ---------------------------------------------------------------------------
// unmarshalApiErrorObject — v3 map[string][]string error format
// ---------------------------------------------------------------------------

func TestUnmarshalApiErrorObject_V3MapFormat(t *testing.T) {
	data := []byte(`{"code":2001,"message":"Unprocessable Entity","errors":{"name":["can't be blank"]}}`)
	aeo, err := unmarshalApiErrorObject(data)
	if err != nil {
		t.Fatal(err)
	}
	if aeo.Code != 2001 {
		t.Errorf("Code = %d, want 2001", aeo.Code)
	}
	if aeo.Message != "Unprocessable Entity" {
		t.Errorf("Message = %q, want %q", aeo.Message, "Unprocessable Entity")
	}
	if len(aeo.Errors) != 1 {
		t.Fatalf("len(Errors) = %d, want 1", len(aeo.Errors))
	}
	if aeo.Errors[0] != "name: can't be blank" {
		t.Errorf("Errors[0] = %q, want %q", aeo.Errors[0], "name: can't be blank")
	}
}

// ---------------------------------------------------------------------------
// ListSchedulesV3
// ---------------------------------------------------------------------------

func TestScheduleV3_List(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testV3EarlyAccessHeader(t, r)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(mockScheduleV3ListResponse))
	})

	client := defaultTestClient(server.URL, "foo")
	res, err := client.ListSchedulesV3(context.Background(), ListSchedulesV3Options{})
	if err != nil {
		t.Fatal(err)
	}

	want := &ListSchedulesV3Response{
		Schedules: []APIObject{
			{
				ID:      testScheduleV3ID,
				Type:    "schedule_reference",
				Summary: "On-Call Schedule",
				Self:    "https://api.pagerduty.com/v3/schedules/SCHED01",
				HTMLURL: "https://example.pagerduty.com/schedules/SCHED01",
			},
		},
	}
	testEqual(t, want, res)
}

func TestScheduleV3_ListWithQuery(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		if got := r.URL.Query().Get("query"); got != "on-call" {
			t.Errorf("query param = %q, want %q", got, "on-call")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(mockScheduleV3ListEmptyResponse))
	})

	client := defaultTestClient(server.URL, "foo")
	res, err := client.ListSchedulesV3(context.Background(), ListSchedulesV3Options{Query: "on-call"})
	if err != nil {
		t.Fatal(err)
	}

	want := &ListSchedulesV3Response{Schedules: []APIObject{}}
	testEqual(t, want, res)
}

func TestScheduleV3_List500Error(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(mockScheduleV3Error500))
	})

	client := defaultTestClient(server.URL, "foo")
	_, err := client.ListSchedulesV3(context.Background(), ListSchedulesV3Options{})
	if !testErrCheck(t, "ListSchedulesV3", "status code 500", err) {
		return
	}
}

// ---------------------------------------------------------------------------
// GetScheduleV3
// ---------------------------------------------------------------------------

func TestScheduleV3_Get(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testV3EarlyAccessHeader(t, r)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(mockScheduleV3GetResponse))
	})

	client := defaultTestClient(server.URL, "foo")
	res, err := client.GetScheduleV3(context.Background(), testScheduleV3ID, GetScheduleV3Options{})
	if err != nil {
		t.Fatal(err)
	}

	want := &ScheduleV3{
		ID:          testScheduleV3ID,
		Type:        "schedule",
		Name:        "On-Call Schedule",
		TimeZone:    "UTC",
		Description: "Test schedule",
		// Events key absent in JSON → nil slice
		Rotations: []RotationV3{
			{ID: testRotationV3ID, Type: "rotation"},
		},
	}
	testEqual(t, want, res)
}

func TestScheduleV3_GetWithFinalSchedule(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testV3EarlyAccessHeader(t, r)
		if got := r.URL.Query().Get("since"); got == "" {
			t.Error("expected since query param")
		}
		if got := r.URL.Query().Get("until"); got == "" {
			t.Error("expected until query param")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(mockScheduleV3GetWithFinalResponse))
	})

	client := defaultTestClient(server.URL, "foo")
	res, err := client.GetScheduleV3(context.Background(), testScheduleV3ID, GetScheduleV3Options{
		Since:   "2026-03-01T00:00:00Z",
		Until:   "2026-03-08T00:00:00Z",
		Include: []string{"final_schedule"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if res.FinalSchedule == nil {
		t.Fatal("expected FinalSchedule to be non-nil")
	}
	if res.FinalSchedule.RenderedCoveragePercentage != 100 {
		t.Errorf("RenderedCoveragePercentage = %v, want 100", res.FinalSchedule.RenderedCoveragePercentage)
	}
	if len(res.FinalSchedule.ComputedShiftAssignments) != 1 {
		t.Fatalf("len(ComputedShiftAssignments) = %d, want 1", len(res.FinalSchedule.ComputedShiftAssignments))
	}
}

func TestScheduleV3_Get404Error(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(mockScheduleV3Error404))
	})

	client := defaultTestClient(server.URL, "foo")
	_, err := client.GetScheduleV3(context.Background(), testScheduleV3ID, GetScheduleV3Options{})
	if !testErrCheck(t, "GetScheduleV3", "Not Found", err) {
		return
	}
}

func TestScheduleV3_GetWithEscalationPolicies(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testV3EarlyAccessHeader(t, r)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(mockScheduleV3GetWithEscalationPoliciesResponse))
	})

	client := defaultTestClient(server.URL, "foo")
	res, err := client.GetScheduleV3(context.Background(), testScheduleV3ID)
	if err != nil {
		t.Fatalf("GetScheduleV3 returned error: %v", err)
	}

	if len(res.EscalationPolicies) != 1 {
		t.Fatalf("len(EscalationPolicies) = %d, want 1", len(res.EscalationPolicies))
	}
	if got := res.EscalationPolicies[0].ID; got != "EP01" {
		t.Errorf("EscalationPolicies[0].ID = %q, want %q", got, "EP01")
	}
	if got := res.EscalationPolicies[0].Type; got != "escalation_policy" {
		t.Errorf("EscalationPolicies[0].Type = %q, want %q", got, "escalation_policy")
	}
}

// ---------------------------------------------------------------------------
// CreateScheduleV3
// ---------------------------------------------------------------------------

func TestScheduleV3_Create(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testV3EarlyAccessHeader(t, r)
		// Verify teams are forwarded in the request payload.
		var body createScheduleV3Request
		testBodyContains(t, r, &body)
		if len(body.Schedule.Teams) != 1 || body.Schedule.Teams[0].ID != "TEAM01" {
			t.Errorf("request teams = %v, want [{TEAM01 team_reference}]", body.Schedule.Teams)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(mockScheduleV3MutateResponse))
	})

	client := defaultTestClient(server.URL, "foo")
	res, err := client.CreateScheduleV3(context.Background(), ScheduleV3Input{
		Name:        "On-Call Schedule",
		TimeZone:    "UTC",
		Description: "Test schedule",
		Teams:       []TeamReferenceV3{{ID: "TEAM01", Type: "team_reference"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	want := &ScheduleV3{
		ID:          testScheduleV3ID,
		Type:        "schedule",
		Name:        "On-Call Schedule",
		TimeZone:    "UTC",
		Description: "Test schedule",
		Teams:       []TeamReferenceV3{{ID: "TEAM01", Type: "team_reference"}},
	}
	testEqual(t, want, res)
}

func TestScheduleV3_Create400Error(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(mockScheduleV3Error400))
	})

	client := defaultTestClient(server.URL, "foo")
	_, err := client.CreateScheduleV3(context.Background(), ScheduleV3Input{})
	if !testErrCheck(t, "CreateScheduleV3", "Invalid Input", err) {
		return
	}
}

// The v3 API must respond 201; any other 2xx is treated as an error.
func TestScheduleV3_CreateNon201Status(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK) // 200 instead of 201
		_, _ = w.Write([]byte(mockScheduleV3MutateResponse))
	})

	client := defaultTestClient(server.URL, "foo")
	_, err := client.CreateScheduleV3(context.Background(), ScheduleV3Input{Name: "Test", TimeZone: "UTC"})
	if !testErrCheck(t, "CreateScheduleV3", "failed to create v3 schedule", err) {
		return
	}
}

// ---------------------------------------------------------------------------
// UpdateScheduleV3
// ---------------------------------------------------------------------------

func TestScheduleV3_Update(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "PUT")
		testV3EarlyAccessHeader(t, r)
		// Verify teams are forwarded in the request payload.
		var body updateScheduleV3Request
		testBodyContains(t, r, &body)
		if len(body.Schedule.Teams) != 1 || body.Schedule.Teams[0].ID != "TEAM01" {
			t.Errorf("request teams = %v, want [{TEAM01 team_reference}]", body.Schedule.Teams)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(mockScheduleV3MutateResponse))
	})

	client := defaultTestClient(server.URL, "foo")
	res, err := client.UpdateScheduleV3(context.Background(), testScheduleV3ID, ScheduleV3Input{
		Name:        "On-Call Schedule",
		TimeZone:    "UTC",
		Description: "Test schedule",
		Teams:       []TeamReferenceV3{{ID: "TEAM01", Type: "team_reference"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	want := &ScheduleV3{
		ID:          testScheduleV3ID,
		Type:        "schedule",
		Name:        "On-Call Schedule",
		TimeZone:    "UTC",
		Description: "Test schedule",
		Teams:       []TeamReferenceV3{{ID: "TEAM01", Type: "team_reference"}},
	}
	testEqual(t, want, res)
}

func TestScheduleV3_Update404Error(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "PUT")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(mockScheduleV3Error404))
	})

	client := defaultTestClient(server.URL, "foo")
	_, err := client.UpdateScheduleV3(context.Background(), testScheduleV3ID, ScheduleV3Input{})
	if !testErrCheck(t, "UpdateScheduleV3", "Not Found", err) {
		return
	}
}

// ---------------------------------------------------------------------------
// DeleteScheduleV3
// ---------------------------------------------------------------------------

func TestScheduleV3_Delete(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "DELETE")
		testV3EarlyAccessHeader(t, r)
		w.WriteHeader(http.StatusNoContent)
	})

	client := defaultTestClient(server.URL, "foo")
	err := client.DeleteScheduleV3(context.Background(), testScheduleV3ID)
	if err != nil {
		t.Fatal(err)
	}
}

func TestScheduleV3_Delete404Error(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "DELETE")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(mockScheduleV3Error404))
	})

	client := defaultTestClient(server.URL, "foo")
	err := client.DeleteScheduleV3(context.Background(), testScheduleV3ID)
	if !testErrCheck(t, "DeleteScheduleV3", "Not Found", err) {
		return
	}
}

// ---------------------------------------------------------------------------
// ListRotationsV3
// ---------------------------------------------------------------------------

func TestRotationV3_List(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/rotations", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testV3EarlyAccessHeader(t, r)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(mockListRotationsV3Response))
	})

	client := defaultTestClient(server.URL, "foo")
	res, err := client.ListRotationsV3(context.Background(), testScheduleV3ID, ListRotationsV3Options{})
	if err != nil {
		t.Fatal(err)
	}

	want := &ListRotationsV3Response{
		Rotations: []RotationV3{
			{ID: testRotationV3ID, Type: "rotation"},
		},
		Limit:  25,
		Offset: 0,
		More:   false,
	}
	testEqual(t, want, res)
}

func TestRotationV3_List404Error(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/rotations", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(mockScheduleV3Error404))
	})

	client := defaultTestClient(server.URL, "foo")
	_, err := client.ListRotationsV3(context.Background(), testScheduleV3ID, ListRotationsV3Options{})
	if !testErrCheck(t, "ListRotationsV3", "Not Found", err) {
		return
	}
}

// ---------------------------------------------------------------------------
// CreateRotationV3
// ---------------------------------------------------------------------------

func TestRotationV3_Create(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/rotations", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testV3EarlyAccessHeader(t, r)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(mockRotationV3Response))
	})

	client := defaultTestClient(server.URL, "foo")
	res, err := client.CreateRotationV3(context.Background(), testScheduleV3ID)
	if err != nil {
		t.Fatal(err)
	}

	want := &RotationV3{
		ID:     testRotationV3ID,
		Type:   "rotation",
		Events: []EventV3{},
	}
	testEqual(t, want, res)
}

func TestRotationV3_CreateNon201Status(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/rotations", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK) // 200 instead of 201
		_, _ = w.Write([]byte(mockRotationV3Response))
	})

	client := defaultTestClient(server.URL, "foo")
	_, err := client.CreateRotationV3(context.Background(), testScheduleV3ID)
	if !testErrCheck(t, "CreateRotationV3", "failed to create v3 rotation", err) {
		return
	}
}

func TestRotationV3_Create404Error(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/rotations", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(mockScheduleV3Error404))
	})

	client := defaultTestClient(server.URL, "foo")
	_, err := client.CreateRotationV3(context.Background(), testScheduleV3ID)
	if !testErrCheck(t, "CreateRotationV3", "Not Found", err) {
		return
	}
}

// ---------------------------------------------------------------------------
// GetRotationV3
// ---------------------------------------------------------------------------

func TestRotationV3_Get(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/rotations/"+testRotationV3ID, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testV3EarlyAccessHeader(t, r)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(mockRotationV3Response))
	})

	client := defaultTestClient(server.URL, "foo")
	res, err := client.GetRotationV3(context.Background(), testScheduleV3ID, testRotationV3ID, GetRotationV3Options{})
	if err != nil {
		t.Fatal(err)
	}

	want := &RotationV3{
		ID:     testRotationV3ID,
		Type:   "rotation",
		Events: []EventV3{},
	}
	testEqual(t, want, res)
}

func TestRotationV3_GetWithTimeRange(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/rotations/"+testRotationV3ID, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		if got := r.URL.Query().Get("since"); got != "2026-03-01T00:00:00Z" {
			t.Errorf("since = %q, want %q", got, "2026-03-01T00:00:00Z")
		}
		if got := r.URL.Query().Get("until"); got != "2026-03-08T00:00:00Z" {
			t.Errorf("until = %q, want %q", got, "2026-03-08T00:00:00Z")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(mockRotationV3Response))
	})

	client := defaultTestClient(server.URL, "foo")
	_, err := client.GetRotationV3(context.Background(), testScheduleV3ID, testRotationV3ID, GetRotationV3Options{
		Since: "2026-03-01T00:00:00Z",
		Until: "2026-03-08T00:00:00Z",
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRotationV3_Get404Error(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/rotations/"+testRotationV3ID, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(mockScheduleV3Error404))
	})

	client := defaultTestClient(server.URL, "foo")
	_, err := client.GetRotationV3(context.Background(), testScheduleV3ID, testRotationV3ID, GetRotationV3Options{})
	if !testErrCheck(t, "GetRotationV3", "Not Found", err) {
		return
	}
}

// ---------------------------------------------------------------------------
// DeleteRotationV3
// ---------------------------------------------------------------------------

func TestRotationV3_Delete(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/rotations/"+testRotationV3ID, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "DELETE")
		testV3EarlyAccessHeader(t, r)
		w.WriteHeader(http.StatusNoContent)
	})

	client := defaultTestClient(server.URL, "foo")
	err := client.DeleteRotationV3(context.Background(), testScheduleV3ID, testRotationV3ID)
	if err != nil {
		t.Fatal(err)
	}
}

func TestRotationV3_Delete404Error(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/rotations/"+testRotationV3ID, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "DELETE")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(mockScheduleV3Error404))
	})

	client := defaultTestClient(server.URL, "foo")
	err := client.DeleteRotationV3(context.Background(), testScheduleV3ID, testRotationV3ID)
	if !testErrCheck(t, "DeleteRotationV3", "Not Found", err) {
		return
	}
}

// ---------------------------------------------------------------------------
// ListEventsV3
// ---------------------------------------------------------------------------

func TestEventV3_List(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/rotations/"+testRotationV3ID+"/events", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testV3EarlyAccessHeader(t, r)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(mockListEventsV3Response))
	})

	client := defaultTestClient(server.URL, "foo")
	res, err := client.ListEventsV3(context.Background(), testScheduleV3ID, testRotationV3ID, ListEventsV3Options{})
	if err != nil {
		t.Fatal(err)
	}

	if len(res.Events) != 1 {
		t.Fatalf("len(Events) = %d, want 1", len(res.Events))
	}
	if res.Events[0].ID != testEventV3ID {
		t.Errorf("Events[0].ID = %q, want %q", res.Events[0].ID, testEventV3ID)
	}
	if res.Limit != 25 {
		t.Errorf("Limit = %d, want 25", res.Limit)
	}
}

func TestEventV3_List404Error(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/rotations/"+testRotationV3ID+"/events", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(mockScheduleV3Error404))
	})

	client := defaultTestClient(server.URL, "foo")
	_, err := client.ListEventsV3(context.Background(), testScheduleV3ID, testRotationV3ID, ListEventsV3Options{})
	if !testErrCheck(t, "ListEventsV3", "Not Found", err) {
		return
	}
}

// ---------------------------------------------------------------------------
// CreateEventV3
// ---------------------------------------------------------------------------

func TestEventV3_Create(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/rotations/"+testRotationV3ID+"/events", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testV3EarlyAccessHeader(t, r)
		// Verify the request body is wrapped under "event" key.
		var body struct {
			Event EventV3 `json:"event"`
		}
		testBodyContains(t, r, &body)
		if body.Event.Name != "On-Call Event" {
			t.Errorf("request body event.name = %q, want %q", body.Event.Name, "On-Call Event")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(mockEventV3Response))
	})

	shiftsPerMember := 1
	userID := "USER01"
	input := EventV3{
		Name:           "On-Call Event",
		StartTime:      EventTimeV3{DateTime: "2026-02-21T09:00:00Z", TimeZone: "America/New_York"},
		EndTime:        EventTimeV3{DateTime: "2026-02-21T17:00:00Z", TimeZone: "America/New_York"},
		EffectiveSince: "2026-02-21T09:00:00Z",
		Recurrence:     []string{"RRULE:FREQ=WEEKLY;BYDAY=MO,TU,WE,TH,FR"},
		AssignmentStrategy: AssignmentStrategyV3{
			Type:            "rotating_member_assignment_strategy",
			ShiftsPerMember: &shiftsPerMember,
			Members:         []MemberV3{{Type: "user_member", UserID: &userID}},
		},
	}

	client := defaultTestClient(server.URL, "foo")
	res, err := client.CreateEventV3(context.Background(), testScheduleV3ID, testRotationV3ID, input)
	if err != nil {
		t.Fatal(err)
	}

	want := &EventV3{
		ID:             testEventV3ID,
		Type:           "schedule_event",
		Name:           "On-Call Event",
		StartTime:      EventTimeV3{DateTime: "2026-02-21T09:00:00Z", TimeZone: "America/New_York"},
		EndTime:        EventTimeV3{DateTime: "2026-02-21T17:00:00Z", TimeZone: "America/New_York"},
		EffectiveSince: "2026-02-21T09:00:00Z",
		EffectiveUntil: nil,
		Recurrence:     []string{"RRULE:FREQ=WEEKLY;BYDAY=MO,TU,WE,TH,FR"},
		AssignmentStrategy: AssignmentStrategyV3{
			Type:            "rotating_member_assignment_strategy",
			ShiftsPerMember: &shiftsPerMember,
			Members:         []MemberV3{{Type: "user_member", UserID: &userID}},
		},
	}
	testEqual(t, want, res)
}

func TestEventV3_CreateNon201Status(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/rotations/"+testRotationV3ID+"/events", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK) // 200 instead of 201
		_, _ = w.Write([]byte(mockEventV3Response))
	})

	client := defaultTestClient(server.URL, "foo")
	_, err := client.CreateEventV3(context.Background(), testScheduleV3ID, testRotationV3ID, EventV3{})
	if !testErrCheck(t, "CreateEventV3", "failed to create v3 event", err) {
		return
	}
}

func TestEventV3_Create400Error(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/rotations/"+testRotationV3ID+"/events", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(mockScheduleV3Error400))
	})

	client := defaultTestClient(server.URL, "foo")
	_, err := client.CreateEventV3(context.Background(), testScheduleV3ID, testRotationV3ID, EventV3{})
	if !testErrCheck(t, "CreateEventV3", "Invalid Input", err) {
		return
	}
}

// ---------------------------------------------------------------------------
// GetEventV3
// ---------------------------------------------------------------------------

func TestEventV3_Get(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/rotations/"+testRotationV3ID+"/events/"+testEventV3ID, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testV3EarlyAccessHeader(t, r)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(mockEventV3Response))
	})

	client := defaultTestClient(server.URL, "foo")
	res, err := client.GetEventV3(context.Background(), testScheduleV3ID, testRotationV3ID, testEventV3ID, GetEventV3Options{})
	if err != nil {
		t.Fatal(err)
	}

	shiftsPerMember := 1
	userID := "USER01"
	want := &EventV3{
		ID:             testEventV3ID,
		Type:           "schedule_event",
		Name:           "On-Call Event",
		StartTime:      EventTimeV3{DateTime: "2026-02-21T09:00:00Z", TimeZone: "America/New_York"},
		EndTime:        EventTimeV3{DateTime: "2026-02-21T17:00:00Z", TimeZone: "America/New_York"},
		EffectiveSince: "2026-02-21T09:00:00Z",
		EffectiveUntil: nil,
		Recurrence:     []string{"RRULE:FREQ=WEEKLY;BYDAY=MO,TU,WE,TH,FR"},
		AssignmentStrategy: AssignmentStrategyV3{
			Type:            "rotating_member_assignment_strategy",
			ShiftsPerMember: &shiftsPerMember,
			Members:         []MemberV3{{Type: "user_member", UserID: &userID}},
		},
	}
	testEqual(t, want, res)
}

func TestEventV3_Get404Error(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/rotations/"+testRotationV3ID+"/events/"+testEventV3ID, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(mockScheduleV3Error404))
	})

	client := defaultTestClient(server.URL, "foo")
	_, err := client.GetEventV3(context.Background(), testScheduleV3ID, testRotationV3ID, testEventV3ID, GetEventV3Options{})
	if !testErrCheck(t, "GetEventV3", "Not Found", err) {
		return
	}
}

// ---------------------------------------------------------------------------
// UpdateEventV3
// ---------------------------------------------------------------------------

func TestEventV3_Update(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/rotations/"+testRotationV3ID+"/events/"+testEventV3ID, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "PUT")
		testV3EarlyAccessHeader(t, r)
		// Verify the request body is wrapped under "event" key.
		var body struct {
			Event EventV3 `json:"event"`
		}
		testBodyContains(t, r, &body)
		if body.Event.Name != "On-Call Event" {
			t.Errorf("request body event.name = %q, want %q", body.Event.Name, "On-Call Event")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(mockEventV3Response))
	})

	shiftsPerMember := 1
	userID := "USER01"
	input := EventV3{
		Name:           "On-Call Event",
		StartTime:      EventTimeV3{DateTime: "2026-02-21T09:00:00Z", TimeZone: "America/New_York"},
		EndTime:        EventTimeV3{DateTime: "2026-02-21T17:00:00Z", TimeZone: "America/New_York"},
		EffectiveSince: "2026-02-21T09:00:00Z",
		Recurrence:     []string{"RRULE:FREQ=WEEKLY;BYDAY=MO,TU,WE,TH,FR"},
		AssignmentStrategy: AssignmentStrategyV3{
			Type:            "rotating_member_assignment_strategy",
			ShiftsPerMember: &shiftsPerMember,
			Members:         []MemberV3{{Type: "user_member", UserID: &userID}},
		},
	}

	client := defaultTestClient(server.URL, "foo")
	res, err := client.UpdateEventV3(context.Background(), testScheduleV3ID, testRotationV3ID, testEventV3ID, input)
	if err != nil {
		t.Fatal(err)
	}

	want := &EventV3{
		ID:             testEventV3ID,
		Type:           "schedule_event",
		Name:           "On-Call Event",
		StartTime:      EventTimeV3{DateTime: "2026-02-21T09:00:00Z", TimeZone: "America/New_York"},
		EndTime:        EventTimeV3{DateTime: "2026-02-21T17:00:00Z", TimeZone: "America/New_York"},
		EffectiveSince: "2026-02-21T09:00:00Z",
		EffectiveUntil: nil,
		Recurrence:     []string{"RRULE:FREQ=WEEKLY;BYDAY=MO,TU,WE,TH,FR"},
		AssignmentStrategy: AssignmentStrategyV3{
			Type:            "rotating_member_assignment_strategy",
			ShiftsPerMember: &shiftsPerMember,
			Members:         []MemberV3{{Type: "user_member", UserID: &userID}},
		},
	}
	testEqual(t, want, res)
}

func TestEventV3_Update404Error(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/rotations/"+testRotationV3ID+"/events/"+testEventV3ID, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "PUT")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(mockScheduleV3Error404))
	})

	client := defaultTestClient(server.URL, "foo")
	_, err := client.UpdateEventV3(context.Background(), testScheduleV3ID, testRotationV3ID, testEventV3ID, EventV3{})
	if !testErrCheck(t, "UpdateEventV3", "Not Found", err) {
		return
	}
}

// ---------------------------------------------------------------------------
// DeleteEventV3
// ---------------------------------------------------------------------------

func TestEventV3_Delete(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/rotations/"+testRotationV3ID+"/events/"+testEventV3ID, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "DELETE")
		testV3EarlyAccessHeader(t, r)
		w.WriteHeader(http.StatusNoContent)
	})

	client := defaultTestClient(server.URL, "foo")
	err := client.DeleteEventV3(context.Background(), testScheduleV3ID, testRotationV3ID, testEventV3ID)
	if err != nil {
		t.Fatal(err)
	}
}

func TestEventV3_Delete404Error(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/rotations/"+testRotationV3ID+"/events/"+testEventV3ID, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "DELETE")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(mockScheduleV3Error404))
	})

	client := defaultTestClient(server.URL, "foo")
	err := client.DeleteEventV3(context.Background(), testScheduleV3ID, testRotationV3ID, testEventV3ID)
	if !testErrCheck(t, "DeleteEventV3", "Not Found", err) {
		return
	}
}

// ---------------------------------------------------------------------------
// ListCustomShiftsV3
// ---------------------------------------------------------------------------

func TestCustomShiftV3_List(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/custom_shifts", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testV3EarlyAccessHeader(t, r)
		if got := r.URL.Query().Get("since"); got == "" {
			t.Error("expected since query param")
		}
		if got := r.URL.Query().Get("until"); got == "" {
			t.Error("expected until query param")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(mockListCustomShiftsV3Response))
	})

	client := defaultTestClient(server.URL, "foo")
	res, err := client.ListCustomShiftsV3(context.Background(), testScheduleV3ID, ListCustomShiftsV3Options{
		Since: "2026-03-01T00:00:00Z",
		Until: "2026-03-31T00:00:00Z",
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(res.CustomShifts) != 1 {
		t.Fatalf("len(CustomShifts) = %d, want 1", len(res.CustomShifts))
	}
	if res.CustomShifts[0].ID != testCustomShiftID {
		t.Errorf("CustomShifts[0].ID = %q, want %q", res.CustomShifts[0].ID, testCustomShiftID)
	}
}

func TestCustomShiftV3_List404Error(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/custom_shifts", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(mockScheduleV3Error404))
	})

	client := defaultTestClient(server.URL, "foo")
	_, err := client.ListCustomShiftsV3(context.Background(), testScheduleV3ID, ListCustomShiftsV3Options{
		Since: "2026-03-01T00:00:00Z",
		Until: "2026-03-31T00:00:00Z",
	})
	if !testErrCheck(t, "ListCustomShiftsV3", "Not Found", err) {
		return
	}
}

// ---------------------------------------------------------------------------
// CreateCustomShiftsV3
// ---------------------------------------------------------------------------

func TestCustomShiftV3_Create(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/custom_shifts", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testV3EarlyAccessHeader(t, r)
		var body createCustomShiftsV3Request
		testBodyContains(t, r, &body)
		if len(body.CustomShifts) != 1 {
			t.Fatalf("len(custom_shifts) = %d, want 1", len(body.CustomShifts))
		}
		if body.CustomShifts[0].StartTime != "2026-03-15T09:00:00Z" {
			t.Errorf("start_time = %q, want %q", body.CustomShifts[0].StartTime, "2026-03-15T09:00:00Z")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(mockCreateCustomShiftsV3Response))
	})

	userID := "USER01"
	input := []CustomShiftInputV3{
		{
			Type:      "custom_shift",
			StartTime: "2026-03-15T09:00:00Z",
			EndTime:   "2026-03-15T17:00:00Z",
			Assignments: []ShiftAssignmentV3{
				{Type: "shift_assignment", Member: MemberV3{Type: "user_member", UserID: &userID}},
			},
		},
	}

	client := defaultTestClient(server.URL, "foo")
	res, err := client.CreateCustomShiftsV3(context.Background(), testScheduleV3ID, input)
	if err != nil {
		t.Fatal(err)
	}

	if len(res) != 1 {
		t.Fatalf("len(result) = %d, want 1", len(res))
	}
	if res[0].ID != testCustomShiftID {
		t.Errorf("ID = %q, want %q", res[0].ID, testCustomShiftID)
	}
}

func TestCustomShiftV3_CreateNon201Status(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/custom_shifts", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK) // 200 instead of 201
		_, _ = w.Write([]byte(mockCreateCustomShiftsV3Response))
	})

	client := defaultTestClient(server.URL, "foo")
	_, err := client.CreateCustomShiftsV3(context.Background(), testScheduleV3ID, []CustomShiftInputV3{})
	if !testErrCheck(t, "CreateCustomShiftsV3", "failed to create v3 custom shifts", err) {
		return
	}
}

// ---------------------------------------------------------------------------
// GetCustomShiftV3
// ---------------------------------------------------------------------------

func TestCustomShiftV3_Get(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/custom_shifts/"+testCustomShiftID, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testV3EarlyAccessHeader(t, r)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(mockCustomShiftV3Response))
	})

	client := defaultTestClient(server.URL, "foo")
	res, err := client.GetCustomShiftV3(context.Background(), testScheduleV3ID, testCustomShiftID)
	if err != nil {
		t.Fatal(err)
	}

	userID := "USER01"
	want := &CustomShiftV3{
		ID:        testCustomShiftID,
		Type:      "custom_shift",
		StartTime: "2026-03-15T09:00:00Z",
		EndTime:   "2026-03-15T17:00:00Z",
		Assignments: []ShiftAssignmentV3{
			{ID: "ASN01", Type: "shift_assignment", Member: MemberV3{Type: "user_member", UserID: &userID}},
		},
	}
	testEqual(t, want, res)
}

func TestCustomShiftV3_Get404Error(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/custom_shifts/"+testCustomShiftID, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(mockScheduleV3Error404))
	})

	client := defaultTestClient(server.URL, "foo")
	_, err := client.GetCustomShiftV3(context.Background(), testScheduleV3ID, testCustomShiftID)
	if !testErrCheck(t, "GetCustomShiftV3", "Not Found", err) {
		return
	}
}

// ---------------------------------------------------------------------------
// UpdateCustomShiftV3
// ---------------------------------------------------------------------------

func TestCustomShiftV3_Update(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/custom_shifts/"+testCustomShiftID, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "PUT")
		testV3EarlyAccessHeader(t, r)
		var body updateCustomShiftV3Request
		testBodyContains(t, r, &body)
		if body.CustomShift.EndTime != "2026-03-15T18:00:00Z" {
			t.Errorf("end_time = %q, want %q", body.CustomShift.EndTime, "2026-03-15T18:00:00Z")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(mockCustomShiftV3Response))
	})

	client := defaultTestClient(server.URL, "foo")
	res, err := client.UpdateCustomShiftV3(context.Background(), testScheduleV3ID, testCustomShiftID, CustomShiftUpdateV3{
		EndTime: "2026-03-15T18:00:00Z",
	})
	if err != nil {
		t.Fatal(err)
	}

	if res.ID != testCustomShiftID {
		t.Errorf("ID = %q, want %q", res.ID, testCustomShiftID)
	}
}

func TestCustomShiftV3_Update404Error(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/custom_shifts/"+testCustomShiftID, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "PUT")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(mockScheduleV3Error404))
	})

	client := defaultTestClient(server.URL, "foo")
	_, err := client.UpdateCustomShiftV3(context.Background(), testScheduleV3ID, testCustomShiftID, CustomShiftUpdateV3{})
	if !testErrCheck(t, "UpdateCustomShiftV3", "Not Found", err) {
		return
	}
}

// ---------------------------------------------------------------------------
// DeleteCustomShiftV3
// ---------------------------------------------------------------------------

func TestCustomShiftV3_Delete(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/custom_shifts/"+testCustomShiftID, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "DELETE")
		testV3EarlyAccessHeader(t, r)
		w.WriteHeader(http.StatusNoContent)
	})

	client := defaultTestClient(server.URL, "foo")
	err := client.DeleteCustomShiftV3(context.Background(), testScheduleV3ID, testCustomShiftID)
	if err != nil {
		t.Fatal(err)
	}
}

func TestCustomShiftV3_Delete404Error(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/custom_shifts/"+testCustomShiftID, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "DELETE")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(mockScheduleV3Error404))
	})

	client := defaultTestClient(server.URL, "foo")
	err := client.DeleteCustomShiftV3(context.Background(), testScheduleV3ID, testCustomShiftID)
	if !testErrCheck(t, "DeleteCustomShiftV3", "Not Found", err) {
		return
	}
}

// ---------------------------------------------------------------------------
// ListOverridesV3
// ---------------------------------------------------------------------------

func TestOverrideV3_List(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/overrides", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testV3EarlyAccessHeader(t, r)
		if got := r.URL.Query().Get("since"); got == "" {
			t.Error("expected since query param")
		}
		if got := r.URL.Query().Get("until"); got == "" {
			t.Error("expected until query param")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(mockListOverridesV3Response))
	})

	client := defaultTestClient(server.URL, "foo")
	res, err := client.ListOverridesV3(context.Background(), testScheduleV3ID, ListOverridesV3Options{
		Since: "2026-03-01T00:00:00Z",
		Until: "2026-03-31T00:00:00Z",
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(res.Overrides) != 1 {
		t.Fatalf("len(Overrides) = %d, want 1", len(res.Overrides))
	}
	if res.Overrides[0].ID != testOverrideV3ID {
		t.Errorf("Overrides[0].ID = %q, want %q", res.Overrides[0].ID, testOverrideV3ID)
	}
}

func TestOverrideV3_List404Error(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/overrides", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(mockScheduleV3Error404))
	})

	client := defaultTestClient(server.URL, "foo")
	_, err := client.ListOverridesV3(context.Background(), testScheduleV3ID, ListOverridesV3Options{
		Since: "2026-03-01T00:00:00Z",
		Until: "2026-03-31T00:00:00Z",
	})
	if !testErrCheck(t, "ListOverridesV3", "Not Found", err) {
		return
	}
}

// ---------------------------------------------------------------------------
// CreateOverridesV3
// ---------------------------------------------------------------------------

func TestOverrideV3_Create(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/overrides", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testV3EarlyAccessHeader(t, r)
		var body createOverridesV3Request
		testBodyContains(t, r, &body)
		if len(body.Overrides) != 1 {
			t.Fatalf("len(overrides) = %d, want 1", len(body.Overrides))
		}
		if body.Overrides[0].RotationID != "ROT01" {
			t.Errorf("rotation_id = %q, want %q", body.Overrides[0].RotationID, "ROT01")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(mockCreateOverridesV3Response))
	})

	user1 := "USER01"
	user2 := "USER02"
	input := []OverrideShiftInputV3{
		{
			Type:             "override_shift",
			RotationID:       "ROT01",
			StartTime:        "2026-03-15T09:00:00Z",
			EndTime:          "2026-03-15T17:00:00Z",
			OverriddenMember: MemberV3{Type: "user_member", UserID: &user1},
			OverridingMember: MemberV3{Type: "user_member", UserID: &user2},
		},
	}

	client := defaultTestClient(server.URL, "foo")
	res, err := client.CreateOverridesV3(context.Background(), testScheduleV3ID, input)
	if err != nil {
		t.Fatal(err)
	}

	if len(res) != 1 {
		t.Fatalf("len(result) = %d, want 1", len(res))
	}
	if res[0].ID != testOverrideV3ID {
		t.Errorf("ID = %q, want %q", res[0].ID, testOverrideV3ID)
	}
}

func TestOverrideV3_CreateNon201Status(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/overrides", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK) // 200 instead of 201
		_, _ = w.Write([]byte(mockCreateOverridesV3Response))
	})

	client := defaultTestClient(server.URL, "foo")
	_, err := client.CreateOverridesV3(context.Background(), testScheduleV3ID, []OverrideShiftInputV3{})
	if !testErrCheck(t, "CreateOverridesV3", "failed to create v3 overrides", err) {
		return
	}
}

// ---------------------------------------------------------------------------
// GetOverrideV3
// ---------------------------------------------------------------------------

func TestOverrideV3_Get(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/overrides/"+testOverrideV3ID, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testV3EarlyAccessHeader(t, r)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(mockOverrideShiftV3Response))
	})

	client := defaultTestClient(server.URL, "foo")
	res, err := client.GetOverrideV3(context.Background(), testScheduleV3ID, testOverrideV3ID)
	if err != nil {
		t.Fatal(err)
	}

	user1 := "USER01"
	user2 := "USER02"
	want := &OverrideShiftV3{
		ID:               testOverrideV3ID,
		Type:             "override_shift",
		RotationID:       "ROT01",
		StartTime:        "2026-03-15T09:00:00Z",
		EndTime:          "2026-03-15T17:00:00Z",
		OverriddenMember: MemberV3{Type: "user_member", UserID: &user1},
		OverridingMember: MemberV3{Type: "user_member", UserID: &user2},
	}
	testEqual(t, want, res)
}

func TestOverrideV3_Get404Error(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/overrides/"+testOverrideV3ID, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(mockScheduleV3Error404))
	})

	client := defaultTestClient(server.URL, "foo")
	_, err := client.GetOverrideV3(context.Background(), testScheduleV3ID, testOverrideV3ID)
	if !testErrCheck(t, "GetOverrideV3", "Not Found", err) {
		return
	}
}

// ---------------------------------------------------------------------------
// UpdateOverrideV3
// ---------------------------------------------------------------------------

func TestOverrideV3_Update(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/overrides/"+testOverrideV3ID, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "PUT")
		testV3EarlyAccessHeader(t, r)
		var body updateOverrideV3Request
		testBodyContains(t, r, &body)
		if body.Override.EndTime != "2026-03-15T18:00:00Z" {
			t.Errorf("end_time = %q, want %q", body.Override.EndTime, "2026-03-15T18:00:00Z")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(mockOverrideShiftV3Response))
	})

	client := defaultTestClient(server.URL, "foo")
	res, err := client.UpdateOverrideV3(context.Background(), testScheduleV3ID, testOverrideV3ID, OverrideShiftUpdateV3{
		EndTime: "2026-03-15T18:00:00Z",
	})
	if err != nil {
		t.Fatal(err)
	}

	if res.ID != testOverrideV3ID {
		t.Errorf("ID = %q, want %q", res.ID, testOverrideV3ID)
	}
}

func TestOverrideV3_Update404Error(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/overrides/"+testOverrideV3ID, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "PUT")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(mockScheduleV3Error404))
	})

	client := defaultTestClient(server.URL, "foo")
	_, err := client.UpdateOverrideV3(context.Background(), testScheduleV3ID, testOverrideV3ID, OverrideShiftUpdateV3{})
	if !testErrCheck(t, "UpdateOverrideV3", "Not Found", err) {
		return
	}
}

// ---------------------------------------------------------------------------
// DeleteOverrideV3
// ---------------------------------------------------------------------------

func TestOverrideV3_Delete(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/overrides/"+testOverrideV3ID, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "DELETE")
		testV3EarlyAccessHeader(t, r)
		w.WriteHeader(http.StatusNoContent)
	})

	client := defaultTestClient(server.URL, "foo")
	err := client.DeleteOverrideV3(context.Background(), testScheduleV3ID, testOverrideV3ID)
	if err != nil {
		t.Fatal(err)
	}
}

func TestOverrideV3_Delete404Error(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/v3/schedules/"+testScheduleV3ID+"/overrides/"+testOverrideV3ID, func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "DELETE")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(mockScheduleV3Error404))
	})

	client := defaultTestClient(server.URL, "foo")
	err := client.DeleteOverrideV3(context.Background(), testScheduleV3ID, testOverrideV3ID)
	if !testErrCheck(t, "DeleteOverrideV3", "Not Found", err) {
		return
	}
}
