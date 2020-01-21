// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package elastic

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/olivere/elastic/v7/uritemplates"
)

// ClusterUpdateSettingsService allows to review and change cluster-wide settings.
//
// See https://www.elastic.co/guide/en/elasticsearch/reference/7.0/cluster-update-settings.html
// for details.
type ClusterUpdateSettingsService struct {
	client *Client

	pretty     *bool       // pretty format the returned JSON response
	human      *bool       // return human readable values for statistics
	errorTrace *bool       // include the stack trace of returned errors
	filterPath []string    // list of filters used to reduce the response
	headers    http.Header // custom request-level HTTP headers

	includeDefaults *bool
	flatSettings    *bool
	bodyJson        interface{}
	bodyString      string
}

// NewClusterUpdateSettingsService returns a new ClusterUpdateSettingsService.
func NewClusterUpdateSettingsService(client *Client) *ClusterUpdateSettingsService {
	return &ClusterUpdateSettingsService{client: client}
}

// Pretty tells Elasticsearch whether to return a formatted JSON response.
func (s *ClusterUpdateSettingsService) Pretty(pretty bool) *ClusterUpdateSettingsService {
	s.pretty = &pretty
	return s
}

// Human specifies whether human readable values should be returned in
// the JSON response, e.g. "7.5mb".
func (s *ClusterUpdateSettingsService) Human(human bool) *ClusterUpdateSettingsService {
	s.human = &human
	return s
}

// ErrorTrace specifies whether to include the stack trace of returned errors.
func (s *ClusterUpdateSettingsService) ErrorTrace(errorTrace bool) *ClusterUpdateSettingsService {
	s.errorTrace = &errorTrace
	return s
}

// FilterPath specifies a list of filters used to reduce the response.
func (s *ClusterUpdateSettingsService) FilterPath(filterPath ...string) *ClusterUpdateSettingsService {
	s.filterPath = filterPath
	return s
}

// Header adds a header to the request.
func (s *ClusterUpdateSettingsService) Header(name string, value string) *ClusterUpdateSettingsService {
	if s.headers == nil {
		s.headers = http.Header{}
	}
	s.headers.Add(name, value)
	return s
}

// Headers specifies the headers of the request.
func (s *ClusterUpdateSettingsService) Headers(headers http.Header) *ClusterUpdateSettingsService {
	s.headers = headers
	return s
}

// IncludeDefaults ensures that the settings which were not set explicitly are also returned.
func (s *ClusterUpdateSettingsService) IncludeDefaults(includeDefaults bool) *ClusterUpdateSettingsService {
	s.includeDefaults = &includeDefaults
	return s
}

// FlatSettings is documented as: Return settings in flat format (default: false).
func (s *ClusterUpdateSettingsService) FlatSettings(flatSettings bool) *ClusterUpdateSettingsService {
	s.flatSettings = &flatSettings
	return s
}

// Body specifies the configuration of the index as a string.
// It is an alias for BodyString.
func (s *ClusterUpdateSettingsService) Body(body string) *ClusterUpdateSettingsService {
	s.bodyString = body
	return s
}

// BodyString specifies the configuration of the index as a string.
func (s *ClusterUpdateSettingsService) BodyString(body string) *ClusterUpdateSettingsService {
	s.bodyString = body
	return s
}

// BodyJson specifies the configuration of the index. The interface{} will
// be serializes as a JSON document, so use a map[string]interface{}.
func (s *ClusterUpdateSettingsService) BodyJson(body interface{}) *ClusterUpdateSettingsService {
	s.bodyJson = body
	return s
}

// Validate checks if the operation is valid.
func (s *ClusterUpdateSettingsService) Validate() error {
	return nil
}

// buildURL builds the URL for the operation.
func (s *ClusterUpdateSettingsService) buildURL() (string, url.Values, error) {
	// Build URL
	var err error
	var path string

	path, err = uritemplates.Expand("/_cluster/settings", map[string]string{})
	if err != nil {
		return "", url.Values{}, err
	}

	// Add query string parameters
	params := url.Values{}
	if v := s.pretty; v != nil {
		params.Set("pretty", fmt.Sprint(*v))
	}
	if v := s.human; v != nil {
		params.Set("human", fmt.Sprint(*v))
	}
	if v := s.errorTrace; v != nil {
		params.Set("error_trace", fmt.Sprint(*v))
	}
	if len(s.filterPath) > 0 {
		params.Set("filter_path", strings.Join(s.filterPath, ","))
	}
	if s.includeDefaults != nil {
		params.Set("include_defaults", fmt.Sprintf("%v", *s.includeDefaults))
	}
	if s.flatSettings != nil {
		params.Set("flat_settings", fmt.Sprintf("%v", *s.flatSettings))
	}
	return path, params, nil
}

// Do executes the operation.
func (s *ClusterUpdateSettingsService) Do(ctx context.Context) (*ClusterUpdateSettingsResponse, error) {
	// Check pre-conditions
	if err := s.Validate(); err != nil {
		return nil, err
	}

	// Get URL for request
	path, params, err := s.buildURL()
	if err != nil {
		return nil, err
	}

	// Setup HTTP request body
	var body interface{}
	if s.bodyJson != nil {
		body = s.bodyJson
	} else {
		body = s.bodyString
	}

	res := new(Response)
	if body != nil {
		// Update the cluster-wide settings.
		res, err = s.client.PerformRequest(ctx, PerformRequestOptions{
			Method:  "PUT",
			Path:    path,
			Params:  params,
			Body:    body,
			Headers: s.headers,
		})
	} else {
		//  Get the cluster-wide settings as response
		res, err = s.client.PerformRequest(ctx, PerformRequestOptions{
			Method:  "GET",
			Path:    path,
			Params:  params,
			Headers: s.headers,
		})
	}
	if err != nil {
		return nil, err
	}

	ret := new(ClusterUpdateSettingsResponse)
	if err := s.client.decoder.Decode(res.Body, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

// -- Result of a create index request.

// ClusterUpdateSettingsResponse is the response of ClusterUpdateSettingsService.Do().
type ClusterUpdateSettingsResponse struct {
	Acknowledged       bool   `json:"acknowledged"`
	ShardsAcknowledged bool   `json:"shards_acknowledged"`
	Index              string `json:"index,omitempty"`
}
