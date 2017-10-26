package api

import "sort"

// Status is used to query the status-related endpoints.
type Status struct {
	client *Client
}

// Status returns a handle on the status endpoints.
func (c *Client) Status() *Status {
	return &Status{client: c}
}

// Leader is used to query for the current cluster leader.
func (s *Status) Leader() (string, error) {
	var resp string
	_, err := s.client.query("/v1/leader", &resp, nil)
	if err != nil {
		return "", err
	}
	return resp, nil
}

// RegionLeader is used to query for the leader in the passed region.
func (s *Status) RegionLeader(region string) (string, error) {
	var resp string
	q := QueryOptions{Region: region}
	_, err := s.client.query("/v1/leader", &resp, &q)
	if err != nil {
		return "", err
	}
	return resp, nil
}

// Peers is used to query the addresses of the server peers
// in the cluster.
func (s *Status) Peers() ([]string, error) {
	var resp []string
	_, err := s.client.query("/v1/peers", &resp, nil)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// List returns a list of all of the regions.
func (s *Status) List() ([]string, error) {
	var resp []string
	if _, err := s.client.query("/v1/regions", &resp, nil); err != nil {
		return nil, err
	}
	sort.Strings(resp)
	return resp, nil
}
