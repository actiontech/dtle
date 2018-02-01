package server

import (
	"udup/internal/models"
)

// Status endpoint is used to check on server status
type Status struct {
	srv *Server
}

// Version is used to allow clients to determine the capabilities
// of the server
func (s *Status) Version(args *models.GenericRequest, reply *models.VersionResponse) error {
	if done, err := s.srv.forward("Status.Version", args, args, reply); done {
		return err
	}

	conf := s.srv.config
	reply.Build = conf.Build
	return nil
}

// Ping is used to just check for connectivity
func (s *Status) Ping(args struct{}, reply *struct{}) error {
	return nil
}

// Leader is used to get the address of the leader
func (s *Status) Leader(args *models.GenericRequest, reply *string) error {
	if args.Region == "" {
		args.Region = s.srv.config.Region
	}
	if done, err := s.srv.forward("Status.Leader", args, args, reply); done {
		return err
	}

	leader := string(s.srv.raft.Leader())
	if leader != "" {
		*reply = leader
	} else {
		*reply = ""
	}
	return nil
}

// Peers is used to get all the Raft peers
func (s *Status) Peers(args *models.GenericRequest, reply *[]string) error {
	if args.Region == "" {
		args.Region = s.srv.config.Region
	}
	if done, err := s.srv.forward("Status.Peers", args, args, reply); done {
		return err
	}

	future := s.srv.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return err
	}

	for _, server := range future.Configuration().Servers {
		*reply = append(*reply, string(server.Address))
	}
	return nil
}

// List is used to list all of the known regions. No leader forwarding is
// required for this endpoint because memberlist is used to populate the
// peers list we read from.
func (s *Status) RegionList(args *models.GenericRequest, reply *[]string) error {
	*reply = s.srv.Regions()
	return nil
}

// Members return the list of servers in a cluster that a particular server is
// aware of
func (s *Status) Members(args *models.GenericRequest, reply *models.ServerMembersResponse) error {
	serfMembers := s.srv.Members()
	members := make([]*models.ServerMember, len(serfMembers))
	for i, mem := range serfMembers {
		members[i] = &models.ServerMember{
			Name:        mem.Name,
			Addr:        mem.Addr,
			Port:        mem.Port,
			Tags:        mem.Tags,
			Status:      mem.Status.String(),
			/*ProtocolMin: mem.ProtocolMin,
			ProtocolMax: mem.ProtocolMax,
			ProtocolCur: mem.ProtocolCur,
			DelegateMin: mem.DelegateMin,
			DelegateMax: mem.DelegateMax,
			DelegateCur: mem.DelegateCur,*/
		}
	}
	*reply = models.ServerMembersResponse{
		ServerName:   s.srv.config.NodeName,
		//ServerRegion: s.srv.config.Region,
		//ServerDC:     s.srv.config.Datacenter,
		Members:      members,
	}
	return nil
}
