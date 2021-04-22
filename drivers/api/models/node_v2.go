package models

type NodeListItemV2 struct {
	NodeAddress           string `json:"node_address"`
	NodeName              string `json:"node_name"`
	NodeId                string `json:"node_id"`
	NodeStatus            string `json:"node_status"`
	NodeStatusDescription string `json:"node_status_description"`
	Datacenter            string `json:"datacenter"`
}

type NodeListRespV2 struct {
	Nodes []NodeListItemV2 `json:"nodes"`
	BaseResp
}
