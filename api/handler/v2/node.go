package v2

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/actiontech/dtle/g"

	"github.com/actiontech/dtle/api/handler"
	"github.com/actiontech/dtle/api/models"
	nomadApi "github.com/hashicorp/nomad/api"
	"github.com/labstack/echo/v4"
)

// @Id NodeListV2
// @Description get node list.
// @Tags node
// @Security ApiKeyAuth
// @Success 200 {object} models.NodeListRespV2
// @Router /v2/nodes [get]
func NodeListV2(c echo.Context) error {
	logger := handler.NewLogger().Named("NodeListV2")
	nodes, err := FindNomadNodes(logger)
	if nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invoke nomad api failed: %v", err)))
	}

	return c.JSON(http.StatusOK, &models.NodeListRespV2{
		Nodes:    nodes,
		BaseResp: models.BuildBaseResp(nil),
	})
}

func FindNomadNodes(logger g.LoggerType) ([]models.NodeListItemV2, error) {
	logger.Info("invoke nomad api begin : nodesUrl")
	nodesUrl := handler.BuildUrl("/v1/nodes")
	nomadNodes := make([]nomadApi.NodeListStub, 0)
	if err := handler.InvokeApiWithKvData(http.MethodGet, nodesUrl, nil, &nomadNodes); nil != err {
		return nil, err
	}
	nomadNodes, err := FindNodeList()
	if err != nil {
		return nil, err
	}

	membersUrl := handler.BuildUrl("/v1/agent/members")
	logger.Info("invoke nomad api begin", "membersUrl", membersUrl)
	nomadMembers := new(nomadApi.ServerMembers)
	if err := handler.InvokeApiWithKvData(http.MethodGet, membersUrl, nil, nomadMembers); nil != err {
		return nil, err
	}
	membersMap := make(map[string]struct{}, len(nomadMembers.Members))
	for _, agentMember := range nomadMembers.Members {
		membersMap[agentMember.Addr] = struct{}{}
	}

	leaderUrl := handler.BuildUrl("/v1/status/leader")
	logger.Info("invoke nomad api begin", "leaderUrl", leaderUrl)
	leader := new(string)
	if err := handler.InvokeApiWithKvData(http.MethodGet, leaderUrl, nil, leader); nil != err {
		return nil, err
	}
	logger.Info("invoke nomad api finished")

	nodes := make([]models.NodeListItemV2, 0)
	for _, nomadNode := range nomadNodes {
		node := models.NodeListItemV2{
			NodeAddress:           nomadNode.Address,
			NodeName:              nomadNode.Name,
			NodeId:                nomadNode.ID,
			NodeStatus:            nomadNode.Status,
			NodeStatusDescription: nomadNode.StatusDescription,
			Datacenter:            nomadNode.Datacenter,
			NomadVersion:          nomadNode.Version,
			DtleVersion:           nomadNode.Drivers["dtle"].Attributes["driver.dtle.full_version"],
		}
		if nomadNode.Drivers["dtle"] != nil {
			node.DtleVersion = nomadNode.Drivers["dtle"].Attributes["driver.dtle.full_version"]
		}
		if strings.Split(*leader, ":")[0] == node.NodeAddress {
			node.Leader = true
		}
		if _, ok := membersMap[node.NodeAddress]; ok {
			node.Member = true
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

func GetNodeInfo(nodeId string) (nomadApi.Node, error) {
	url := handler.BuildUrl(fmt.Sprintf("/v1/node/%s", nodeId))
	nomadNode := nomadApi.Node{}
	if err := handler.InvokeApiWithKvData(http.MethodGet, url, nil, &nomadNode); nil != err {
		return nomadNode, err
	}
	return nomadNode, nil
}

func FindNodeList() ([]nomadApi.NodeListStub, error) {
	nodesUrl := handler.BuildUrl("/v1/nodes")
	nomadNodes := make([]nomadApi.NodeListStub, 0)
	if err := handler.InvokeApiWithKvData(http.MethodGet, nodesUrl, nil, &nomadNodes); nil != err {
		return nil, err
	}
	return nomadNodes, nil
}
