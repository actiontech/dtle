package v2

import (
	"fmt"
	"github.com/actiontech/dtle/g"
	"net/http"

	"github.com/actiontech/dtle/drivers/api/handler"
	"github.com/actiontech/dtle/drivers/api/models"
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
	url := handler.BuildUrl("/v1/nodes")
	logger.Info("invoke nomad api begin", "url", url)
	nomadNodes := []nomadApi.NodeListStub{}
	if err := handler.InvokeApiWithKvData(http.MethodGet, url, nil, &nomadNodes); nil != err {
		return nil, err
	}
	logger.Info("invoke nomad api finished")
	nodes := []models.NodeListItemV2{}
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
		nodes = append(nodes, node)
	}
	return nodes, nil
}
