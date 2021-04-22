package v2

import (
	"fmt"
	"net/http"

	"github.com/actiontech/dtle/drivers/api/handler"
	"github.com/actiontech/dtle/drivers/api/models"
	nomadApi "github.com/hashicorp/nomad/api"
	"github.com/labstack/echo/v4"
)

// @Description get node list.
// @Tags node
// @Success 200 {object} models.NodeListRespV2
// @Router /v2/nodes [get]
func NodeListV2(c echo.Context) error {
	url := handler.BuildUrl("/v1/nodes")
	nomadNodes := []nomadApi.Node{}
	if err := handler.InvokeNomadGetApi(url, &nomadNodes); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invoke nomad api %v failed: %v", url, err)))
	}

	nodes := []models.NodeListItemV2{}
	for _, nomadNode := range nomadNodes {
		nodes = append(nodes, models.NodeListItemV2{
			NodeAddress:           nomadNode.HTTPAddr,
			NodeName:              nomadNode.Name,
			NodeId:                nomadNode.ID,
			NodeStatus:            nomadNode.Status,
			NodeStatusDescription: nomadNode.StatusDescription,
			Datacenter:            nomadNode.Datacenter,
		})
	}

	return c.JSON(http.StatusOK, &models.NodeListRespV2{
		Nodes:    nodes,
		BaseResp: models.BuildBaseResp(nil),
	})
}
