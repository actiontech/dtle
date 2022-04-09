package v2

import (
	"fmt"
	"net/http"

	"github.com/actiontech/dtle/api/handler"
	"github.com/actiontech/dtle/g"
	nomadApi "github.com/hashicorp/nomad/api"
)

// job's allocations
func findAllocations(logger g.LoggerType, jobId string) (allocations []nomadApi.Allocation, err error) {
	url := handler.BuildUrl(fmt.Sprintf("/v1/job/%v/allocations", jobId))
	logger.Info("invoke nomad api begin", "url", url)
	if err := handler.InvokeApiWithKvData(http.MethodGet, url, nil, &allocations); nil != err {
		return allocations, fmt.Errorf("invoke nomad api %v failed: %v", url, err)
	}
	logger.Info("invoke nomad api finished")
	return
}
