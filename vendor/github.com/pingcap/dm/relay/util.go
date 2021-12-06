// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package relay

import (
	"context"
	"database/sql"
	"strings"

	"github.com/pingcap/dm/pkg/utils"
)

// isNewServer checks whether is connecting to a new server.
func isNewServer(ctx context.Context, prevUUID string, db *sql.DB, flavor string) (bool, error) {
	if len(prevUUID) == 0 {
		// no sub dir exists before
		return true, nil
	}
	uuid, err := utils.GetServerUUID(ctx, db, flavor)
	if err != nil {
		return false, err
	}
	if strings.HasPrefix(prevUUID, uuid) {
		// same server as before
		return false, nil
	}
	return true, nil
}
