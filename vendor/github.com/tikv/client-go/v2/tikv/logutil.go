// Copyright 2021 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"context"

	"github.com/tikv/client-go/v2/internal/logutil"
	"go.uber.org/zap"
)

// WithLogContext returns a copy of context that is associated with a logger.
func WithLogContext(ctx context.Context, logger *zap.Logger) context.Context {
	return context.WithValue(ctx, logutil.CtxLogKey, logger)
}

// SetLogContextKey sets the context key which is used by client to retrieve *zap.Logger from context.
func SetLogContextKey(key interface{}) {
	logutil.CtxLogKey = key
}
