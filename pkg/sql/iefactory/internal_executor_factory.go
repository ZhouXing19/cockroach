// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package iefactory

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlextratxnstate"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
)

// SessionBoundInternalExecutorFactory is a function that produces a "session
// bound" internal executor.
type SessionBoundInternalExecutorFactory func(
	context.Context, *sessiondata.SessionData, *sqlextratxnstate.ExtraTxnState,
) sqlutil.InternalExecutor
