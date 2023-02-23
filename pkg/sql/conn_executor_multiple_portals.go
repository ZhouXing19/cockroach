// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessionphase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel/attribute"
	"time"
)

// connExecutor.execStmt
func (ex *connExecutor) execStmtSetup(
	ctx context.Context,
	parserStmt parser.Statement,
	portal *PreparedPortal,
	prepared *PreparedStatement,
	pinfo *tree.PlaceholderInfo,
	res RestrictedCommandResult,
	canAutoCommit bool,
) error {
	ast := portal.sqlStmt.AST
	if log.V(2) || logStatementsExecuteEnabled.Get(&ex.server.cfg.Settings.SV) ||
		log.HasSpanOrEvent(ctx) {
		log.VEventf(ctx, 2, "executing: %s in state: %s", ast, ex.machine.CurState())
	}

	// Stop the session idle timeout when a new statement is executed.
	// TODO(jane): I think we want to keep this each time executing a portal.
	ex.mu.IdleInSessionTimeout.Stop()
	ex.mu.IdleInTransactionSessionTimeout.Stop()

	// Run observer statements in a separate code path; their execution does not
	// depend on the current transaction state.
	// TODO(jane): Don't think we will run these stmts for portals.
	if _, ok := ast.(tree.ObserverStatement); ok {
		ex.statsCollector.Reset(ex.applicationStats, ex.phaseTimes)
		err := ex.runObserverStatement(ctx, ast, res)
		// Note that regardless of res.Err(), these observer statements don't
		// generate error events; transactions are always allowed to continue.
		return err
	}

	// -------------
	// ------T------
	// ------O------
	// ------D------
	// ------O------
	// -------------
	// Execute the query based on the txn state

	if ex.sessionData().IdleInSessionTimeout > 0 {
		// Cancel the session if the idle time exceeds the idle in session timeout.
		ex.mu.IdleInSessionTimeout = timeout{time.AfterFunc(
			ex.sessionData().IdleInSessionTimeout,
			ex.CancelSession,
		)}
	}

	if ex.sessionData().IdleInTransactionSessionTimeout > 0 {
		startIdleInTransactionSessionTimeout := func() {
			switch ast.(type) {
			case *tree.CommitTransaction, *tree.RollbackTransaction:
				// Do nothing, the transaction is completed, we do not want to start
				// an idle timer.
			default:
				ex.mu.IdleInTransactionSessionTimeout = timeout{time.AfterFunc(
					ex.sessionData().IdleInTransactionSessionTimeout,
					ex.CancelSession,
				)}
			}
		}
		switch ex.machine.CurState().(type) {
		case stateAborted, stateCommitWait:
			startIdleInTransactionSessionTimeout()
		case stateOpen:
			// Only start timeout if the statement is executed in an
			// explicit transaction.
			if !ex.implicitTxn() {
				startIdleInTransactionSessionTimeout()
			}
		}
	}
	return nil
}

type timeoutTracker struct {
	queryTimedOut      bool
	txnTimedOut        bool
	queryTimeoutTicker *time.Timer
	txnTimeoutTicker   *time.Timer
	// queryDoneAfterFunc and txnDoneAfterFunc will be allocated only when
	// queryTimeoutTicker or txnTimeoutTicker is non-nil.
	queryDoneAfterFunc chan struct{}
	txnDoneAfterFunc   chan struct{}
	cancelQuery        context.CancelFunc
}

// connExecutor.execStmtInOpenState
func (ex *connExecutor) execStmtInOpenStateSetup(
	ctx context.Context,
	parserStmt parser.Statement,
	portal *PreparedPortal,
	prepared *PreparedStatement,
	pinfo *tree.PlaceholderInfo,
	res RestrictedCommandResult,
	canAutoCommit bool,
) (retEv fsm.Event, retPayload fsm.EventPayload, retErr error) {
	// TODO(jane): I feel this should happen only once. To confirm.
	ctx, sp := tracing.EnsureChildSpan(ctx, ex.server.cfg.AmbientCtx.Tracer, "sql query")
	// TODO(andrei): Consider adding the placeholders as tags too.
	sp.SetTag("statement", attribute.StringValue(parserStmt.SQL))
	portal.AddCleanupFunc(func() error {
		sp.Finish()
		return nil
	})
	ast := portal.sqlStmt.AST
	ctx = withStatement(ctx, ast)

	makeErrEvent := func(err error) (fsm.Event, fsm.EventPayload, error) {
		ev, payload := ex.makeErrEvent(err, ast)
		return ev, payload, nil
	}

	var stmt Statement
	queryID := ex.generateID()
	// Update the deadline on the transaction based on the collections.
	err := ex.extraTxnState.descCollection.MaybeUpdateDeadline(ctx, ex.state.mu.txn)
	if err != nil {
		return makeErrEvent(err)
	}

	isExtendedProtocol := prepared != nil
	if isExtendedProtocol {
		stmt = makeStatementFromPrepared(prepared, queryID)
	} else {
		stmt = makeStatement(parserStmt, queryID)
	}

	portal.sqlStmt = &stmt

	ex.incrementStartedStmtCounter(ast)

	portal.AddCleanupFunc(func() error {
		if retErr == nil && !payloadHasError(retPayload) {
			ex.incrementExecutedStmtCounter(ast)
		}
		return nil
	})

	func(st *txnState) {
		st.mu.Lock()
		defer st.mu.Unlock()
		st.mu.stmtCount++
	}(&ex.state)

	var cancelQuery context.CancelFunc
	ctx, cancelQuery = contextutil.WithCancel(ctx)
	ex.addActiveQuery(parserStmt, pinfo, queryID, cancelQuery)

	tt := &timeoutTracker{cancelQuery: cancelQuery}

	// Make sure that we always unregister the query. It also deals with
	// overwriting res.Error to a more user-friendly message in case of query
	// cancellation.
	portal.AddCleanupFunc(func() error {
		if tt.queryTimeoutTicker != nil {
			if !tt.queryTimeoutTicker.Stop() {
				// Wait for the timer callback to complete to avoid a data race on
				// queryTimedOut.
				<-tt.queryDoneAfterFunc
			}
		}
		if tt.txnTimeoutTicker != nil {
			if !tt.txnTimeoutTicker.Stop() {
				// Wait for the timer callback to complete to avoid a data race on
				// txnTimedOut.
				<-tt.txnDoneAfterFunc
			}
		}

		// Detect context cancelation and overwrite whatever error might have been
		// set on the result before. The idea is that once the query's context is
		// canceled, all sorts of actors can detect the cancelation and set all
		// sorts of errors on the result. Rather than trying to impose discipline
		// in that jungle, we just overwrite them all here with an error that's
		// nicer to look at for the client.
		if res != nil && ctx.Err() != nil && res.Err() != nil {
			// Even in the cases where the error is a retryable error, we want to
			// intercept the event and payload returned here to ensure that the query
			// is not retried.
			retEv = eventNonRetriableErr{
				IsCommit: fsm.FromBool(isCommit(ast)),
			}
			res.SetError(cancelchecker.QueryCanceledError)
			retPayload = eventNonRetriableErrPayload{err: cancelchecker.QueryCanceledError}
		}

		ex.removeActiveQuery(queryID, ast)
		cancelQuery()
		if ex.executorType != executorTypeInternal {
			ex.metrics.EngineMetrics.SQLActiveStatements.Dec(1)
		}

		// If the query timed out, we intercept the error, payload, and event here
		// for the same reasons we intercept them for canceled queries above.
		// Overriding queries with a QueryTimedOut error needs to happen after
		// we've checked for canceled queries as some queries may be canceled
		// because of a timeout, in which case the appropriate error to return to
		// the client is one that indicates the timeout, rather than the more general
		// query canceled error. It's important to note that a timed out query may
		// not have been canceled (eg. We never even start executing a query
		// because the timeout has already expired), and therefore this check needs
		// to happen outside the canceled query check above.
		if tt.queryTimedOut {
			// A timed out query should never produce retryable errors/events/payloads
			// so we intercept and overwrite them all here.
			retEv = eventNonRetriableErr{
				IsCommit: fsm.FromBool(isCommit(ast)),
			}
			res.SetError(sqlerrors.QueryTimeoutError)
			retPayload = eventNonRetriableErrPayload{err: sqlerrors.QueryTimeoutError}
		} else if tt.txnTimedOut {
			retEv = eventNonRetriableErr{
				IsCommit: fsm.FromBool(isCommit(ast)),
			}
			res.SetError(sqlerrors.TxnTimeoutError)
			retPayload = eventNonRetriableErrPayload{err: sqlerrors.TxnTimeoutError}
		}
		return nil
	})

	if ex.executorType != executorTypeInternal {
		ex.metrics.EngineMetrics.SQLActiveStatements.Inc(1)
	}
	// TODO(jane): get the correct return
	return nil, nil, nil
}

func (ex *connExecutor) execStmtInOpenStateExec(
	ctx context.Context,
	portal *PreparedPortal,
	res RestrictedCommandResult,
	tt *timeoutTracker,
	pinfo *tree.PlaceholderInfo,
) (retEv fsm.Event, retPayload fsm.EventPayload, retErr error) {

	makeErrEvent := func(err error) (fsm.Event, fsm.EventPayload, error) {
		ev, payload := ex.makeErrEvent(err, portal.sqlStmt.AST)
		return ev, payload, nil
	}

	if tt == nil {
		return makeErrEvent(errors.New("timeoutTracker cannot be nil"))
	}

	if tree.CanWriteData(portal.sqlStmt.AST) || tree.CanModifySchema(portal.sqlStmt.AST) {
		return makeErrEvent(errors.New("multiple active portals only allow read-only queries"))
	}

	p := &ex.planner
	stmtTS := ex.server.cfg.Clock.PhysicalTime()
	ex.statsCollector.Reset(ex.applicationStats, ex.phaseTimes)
	ex.resetPlanner(ctx, p, ex.state.mu.txn, stmtTS)
	p.sessionDataMutatorIterator.paramStatusUpdater = res
	p.noticeSender = res
	ih := &p.instrumentation

	os := ex.machine.CurState().(stateOpen)
	var needFinish bool
	ctx, needFinish = ih.Setup(
		ctx, ex.server.cfg, ex.statsCollector, p, ex.stmtDiagnosticsRecorder,
		portal.sqlStmt.StmtNoConstants, os.ImplicitTxn.Get(), ex.extraTxnState.shouldCollectTxnExecutionStats,
	)

	//TODO(janexing): what is this??
	if needFinish {
		sql := portal.sqlStmt.SQL
		defer func() {
			retErr = ih.Finish(
				ex.server.cfg,
				ex.statsCollector,
				&ex.extraTxnState.accumulatedStats,
				ih.collectExecStats,
				p,
				portal.sqlStmt.AST,
				sql,
				res,
				retPayload,
				retErr,
			)
		}()
	}

	if ex.sessionData().TransactionTimeout > 0 && !ex.implicitTxn() {
		timerDuration :=
			ex.sessionData().TransactionTimeout - timeutil.Since(ex.phaseTimes.GetSessionPhaseTime(sessionphase.SessionTransactionStarted))

		// If the timer already expired, but the transaction is not yet aborted,
		// we should error immediately without executing. If the timer
		// expired but the transaction already is aborted, then we should still
		// proceed with executing the statement in order to get a
		// TransactionAbortedError.
		_, txnAborted := ex.machine.CurState().(stateAborted)

		if timerDuration < 0 && !txnAborted {
			tt.txnTimedOut = true
			return makeErrEvent(sqlerrors.TxnTimeoutError)
		}

		if timerDuration > 0 {
			tt.txnDoneAfterFunc = make(chan struct{}, 1)
			tt.txnTimeoutTicker = time.AfterFunc(
				timerDuration,
				func() {
					tt.cancelQuery()
					tt.txnTimedOut = true
					tt.txnDoneAfterFunc <- struct{}{}
				})
		}
	}

	// We exempt `SET` statements from the statement timeout, particularly so as
	// not to block the `SET statement_timeout` command itself.
	if ex.sessionData().StmtTimeout > 0 && portal.sqlStmt.AST.StatementTag() != "SET" {
		timerDuration :=
			ex.sessionData().StmtTimeout - timeutil.Since(ex.phaseTimes.GetSessionPhaseTime(sessionphase.SessionQueryReceived))
		// There's no need to proceed with execution if the timer has already expired.
		if timerDuration < 0 {
			tt.queryTimedOut = true
			return makeErrEvent(sqlerrors.QueryTimeoutError)
		}
		tt.queryDoneAfterFunc = make(chan struct{}, 1)
		tt.queryTimeoutTicker = time.AfterFunc(
			timerDuration,
			func() {
				tt.cancelQuery()
				tt.queryTimedOut = true
				tt.queryDoneAfterFunc <- struct{}{}
			})
	}

	defer func(ctx context.Context) {
		if filter := ex.server.cfg.TestingKnobs.StatementFilter; retErr == nil && filter != nil {
			var execErr error
			if perr, ok := retPayload.(payloadWithError); ok {
				execErr = perr.errorCause()
			}
			filter(ctx, ex.sessionData(), portal.sqlStmt.AST.String(), execErr)
		}

		// Do the auto-commit, if necessary. In the extended protocol, the
		// auto-commit happens when the Sync message is handled.
		if retEv != nil || retErr != nil {
			return
		}
	}(ctx)

	p.semaCtx.Annotations = tree.MakeAnnotations(portal.sqlStmt.NumAnnotations)

	// For regular statements (the ones that get to this point), we
	// don't return any event unless an error happens.

	if err := ex.handleAOST(ctx, portal.sqlStmt.AST); err != nil {
		return makeErrEvent(err)
	}

	// The first order of business is to ensure proper sequencing
	// semantics.  As per PostgreSQL's dialect specs, the "read" part of
	// statements always see the data as per a snapshot of the database
	// taken the instant the statement begins to run. In particular a
	// mutation does not see its own writes. If a query contains
	// multiple mutations using CTEs (WITH) or a read part following a
	// mutation, all still operate on the same read snapshot.
	//
	// (To communicate data between CTEs and a main query, the result
	// set / RETURNING can be used instead. However this is not relevant
	// here.)

	// We first ensure stepping mode is enabled.
	//
	// This ought to be done just once when a txn gets initialized;
	// unfortunately, there are too many places where the txn object
	// is re-configured, re-set etc without using NewTxnWithSteppingEnabled().
	//
	// Manually hunting them down and calling ConfigureStepping() each
	// time would be error prone (and increase the chance that a future
	// change would forget to add the call).
	//
	// TODO(andrei): really the code should be rearchitected to ensure
	// that all uses of SQL execution initialize the client.Txn using a
	// single/common function. That would be where the stepping mode
	// gets enabled once for all SQL statements executed "underneath".
	prevSteppingMode := ex.state.mu.txn.ConfigureStepping(ctx, kv.SteppingEnabled)
	defer func() { _ = ex.state.mu.txn.ConfigureStepping(ctx, prevSteppingMode) }()

	// Then we create a sequencing point.
	//
	// This is not the only place where a sequencing point is
	// placed. There are also sequencing point after every stage of
	// constraint checks and cascading actions at the _end_ of a
	// statement's execution.
	//
	// TODO(knz): At the time of this writing CockroachDB performs
	// cascading actions and the corresponding FK existence checks
	// interleaved with mutations. This is incorrect; the correct
	// behavior, as described in issue
	// https://github.com/cockroachdb/cockroach/issues/33475, is to
	// execute cascading actions no earlier than after all the "main
	// effects" of the current statement (including all its CTEs) have
	// completed. There should be a sequence point between the end of
	// the main execution and the start of the cascading actions, as
	// well as in-between very stage of cascading actions.
	// This TODO can be removed when the cascading code is reorganized
	// accordingly and the missing call to Step() is introduced.
	if err := ex.state.mu.txn.Step(ctx); err != nil {
		return makeErrEvent(err)
	}

	if err := p.semaCtx.Placeholders.Assign(pinfo, portal.sqlStmt.NumPlaceholders); err != nil {
		return makeErrEvent(err)
	}

	p.extendedEvalCtx.Placeholders = &p.semaCtx.Placeholders
	p.extendedEvalCtx.Annotations = &p.semaCtx.Annotations
	p.stmt = *portal.sqlStmt
	p.cancelChecker.Reset(ctx)
	// We're in extended pgwire protocol so can't autocommit. Commit is handled
	// by the sync message.
	p.autoCommit = false
	ex.extraTxnState.firstStmtExecuted = true

	var stmtThresholdSpan *tracing.Span
	alreadyRecording := ex.transitionCtx.sessionTracing.Enabled()
	stmtTraceThreshold := TraceStmtThreshold.Get(&ex.planner.execCfg.Settings.SV)
	var stmtCtx context.Context
	// TODO(andrei): I think we should do this even if alreadyRecording == true.
	if !alreadyRecording && stmtTraceThreshold > 0 {
		stmtCtx, stmtThresholdSpan = tracing.EnsureChildSpan(ctx, ex.server.cfg.AmbientCtx.Tracer, "trace-stmt-threshold", tracing.WithRecording(tracingpb.RecordingVerbose))
	} else {
		stmtCtx = ctx
	}
	_ = stmtCtx
	// TODO(jane): dispatch to distsql

	if stmtThresholdSpan != nil {
		stmtDur := timeutil.Since(ex.phaseTimes.GetSessionPhaseTime(sessionphase.SessionQueryReceived))
		needRecording := stmtTraceThreshold < stmtDur
		if needRecording {
			rec := stmtThresholdSpan.FinishAndGetRecording(tracingpb.RecordingVerbose)
			// NB: This recording does not include the commit for implicit
			// transactions if the statement didn't auto-commit.
			logTraceAboveThreshold(
				ctx,
				rec,
				fmt.Sprintf("SQL stmt %s", portal.Stmt.AST.String()),
				stmtTraceThreshold,
				stmtDur,
			)
		} else {
			stmtThresholdSpan.Finish()
		}
	}

	txn := ex.state.mu.txn

	// TODO(janexing): figure out what is this.
	if !os.ImplicitTxn.Get() && txn.IsSerializablePushAndRefreshNotPossible() {
		rc, canAutoRetry := ex.getRewindTxnCapability()
		if canAutoRetry {
			ev := eventRetriableErr{
				IsCommit:     fsm.FromBool(isCommit(portal.sqlStmt.AST)),
				CanAutoRetry: fsm.FromBool(canAutoRetry),
			}
			payload := eventRetriableErrPayload{
				err:    txn.GenerateForcedRetryableError(ctx, "serializable transaction timestamp pushed (detected by connExecutor)"),
				rewCap: rc,
			}
			return ev, payload, nil
		}
		log.VEventf(ctx, 2, "push detected for non-refreshable txn but auto-retry not possible")
	}

	// No event was generated.
	return nil, nil, nil

}
